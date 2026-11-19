//! The projection lifecycle, in one place: whether a projection is current,
//! rebuilding it, catching it up, reading it and retrying its dead letters.
//!
//! A projection is **current** when its checkpoint was built by the code
//! running now: the same version and the same tables. Only the projector
//! (`tick`) makes it so, by rebuilding it; every read takes it as it is. A
//! read of a projection that is not current answers as of a rebuild not yet
//! begun: no rows, no dead letters. So no read writes, and none serves rows
//! or retries dead letters another version made.

use factum_core::{Event, OnFailure, Projection, Query, RowOp, Table, Uuid, Value, Version, codes};
use factum_store::{Batch, Checkpoint, DeadLetter, Store, StoreError};

use crate::engine::{DISPATCH, Engine, Error, Fault, Races, rejected};
use factum_core::Instant;

/// What keeps a checkpoint and follows the log from it: dispatch, or a
/// projection.
#[derive(Clone, Copy)]
pub(crate) enum Keeper<'a> {
    Dispatch,
    Projection(&'a dyn Projection),
}

impl Keeper<'_> {
    fn name(&self) -> &str {
        match self {
            Keeper::Dispatch => DISPATCH,
            Keeper::Projection(p) => p.name(),
        }
    }

    /// Whether an event it fails on holds its checkpoint there.
    fn on_failure(&self) -> OnFailure {
        match self {
            Keeper::Dispatch => OnFailure::Skip,
            Keeper::Projection(p) => p.on_failure(),
        }
    }

    /// The version its checkpoint must carry to be current.
    fn version(&self) -> u32 {
        match self {
            Keeper::Dispatch => 1,
            Keeper::Projection(p) => p.version(),
        }
    }

    /// The tables its checkpoint must carry to be current.
    fn tables(&self) -> Vec<Table> {
        match self {
            Keeper::Dispatch => Vec::new(),
            Keeper::Projection(p) => p.tables(),
        }
    }
}

/// What one event did as a keeper followed it: the row ops it makes, or
/// why it failed; and how many things that was.
struct Step {
    ops: Result<Vec<RowOp>, String>,
    done: usize,
}

/// The identity a checkpoint must carry to be the running code's.
fn schema(tables: &[Table]) -> Result<String, Error> {
    serde_json::to_string(tables).map_err(|e| Error::Fault(e.to_string()))
}

impl<S: Store> Engine<S> {
    fn projection(&self, name: &str) -> Result<&dyn Projection, Error> {
        self.model
            .projections
            .iter()
            .find(|p| p.name() == name)
            .map(|p| p.as_ref())
            .ok_or_else(|| rejected(codes::UNKNOWN, format!("no projection {name}")))
    }

    /// What keeps the checkpoint `name`.
    fn keeper(&self, name: &str) -> Result<Keeper<'_>, Error> {
        if name == DISPATCH {
            return Ok(Keeper::Dispatch);
        }
        Ok(Keeper::Projection(self.projection(name)?))
    }

    /// The checkpoint of `name` if it was built as `version` with `tables`;
    /// `None` if it was not built yet, or by other code.
    async fn current(
        &self,
        name: &str,
        version: u32,
        tables: &[Table],
    ) -> Result<Option<Checkpoint>, Error> {
        let schema = schema(tables)?;
        Ok(self
            .store
            .checkpoint(name)
            .await?
            .filter(|c| c.version == version && c.schema == schema))
    }

    /// Where `name` stands, rebuilt from the start if it is not current.
    /// Only the loops call this: it is the one write that makes a
    /// projection current.
    pub(crate) async fn make_current(
        &self,
        name: &str,
        version: u32,
        tables: &[Table],
    ) -> Result<Checkpoint, Error> {
        if let Some(c) = self.current(name, version, tables).await? {
            return Ok(c);
        }
        let schema = schema(tables)?;
        self.store
            .reset_projection(name, version, &schema, tables)
            .await?;
        Ok(Checkpoint {
            version,
            schema,
            position: Default::default(),
            stopped: false,
        })
    }

    /// The projection `name`, if current, and the declared table `table`.
    /// Refused `invalid_query` if it declares no such table.
    async fn readable(&self, name: &str, table: &str) -> Result<(Table, bool), Error> {
        let p = self.projection(name)?;
        let tables = p.tables();
        let Some(t) = tables.iter().find(|t| t.name == table).cloned() else {
            return Err(rejected(
                codes::INVALID_QUERY,
                format!("{name} has no table {table}"),
            ));
        };
        let current = self.current(name, p.version(), &tables).await?.is_some();
        Ok((t, current))
    }

    /// The row ops `p` makes of `e`, in its current shape; a failure if it
    /// fails or writes a table it did not declare.
    fn project_one(
        &self,
        p: &dyn Projection,
        tables: &[Table],
        e: Event,
    ) -> Result<Vec<RowOp>, String> {
        let ops = p.project(&self.upcast(e).map_err(|e| e.to_string())?)?;
        for op in &ops {
            let (table, _) = factum_store::op_target(op);
            if !tables.iter().any(|t| t.name == table) {
                return Err(format!(
                    "writes table {table:?}, which {} does not declare",
                    p.name()
                ));
            }
        }
        Ok(ops)
    }

    /// What `k` does with `e`: a projection's row ops; for dispatch, the
    /// processes it starts and the waits it answers, its fault set aside.
    /// Only a store error ends it.
    async fn step(
        &self,
        k: Keeper<'_>,
        tables: &[Table],
        e: Event,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<Step, Error> {
        let Keeper::Projection(p) = k else {
            let event = e.id;
            return match self.dispatch_event(e, now, faults).await {
                Ok(done) => Ok(Step {
                    ops: Ok(Vec::new()),
                    done,
                }),
                Err(Error::Store(e)) => Err(e.into()),
                Err(err) => {
                    let error = err.to_string();
                    faults.push(Fault {
                        item: format!("event {event}"),
                        error: error.clone(),
                    });
                    Ok(Step {
                        ops: Err(error),
                        done: 0,
                    })
                }
            };
        };
        Ok(Step {
            ops: self.project_one(p, tables, e),
            done: 1,
        })
    }

    /// Moves `k`'s checkpoint from `at` to `to` in one batch, with the rows,
    /// dead letters and retried dead letters of that move. Answers false if
    /// `at` is no longer where it stands.
    async fn advance(
        &self,
        k: Keeper<'_>,
        at: &Checkpoint,
        to: &Checkpoint,
        ops: Vec<RowOp>,
        dead: Vec<DeadLetter>,
        resolved: Vec<Uuid>,
    ) -> Result<bool, Error> {
        let batch = Batch {
            projection: k.name().into(),
            version: k.version(),
            from: at.position,
            to: to.position,
            ops,
            dead,
            resolved,
            stop: to.stopped,
        };
        match self.store.apply_projection(batch).await {
            Ok(()) => Ok(true),
            Err(StoreError::Stale(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Catches `k`'s checkpoint up with the log, rebuilding it if it is not
    /// current: each event a step, a failing one a dead letter. Gives up if
    /// another catch-up moved it first. Answers how many things it did.
    pub(crate) async fn follow(
        &self,
        k: Keeper<'_>,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let tables = k.tables();
        let mut at = self.make_current(k.name(), k.version(), &tables).await?;
        let mut done = 0;
        while !at.stopped {
            let events = self.store.read_log(at.position, self.options.batch).await?;
            if events.is_empty() {
                break;
            }
            let (mut ops, mut dead, mut to) = (Vec::new(), Vec::new(), at.clone());
            for e in events {
                let (position, event, stream, version) =
                    (e.position, e.id, e.stream.clone(), e.version);
                let step = self.step(k, &tables, e, now, faults).await?;
                match step.ops {
                    Ok(o) => ops.extend(o),
                    Err(error) => {
                        dead.push(DeadLetter {
                            projection: k.name().into(),
                            position,
                            event,
                            stream,
                            version,
                            error,
                        });
                        if k.on_failure() == OnFailure::Stop {
                            to.stopped = true;
                            break;
                        }
                    }
                }
                to.position = position;
                done += step.done;
            }
            if !self.advance(k, &at, &to, ops, dead, Vec::new()).await? {
                break;
            }
            at = to;
        }
        Ok(done)
    }

    /// Catches every projection up with the log. Answers how many events it
    /// projected.
    pub(crate) async fn project(&self, now: Instant) -> Result<usize, Error> {
        let mut done = 0;
        for p in &self.model.projections {
            done += self
                .follow(Keeper::Projection(p.as_ref()), now, &mut Vec::new())
                .await?;
        }
        Ok(done)
    }

    // ---- reads ----

    /// Rows of a projection's table that the query asks for. Refused
    /// `invalid_query` if it names a table or column the projection does not
    /// declare.
    pub async fn query(
        &self,
        projection: &str,
        query: &Query,
    ) -> Result<Vec<(String, Value)>, Error> {
        let (table, current) = self.readable(projection, &query.table).await?;
        if let Some(c) = query
            .columns()
            .find(|c| !table.columns.iter().any(|d| d == c))
        {
            return Err(rejected(
                codes::INVALID_QUERY,
                format!("{} has no column {c}", query.table),
            ));
        }
        if !current {
            return Ok(Vec::new());
        }
        Ok(self.store.query(projection, query).await?)
    }

    pub async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>, Error> {
        if !self.readable(projection, table).await?.1 {
            return Ok(Vec::new());
        }
        Ok(self.store.rows(projection, table).await?)
    }

    pub async fn row(
        &self,
        projection: &str,
        table: &str,
        key: &str,
    ) -> Result<Option<Value>, Error> {
        if !self.readable(projection, table).await?.1 {
            return Ok(None);
        }
        Ok(self.store.row(projection, table, key).await?)
    }

    /// The events `projection` failed on; for [`DISPATCH`], the events the
    /// engine could not dispatch.
    pub async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>, Error> {
        let k = self.keeper(projection)?;
        if self
            .current(projection, k.version(), &k.tables())
            .await?
            .is_none()
        {
            return Ok(Vec::new());
        }
        Ok(self.store.dead_letters(projection).await?)
    }

    /// Projects a dead letter's event again with the code running now. If
    /// it projects, its rows are written and the dead letter goes, in one
    /// batch; a projection that stopped at it goes on past it. Answers
    /// whether it projected; if not, the dead letter stays. Refused
    /// `unknown` if the projection, as current, has no such dead letter.
    /// For [`DISPATCH`], dispatches the event again: the processes it starts
    /// and the waits it answers.
    pub async fn retry_dead_letter(&self, projection: &str, event: Uuid) -> Result<bool, Error> {
        let k = self.keeper(projection)?;
        let tables = k.tables();
        let mut races = Races::new();
        loop {
            let Some(at) = self.current(projection, k.version(), &tables).await? else {
                return Err(no_dead_letter(projection, event));
            };
            let Some(dead) = self
                .store
                .dead_letters(projection)
                .await?
                .into_iter()
                .find(|d| d.event == event)
            else {
                return Err(no_dead_letter(projection, event));
            };
            let Some(e) = self
                .store
                .read_stream(&dead.stream, Version(dead.version.0 - 1))
                .await?
                .into_iter()
                .next()
            else {
                return Err(Error::Fault(format!(
                    "{} has no version {:?}",
                    dead.stream, dead.version
                )));
            };
            let step = self
                .step(k, &tables, e, self.now(), &mut Vec::new())
                .await?;
            let Ok(ops) = step.ops else {
                return Ok(false);
            };
            // Past the checkpoint only if it is held at this dead letter: it
            // is projected now, so go on past it.
            let past = dead.position > at.position;
            let mut to = at.clone();
            if past {
                (to.position, to.stopped) = (dead.position, false);
            }
            if self
                .advance(k, &at, &to, ops, Vec::new(), vec![event])
                .await?
            {
                return Ok(true);
            }
            races
                .lost(format!("{projection}: retrying {event}"))
                .await?;
        }
    }
}

fn no_dead_letter(projection: &str, event: Uuid) -> Error {
    rejected(
        codes::UNKNOWN,
        format!("{projection} has no dead letter {event}"),
    )
}
