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

use crate::engine::{DISPATCH, Engine, Error, MAX_RETRIES, rejected, yield_now};

/// What keeps a checkpoint: a projection, or dispatch (`None`), with the
/// version and tables its checkpoint must carry to be current.
type Keeper<'a> = (Option<&'a dyn Projection>, u32, Vec<Table>);

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
            return Ok((None, 1, Vec::new()));
        }
        let p = self.projection(name)?;
        Ok((Some(p), p.version(), p.tables()))
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

    /// Catches every projection up with the log, rebuilding those that are
    /// not current. Answers how many events it projected.
    pub(crate) async fn project(&self) -> Result<usize, Error> {
        let mut done = 0;
        for p in &self.model.projections {
            let tables = p.tables();
            let mut at = self.make_current(p.name(), p.version(), &tables).await?;
            while !at.stopped {
                let events = self.store.read_log(at.position, self.options.batch).await?;
                if events.is_empty() {
                    break;
                }
                let (mut ops, mut dead, mut to, mut stop) =
                    (Vec::new(), Vec::new(), at.position, false);
                for e in events {
                    let (position, id, stream, version) =
                        (e.position, e.id, e.stream.clone(), e.version);
                    match self.project_one(p.as_ref(), &tables, e) {
                        Ok(o) => ops.extend(o),
                        Err(error) => {
                            dead.push(DeadLetter {
                                projection: p.name().into(),
                                position,
                                event: id,
                                stream,
                                version,
                                error,
                            });
                            if p.on_failure() == OnFailure::Stop {
                                stop = true;
                                break;
                            }
                        }
                    }
                    to = position;
                    done += 1;
                }
                let batch = Batch {
                    projection: p.name().into(),
                    version: p.version(),
                    from: at.position,
                    to,
                    ops,
                    dead,
                    resolved: Vec::new(),
                    stop,
                };
                match self.store.apply_projection(batch).await {
                    Ok(()) => {
                        at.position = to;
                        at.stopped = stop;
                    }
                    Err(StoreError::Stale(_)) => break,
                    Err(e) => return Err(e.into()),
                }
            }
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
        let (_, version, tables) = self.keeper(projection)?;
        if self.current(projection, version, &tables).await?.is_none() {
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
        let (p, version, tables) = self.keeper(projection)?;
        for _ in 0..MAX_RETRIES {
            let Some(at) = self.current(projection, version, &tables).await? else {
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
            let ops = match p {
                Some(p) => match self.project_one(p, &tables, e) {
                    Ok(ops) => ops,
                    Err(_) => return Ok(false),
                },
                None => match self.dispatch_event(e, self.now(), &mut Vec::new()).await {
                    Ok(_) => Vec::new(),
                    Err(Error::Store(e)) => return Err(e.into()),
                    Err(_) => return Ok(false),
                },
            };
            // Held at this dead letter: it is projected now, so go on past it.
            let past = at.stopped && dead.position > at.position;
            let batch = Batch {
                projection: projection.into(),
                version,
                from: at.position,
                to: if past { dead.position } else { at.position },
                ops,
                dead: Vec::new(),
                resolved: vec![event],
                stop: at.stopped && !past,
            };
            match self.store.apply_projection(batch).await {
                Ok(()) => return Ok(true),
                Err(StoreError::Stale(_)) => yield_now().await,
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::Fault(format!(
            "{projection}: gave up retrying {event} after {MAX_RETRIES} races"
        )))
    }
}

fn no_dead_letter(projection: &str, event: Uuid) -> Error {
    rejected(
        codes::UNKNOWN,
        format!("{projection} has no dead letter {event}"),
    )
}
