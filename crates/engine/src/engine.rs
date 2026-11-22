use std::sync::Arc;

use factum_core::{
    By, DynAggregate, Event, Finished, Instant, Meta, NewEvent, PROCESS, ProcessEvent,
    ProcessState, Progress, Rejection, SCHEDULED, Step, StreamId, Uuid, Value, Version, WorkChange,
    WorkKind, codes, effects, fingerprint, start_key,
};
use factum_store::{Answer, AnswerResult, Append, Snapshot, Store, StoreError, Unit};
use serde::Serialize;

use crate::clock::Clock;
use crate::model::Model;
use crate::projector::Keeper;

#[derive(Debug)]
pub enum Error {
    /// The command, or factum on its behalf, said no.
    Rejected(Rejection),
    Store(StoreError),
    /// Declared code misbehaved: a fold, an upcast, a model mismatch.
    Fault(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Rejected(r) => write!(f, "rejected: {r}"),
            Error::Store(e) => write!(f, "{e}"),
            Error::Fault(e) => write!(f, "fault: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Error {
        Error::Store(e)
    }
}

/// One item a tick set aside: an event it could not dispatch, a process it
/// could not advance, a call, schedule, recurring command or wait it could
/// not run. The rest of the tick went on.
#[derive(Clone, Debug, PartialEq)]
pub struct Fault {
    /// What faulted: `event <id>`, `process/<id>`, `action <name> <id>`,
    /// `schedule <id>`, `recurring <name>` or `wait <id>`.
    pub item: String,
    pub error: String,
}

/// What one pass of the loops did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tick {
    /// How many things it did.
    pub done: usize,
    /// The items it set aside.
    pub faults: Vec<Fault>,
}

/// An item's result, its fault set aside: a fault is the item's and is
/// reported in `faults`; a store error is no item's and ends the tick.
pub(crate) fn set_aside<T>(
    result: Result<T, Error>,
    item: impl FnOnce() -> String,
    faults: &mut Vec<Fault>,
) -> Result<Option<T>, Error> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(Error::Store(e)) => Err(Error::Store(e)),
        Err(e) => {
            faults.push(Fault {
                item: item(),
                error: e.to_string(),
            });
            Ok(None)
        }
    }
}

pub(crate) fn rejected(code: &str, message: impl Into<String>) -> Error {
    Error::Rejected(Rejection::new(code, message))
}

/// A command sent under an idempotency key.
#[derive(Clone, Debug)]
pub struct Submission {
    pub command: String,
    pub params: Value,
    /// Sending the same key again answers the first answer that wrote
    /// events; a refusal, or one that wrote none, keeps nothing, so a
    /// re-send decides again.
    pub key: Uuid,
    pub by: By,
}

impl Submission {
    pub fn new(command: &str, params: impl Serialize, key: Uuid, by: By) -> Submission {
        Submission {
            command: command.to_string(),
            params: serde_json::to_value(params).unwrap_or(Value::Null),
            key,
            by,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    pub events: Vec<Event>,
    /// The key had an answer already; nothing new was recorded.
    pub replayed: bool,
}

/// A process as its stream says it stands.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessView {
    pub name: String,
    pub input: Value,
    pub progress: Progress,
    pub pending: Option<(String, Step)>,
    pub finished: Option<Finished>,
}

#[derive(Clone, Debug)]
pub struct Options {
    /// Keep a snapshot when a fold read this many events past the last one.
    pub snapshot_every: u64,
    /// How long one runner holds a process while it advances it.
    pub advance_lease_ms: i64,
    /// How long a runner holds a due schedule or an expired wait.
    pub schedule_lease_ms: i64,
    /// How much one pass of each loop takes.
    pub batch: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            snapshot_every: 100,
            advance_lease_ms: 30_000,
            schedule_lease_ms: 60_000,
            batch: 100,
        }
    }
}

/// Where the engine keeps how far it has dispatched the log: a checkpoint
/// with no tables, under a name no projection can have. The events it could
/// not dispatch are its dead letters.
pub const DISPATCH: &str = "factum.dispatch";
/// How many ticks `settle` takes before it calls the engine stuck busy.
const SETTLE_TICKS: usize = 1000;

/// How often one writer decides again after losing a race. Every loss is
/// another writer's win, so the log always moves; the bound only keeps one
/// writer from losing forever.
const MAX_RETRIES: usize = 1000;

/// The one home of deciding again after a lost race (SPEC §7). A writer
/// counts its losses here; each one yields once, so the winners finish
/// before it decides again, until the bound, where it gives up. What counts
/// as a lost race is the writer's own.
pub(crate) struct Races {
    lost: usize,
}

impl Races {
    pub(crate) fn new() -> Races {
        Races { lost: 0 }
    }

    /// `what` lost a race: lets the other tasks run once, or gives up with
    /// a fault naming it after `MAX_RETRIES` losses.
    pub(crate) async fn lost(&mut self, what: impl std::fmt::Display) -> Result<(), Error> {
        self.lost += 1;
        if self.lost >= MAX_RETRIES {
            return Err(Error::Fault(format!(
                "{what}: gave up after losing {MAX_RETRIES} races"
            )));
        }
        yield_now().await;
        Ok(())
    }
}

/// Lets the other tasks run once. Needs no particular runtime.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

pub struct Engine<S: Store> {
    pub(crate) store: Arc<S>,
    pub(crate) model: Arc<Model>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) holder: String,
    pub(crate) options: Options,
}

impl<S: Store> Engine<S> {
    /// `holder` names this runner in the leases it takes.
    pub fn new(
        store: Arc<S>,
        model: Arc<Model>,
        clock: Arc<dyn Clock>,
        holder: impl Into<String>,
    ) -> Engine<S> {
        Engine {
            store,
            model,
            clock,
            holder: holder.into(),
            options: Options::default(),
        }
    }

    pub fn with_options(mut self, options: Options) -> Engine<S> {
        self.options = options;
        self
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    // ---- commands ----

    /// Decides and records a command, or answers what its key answered.
    pub async fn submit(&self, submission: Submission) -> Result<Applied, Error> {
        self.submit_at(submission, self.clock.now(), false).await
    }

    /// Submits at `now`. A refusal, or an answer that wrote no events, is
    /// kept for the key only when `keep`: factum's own runners (steps,
    /// schedules, recurring) keep theirs, so a runner that took one over
    /// answers what the first decided.
    pub(crate) async fn submit_at(
        &self,
        sub: Submission,
        now: Instant,
        keep: bool,
    ) -> Result<Applied, Error> {
        let print = fingerprint(&sub.command, &sub.params);
        let mut races = Races::new();
        loop {
            if let Some(answer) = self.store.answer(sub.key).await? {
                return replay(answer, print);
            }
            let (stream, agg, decided) = match self.decide(&sub, now).await {
                Ok(d) => d,
                Err(Error::Rejected(r)) if keep => {
                    let unit = Unit {
                        answer: Some((
                            sub.key,
                            Answer {
                                fingerprint: print,
                                result: AnswerResult::Rejected(r.clone()),
                            },
                        )),
                        ..Unit::default()
                    };
                    match self.store.commit(unit).await {
                        Ok(_) => return Err(Error::Rejected(r)),
                        // Another writer answered the key first: answer what it holds.
                        Err(StoreError::KeyTaken(_)) => {
                            races.lost(&sub.command).await?;
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e),
            };
            let (loaded, new) = decided;
            // Like a refusal, an external submit that writes nothing keeps
            // nothing: a re-send decides again and can apply nothing twice.
            if new.is_empty() && !keep {
                return Ok(Applied {
                    events: Vec::new(),
                    replayed: false,
                });
            }
            let write = Write {
                stream: stream.clone(),
                expected: loaded.version,
                events: new,
                meta: Meta {
                    now,
                    by: sub.by.clone(),
                    key: Some(sub.key),
                    command: sub.command.clone(),
                },
                fingerprint: Some(print),
                work: Vec::new(),
                guards: Vec::new(),
            };
            match self.write(write).await {
                Ok(committed) => {
                    self.keep_snapshot(agg, &stream, loaded, &committed).await?;
                    return Ok(Applied {
                        events: committed,
                        replayed: false,
                    });
                }
                Err(StoreError::Conflict(_) | StoreError::KeyTaken(_)) => {
                    races.lost(&sub.command).await?
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// What the command would record, recording nothing.
    pub async fn plan(
        &self,
        command: &str,
        params: impl Serialize,
        by: By,
    ) -> Result<Vec<NewEvent>, Error> {
        let params = serde_json::to_value(params)
            .map_err(|e| rejected(codes::INVALID_PARAMS, e.to_string()))?;
        let sub = Submission {
            command: command.to_string(),
            params,
            key: Uuid::nil(),
            by,
        };
        let (_, _, (_, events)) = self.decide(&sub, self.clock.now()).await?;
        Ok(events)
    }

    #[allow(clippy::type_complexity)]
    async fn decide(
        &self,
        sub: &Submission,
        now: Instant,
    ) -> Result<(StreamId, &dyn DynAggregate, (Loaded, Vec<NewEvent>)), Error> {
        let command = self
            .model
            .commands
            .get(&sub.command)
            .ok_or_else(|| rejected(codes::UNKNOWN, format!("no command {}", sub.command)))?;
        let agg = self.aggregate(command.aggregate())?;
        let stream = StreamId::new(
            command.aggregate(),
            command.stream(&sub.params).map_err(Error::Rejected)?,
        );
        let loaded = self.load(agg, &stream).await?;
        if loaded.last_kind.as_deref().is_some_and(|k| agg.ending(k)) {
            return Err(rejected(codes::ENDED, format!("{stream} has ended")));
        }
        let events = command
            .decide(&loaded.state, &sub.params, &sub.by, now)
            .map_err(Error::Fault)?
            .map_err(Error::Rejected)?;
        Ok((stream, agg, (loaded, events)))
    }

    /// The one way events are written: numbered from `expected`, stamped
    /// with `meta` and their current type version, and committed with the
    /// work they imply (`effects`), the work given beside it, its guards
    /// and, under a fingerprint, as the answer of `meta.key`. Every work row
    /// a commit changes is thereby implied by an event committed with it.
    pub(crate) async fn write(&self, w: Write) -> Result<Vec<Event>, StoreError> {
        let agg = self.model.aggregates.get(&w.stream.aggregate);
        let mut version = w.expected;
        let events: Vec<Event> = w
            .events
            .into_iter()
            .map(|e| {
                version = version.next();
                Event {
                    id: Uuid::now_v7(),
                    stream: w.stream.clone(),
                    version,
                    type_version: agg.map_or(1, |a| a.current_version(&e.kind)),
                    kind: e.kind,
                    data: e.data,
                    meta: w.meta.clone(),
                    position: Default::default(),
                }
            })
            .collect();
        let mut work: Vec<WorkChange> = events.iter().flat_map(effects).collect();
        work.extend(w.work);
        let answer = w.fingerprint.zip(w.meta.key).map(|(fingerprint, key)| {
            let result = AnswerResult::Applied(events.clone());
            (
                key,
                Answer {
                    fingerprint,
                    result,
                },
            )
        });
        // Even with no events the append checks the stream is still at
        // `expected`: the work given was decided on that version.
        let unit = Unit {
            appends: vec![Append {
                stream: w.stream,
                expected: w.expected,
                events,
            }],
            answer,
            work,
            guards: w.guards,
        };
        self.store.commit(unit).await
    }

    pub(crate) fn aggregate(&self, name: &str) -> Result<&dyn DynAggregate, Error> {
        self.model
            .aggregates
            .get(name)
            .map(|a| a.as_ref())
            .ok_or_else(|| Error::Fault(format!("no aggregate {name} declared")))
    }

    async fn load(&self, agg: &dyn DynAggregate, stream: &StreamId) -> Result<Loaded, Error> {
        let snapshot = self
            .store
            .snapshot(stream)
            .await?
            .filter(|s| s.code == agg.code());
        let (state, after) = match snapshot {
            Some(s) => (Some(s.state), s.version),
            None => (None, Version(0)),
        };
        let events = self.store.read_stream(stream, after).await?;
        let mut folded = Vec::with_capacity(events.len());
        for e in &events {
            folded.push((
                e.kind.clone(),
                agg.upcast(&e.kind, e.type_version, e.data.clone())
                    .map_err(Error::Fault)?,
            ));
        }
        let state = agg.fold(state, &folded).map_err(Error::Fault)?;
        let version = events.last().map_or(after, |e| e.version);
        // The last domain event: the engine's own `CommandScheduled` neither
        // ends a stream nor keeps it open.
        let domain = |e: &Event| (e.kind != SCHEDULED).then(|| e.kind.clone());
        let mut last_kind = events.iter().rev().find_map(domain);
        let mut at = after.0;
        while last_kind.is_none() && at > 0 {
            let read = self.store.read_stream(stream, Version(at - 1)).await?;
            last_kind = read.first().and_then(domain);
            at -= 1;
        }
        Ok(Loaded {
            state,
            version,
            last_kind,
            read: events.len() as u64,
        })
    }

    async fn keep_snapshot(
        &self,
        agg: &dyn DynAggregate,
        stream: &StreamId,
        loaded: Loaded,
        committed: &[Event],
    ) -> Result<(), Error> {
        if loaded.read + committed.len() as u64 <= self.options.snapshot_every {
            return Ok(());
        }
        let new: Vec<(String, Value)> = committed
            .iter()
            .map(|e| (e.kind.clone(), e.data.clone()))
            .collect();
        let state = agg.fold(Some(loaded.state), &new).map_err(Error::Fault)?;
        let version = committed.last().map_or(loaded.version, |e| e.version);
        let snapshot = Snapshot {
            version,
            code: agg.code().to_string(),
            state,
        };
        Ok(self.store.keep_snapshot(stream, snapshot).await?)
    }

    /// The stream's state, folded.
    pub async fn state(&self, stream: &StreamId) -> Result<Value, Error> {
        let agg = self.aggregate(&stream.aggregate)?;
        Ok(self.load(agg, stream).await?.state)
    }

    /// The stream's events, each in its current shape.
    pub async fn history(&self, stream: &StreamId) -> Result<Vec<Event>, Error> {
        let events = self.store.read_stream(stream, Version(0)).await?;
        events.into_iter().map(|e| self.upcast(e)).collect()
    }

    pub(crate) fn upcast(&self, mut e: Event) -> Result<Event, Error> {
        if let Some(agg) = self.model.aggregates.get(&e.stream.aggregate) {
            e.data = agg
                .upcast(&e.kind, e.type_version, e.data)
                .map_err(Error::Fault)?;
            e.type_version = agg.current_version(&e.kind);
        }
        Ok(e)
    }

    // ---- processes ----

    /// Starts the process `name` as `process/<id>`. Starting it again while
    /// it runs, with the same input, does nothing; once it has finished it
    /// is refused `ended`.
    pub async fn start(
        &self,
        name: &str,
        id: Uuid,
        input: impl Serialize,
        by: By,
    ) -> Result<StreamId, Error> {
        let input = serde_json::to_value(input)
            .map_err(|e| rejected(codes::INVALID_PARAMS, e.to_string()))?;
        self.start_at(name, id, input, by, self.clock.now()).await
    }

    async fn start_at(
        &self,
        name: &str,
        id: Uuid,
        input: Value,
        by: By,
        now: Instant,
    ) -> Result<StreamId, Error> {
        if !self.model.processes.contains_key(name) {
            return Err(rejected(codes::UNKNOWN, format!("no process {name}")));
        }
        let stream = StreamId::new(PROCESS, id);
        let mut races = Races::new();
        loop {
            let state = self.process_state(id).await?;
            if !state.name.is_empty() {
                if state.name != name || state.input != input {
                    return Err(rejected(
                        codes::KEY_REUSED,
                        format!("{stream} was started as another"),
                    ));
                }
                if state.finished.is_some() {
                    return Err(rejected(codes::ENDED, format!("{stream} has finished")));
                }
                return Ok(stream);
            }
            let started = ProcessEvent::Started {
                process: name.to_string(),
                input: input.clone(),
            };
            match self
                .record(
                    id,
                    Version(0),
                    vec![started],
                    &by,
                    now,
                    Vec::new(),
                    Vec::new(),
                )
                .await
            {
                Ok(()) => return Ok(stream),
                Err(StoreError::Conflict(_)) => races.lost(format!("starting {stream}")).await?,
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn process(&self, id: Uuid) -> Result<ProcessView, Error> {
        let state = self.process_state(id).await?;
        if state.name.is_empty() {
            return Err(rejected(
                codes::UNKNOWN,
                format!("no process {PROCESS}/{id}"),
            ));
        }
        let pending = state.pending().map(|(k, s)| (k.clone(), s.clone()));
        Ok(ProcessView {
            name: state.name,
            input: state.input,
            progress: state.progress,
            pending,
            finished: state.finished,
        })
    }

    pub(crate) async fn process_events(&self, id: Uuid) -> Result<(ProcessState, Version), Error> {
        let events = self
            .store
            .read_stream(&StreamId::new(PROCESS, id), Version(0))
            .await?;
        let state = ProcessState::fold(events.iter().map(|e| (e.kind.as_str(), &e.data)))
            .map_err(Error::Fault)?;
        Ok((state, events.last().map_or(Version(0), |e| e.version)))
    }

    async fn process_state(&self, id: Uuid) -> Result<ProcessState, Error> {
        Ok(self.process_events(id).await?.0)
    }

    /// Appends process events at `expected`, with the work they imply.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record(
        &self,
        id: Uuid,
        expected: Version,
        events: Vec<ProcessEvent>,
        by: &By,
        now: Instant,
        work: Vec<WorkChange>,
        guards: Vec<(WorkKind, Uuid, Uuid)>,
    ) -> Result<(), StoreError> {
        let events = events
            .into_iter()
            .map(|e| NewEvent {
                kind: e.kind().to_string(),
                data: e.data(),
            })
            .collect();
        let write = Write {
            stream: StreamId::new(PROCESS, id),
            expected,
            events,
            meta: Meta {
                now,
                by: by.clone(),
                key: None,
                command: PROCESS.to_string(),
            },
            fingerprint: None,
            work,
            guards,
        };
        self.write(write).await.map(|_| ())
    }

    // ---- the loops ----

    /// One pass of every loop: dispatch new events (process starts and
    /// awaited events), advance processes, deliver actions, run due
    /// schedules and recurring commands, end expired waits, and catch the
    /// projections up. A fault in one item sets that item aside and the
    /// rest goes on: an event that cannot be dispatched becomes a dead
    /// letter of [`DISPATCH`]; any other item keeps its row, tried again
    /// once its lease runs out. Answers how many things it did and the
    /// faults it set aside; only a store error ends it.
    pub async fn tick(&self) -> Result<Tick, Error> {
        let now = self.clock.now();
        let mut faults = Vec::new();
        let mut done = self.follow(Keeper::Dispatch, now, &mut faults).await?;
        done += self.advance_due(now, &mut faults).await?;
        done += self.deliver(now, &mut faults).await?;
        done += self.run_due(now, &mut faults).await?;
        done += self.expire_waits(now, &mut faults).await?;
        done += self.project(now).await?;
        Ok(Tick { done, faults })
    }

    /// Ticks until a tick does nothing (at most `SETTLE_TICKS` times),
    /// without moving the clock. A fault: every tick ran, but some item was
    /// set aside.
    pub async fn settle(&self) -> Result<(), Error> {
        let mut faults: Vec<Fault> = Vec::new();
        for _ in 0..SETTLE_TICKS {
            let tick = self.tick().await?;
            faults.extend(tick.faults);
            if tick.done == 0 {
                if faults.is_empty() {
                    return Ok(());
                }
                let list: Vec<String> = faults
                    .iter()
                    .map(|f| format!("{}: {}", f.item, f.error))
                    .collect();
                return Err(Error::Fault(list.join("; ")));
            }
        }
        Err(Error::Fault(format!(
            "settle: still busy after {SETTLE_TICKS} ticks"
        )))
    }

    /// Starts the processes `e` starts and answers the waits for it. A wait
    /// row that faults is set aside; the event goes on to the rest.
    pub(crate) async fn dispatch_event(
        &self,
        e: Event,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let mut done = 0;
        let e = self.upcast(e)?;
        for (name, process) in &self.model.processes {
            let Some(on) = process.starts_on() else {
                continue;
            };
            if on.aggregate == e.stream.aggregate && on.event == e.kind {
                let input = crate::work::event_json(&e);
                match self
                    .start_at(name, start_key(e.id, name), input, e.meta.by.clone(), now)
                    .await
                {
                    Ok(_) | Err(Error::Rejected(_)) => done += 1,
                    Err(err) => return Err(err),
                }
            }
        }
        done += self.arrived(&e, now, faults).await?;
        Ok(done)
    }
}

/// Events for one stream and what commits with them: see [`Engine::write`].
pub(crate) struct Write {
    pub stream: StreamId,
    pub expected: Version,
    pub events: Vec<NewEvent>,
    pub meta: Meta,
    /// Keeps the events as the answer of `meta.key`, under this fingerprint.
    pub fingerprint: Option<Uuid>,
    /// Work beside the events' effects, such as a release or a pace.
    pub work: Vec<WorkChange>,
    /// Rows that must still be held under these leases.
    pub guards: Vec<(WorkKind, Uuid, Uuid)>,
}

pub(crate) struct Loaded {
    state: Value,
    version: Version,
    last_kind: Option<String>,
    /// How many events the fold read past the snapshot.
    read: u64,
}

fn replay(answer: Answer, print: Uuid) -> Result<Applied, Error> {
    if answer.fingerprint != print {
        return Err(rejected(
            codes::KEY_REUSED,
            "the key was sent with another command or params",
        ));
    }
    match answer.result {
        AnswerResult::Applied(events) => Ok(Applied {
            events,
            replayed: true,
        }),
        AnswerResult::Rejected(r) => Err(Error::Rejected(r)),
    }
}
