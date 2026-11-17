use std::sync::Arc;

use factum_core::{
    By, DynAggregate, Event, Finished, Instant, Meta, NewEvent, OnFailure, PROCESS, ProcessEvent,
    ProcessState, Progress, Rejection, SCHEDULED, Step, StreamId, Table, Uuid, Value, Version,
    codes, effects, fingerprint, start_key,
};
use factum_store::{
    Answer, AnswerResult, Append, Batch, Checkpoint, DeadLetter, Snapshot, Store, StoreError, Unit,
};
use serde::Serialize;
use serde_json::json;

use crate::clock::Clock;
use crate::model::Model;

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

pub(crate) fn rejected(code: &str, message: impl Into<String>) -> Error {
    Error::Rejected(Rejection::new(code, message))
}

/// A command sent under an idempotency key.
#[derive(Clone, Debug)]
pub struct Submission {
    pub command: String,
    pub params: Value,
    /// Sending the same key again answers the first answer.
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

/// Where the engine keeps how far it has dispatched the log.
const DISPATCH: &str = "factum.dispatch";
/// How often one writer decides again after losing a race. Every loss is
/// another writer's win, so the log always moves; the bound only keeps one
/// writer from losing forever.
const MAX_RETRIES: usize = 1000;

/// Lets the other tasks run once: a writer that lost a race lets the
/// winners finish before it decides again. Needs no particular runtime.
pub(crate) async fn yield_now() {
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
        self.submit_at(submission, self.clock.now()).await
    }

    pub(crate) async fn submit_at(&self, sub: Submission, now: Instant) -> Result<Applied, Error> {
        let print = fingerprint(&sub.command, &sub.params);
        for _ in 0..MAX_RETRIES {
            if let Some(answer) = self.store.answer(sub.key).await? {
                return replay(answer, print);
            }
            let (stream, agg, decided) = match self.decide(&sub, now).await {
                Ok(d) => d,
                Err(Error::Rejected(r)) => {
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
                        Ok(_) | Err(StoreError::KeyTaken(_)) => {}
                        Err(e) => return Err(e.into()),
                    }
                    return Err(Error::Rejected(r));
                }
                Err(e) => return Err(e),
            };
            let (loaded, new) = decided;
            let events = self.events(
                &stream,
                loaded.version,
                agg,
                new,
                &sub.by,
                Some(sub.key),
                &sub.command,
                now,
            );
            let unit = Unit {
                appends: vec![Append {
                    stream: stream.clone(),
                    expected: loaded.version,
                    events: events.clone(),
                }],
                answer: Some((
                    sub.key,
                    Answer {
                        fingerprint: print,
                        result: AnswerResult::Applied(events.clone()),
                    },
                )),
                work: events.iter().flat_map(effects).collect(),
                guards: Vec::new(),
            };
            match self.store.commit(unit).await {
                Ok(committed) => {
                    self.keep_snapshot(agg, &stream, loaded, &committed).await?;
                    return Ok(Applied {
                        events: committed,
                        replayed: false,
                    });
                }
                Err(StoreError::Conflict(_) | StoreError::KeyTaken(_)) => yield_now().await,
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::Fault(format!(
            "{}: gave up after {MAX_RETRIES} conflicts",
            sub.command
        )))
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
            .map_err(Error::Rejected)?;
        Ok((stream, agg, (loaded, events)))
    }

    #[allow(clippy::too_many_arguments)]
    fn events(
        &self,
        stream: &StreamId,
        at: Version,
        agg: &dyn DynAggregate,
        new: Vec<NewEvent>,
        by: &By,
        key: Option<Uuid>,
        command: &str,
        now: Instant,
    ) -> Vec<Event> {
        let mut version = at;
        new.into_iter()
            .map(|e| {
                version = version.next();
                Event {
                    id: Uuid::now_v7(),
                    stream: stream.clone(),
                    version,
                    type_version: agg.current_version(&e.kind),
                    kind: e.kind,
                    data: e.data,
                    meta: Meta {
                        now,
                        by: by.clone(),
                        key,
                        command: command.to_string(),
                    },
                    position: Default::default(),
                }
            })
            .collect()
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
        for _ in 0..MAX_RETRIES {
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
                Err(StoreError::Conflict(_)) => yield_now().await,
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::Fault(format!("{stream}: gave up starting")))
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
        work: Vec<factum_core::WorkChange>,
        guards: Vec<(factum_core::WorkKind, Uuid, Uuid)>,
    ) -> Result<(), StoreError> {
        let stream = StreamId::new(PROCESS, id);
        let mut version = expected;
        let events: Vec<Event> = events
            .into_iter()
            .map(|e| {
                version = version.next();
                Event {
                    id: Uuid::now_v7(),
                    stream: stream.clone(),
                    version,
                    kind: e.kind().to_string(),
                    type_version: 1,
                    data: e.data(),
                    meta: Meta {
                        now,
                        by: by.clone(),
                        key: None,
                        command: PROCESS.to_string(),
                    },
                    position: Default::default(),
                }
            })
            .collect();
        let mut changes: Vec<_> = events.iter().flat_map(effects).collect();
        changes.extend(work);
        let unit = Unit {
            appends: vec![Append {
                stream,
                expected,
                events,
            }],
            answer: None,
            work: changes,
            guards,
        };
        self.store.commit(unit).await.map(|_| ())
    }

    // ---- the loops ----

    /// One pass of every loop: dispatch new events (process starts and
    /// awaited events), advance processes, deliver actions, run due
    /// schedules and recurring commands, end expired waits, and catch the
    /// projections up. Answers how many things it did.
    pub async fn tick(&self) -> Result<usize, Error> {
        let now = self.clock.now();
        let mut done = self.dispatch(now).await?;
        done += self.advance_due(now).await?;
        done += self.deliver(now).await?;
        done += self.run_due(now).await?;
        done += self.expire_waits(now).await?;
        done += self.project().await?;
        Ok(done)
    }

    /// Ticks until a tick does nothing (at most `rounds` times), without
    /// moving the clock.
    pub async fn settle(&self) -> Result<(), Error> {
        for _ in 0..1000 {
            if self.tick().await? == 0 {
                return Ok(());
            }
        }
        Err(Error::Fault("settle: still busy after 1000 ticks".into()))
    }

    async fn dispatch(&self, now: Instant) -> Result<usize, Error> {
        let mut at = self.checkpoint_of(DISPATCH, 1, &[]).await?;
        let mut done = 0;
        loop {
            let events = self.store.read_log(at.position, self.options.batch).await?;
            let Some(last) = events.last().map(|e| e.position) else {
                return Ok(done);
            };
            for e in events {
                done += self.dispatch_event(e, now).await?;
            }
            let batch = Batch {
                projection: DISPATCH.into(),
                version: 1,
                from: at.position,
                to: last,
                ops: Vec::new(),
                dead: Vec::new(),
                stop: false,
            };
            match self.store.apply_projection(batch).await {
                Ok(()) => at.position = last,
                Err(StoreError::Conflict(_)) => return Ok(done),
                Err(e) => return Err(e.into()),
            }
        }
    }

    async fn dispatch_event(&self, e: Event, now: Instant) -> Result<usize, Error> {
        let mut done = 0;
        let e = self.upcast(e)?;
        for (name, process) in &self.model.processes {
            let Some(on) = process.starts_on() else {
                continue;
            };
            if on.aggregate == e.stream.aggregate && on.event == e.kind {
                let input =
                    json!({ "id": e.id, "stream": e.stream, "type": e.kind, "data": e.data });
                match self
                    .start_at(name, start_key(e.id, name), input, e.meta.by.clone(), now)
                    .await
                {
                    Ok(_) | Err(Error::Rejected(_)) => done += 1,
                    Err(err) => return Err(err),
                }
            }
        }
        done += self.arrived(&e, now).await?;
        Ok(done)
    }

    /// Where the projection `name` stands, built afresh if its version or its
    /// tables changed since it was built.
    pub(crate) async fn checkpoint_of(
        &self,
        name: &str,
        version: u32,
        tables: &[Table],
    ) -> Result<Checkpoint, Error> {
        let schema = serde_json::to_string(tables).map_err(|e| Error::Fault(e.to_string()))?;
        match self.store.checkpoint(name).await? {
            Some(c) if c.version == version && c.schema == schema => Ok(c),
            _ => {
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
        }
    }

    async fn project(&self) -> Result<usize, Error> {
        let mut done = 0;
        for p in &self.model.projections {
            let tables = p.tables();
            let mut at = self.checkpoint_of(p.name(), p.version(), &tables).await?;
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
                    let result = self.project_one(p.as_ref(), &tables, e);
                    match result {
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
                    stop,
                };
                match self.store.apply_projection(batch).await {
                    Ok(()) => {
                        at.position = to;
                        at.stopped = stop;
                    }
                    Err(StoreError::Conflict(_)) => break,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(done)
    }

    // ---- reads ----

    pub async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>, Error> {
        Ok(self.store.rows(projection, table).await?)
    }

    pub async fn row(
        &self,
        projection: &str,
        table: &str,
        key: &str,
    ) -> Result<Option<Value>, Error> {
        Ok(self.store.row(projection, table, key).await?)
    }

    pub async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>, Error> {
        Ok(self.store.dead_letters(projection).await?)
    }
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
