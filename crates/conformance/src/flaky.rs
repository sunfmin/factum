//! A store that crashes on cue, fails any call on cue, and lets another
//! writer in on cue: how a scenario makes a runner die between two writes,
//! meet a broken store, or lose a race.

use std::sync::{Arc, Mutex};

use factum::{BoxFuture, Instant, StreamId, Uuid, Value, Version};
use factum_core::{Event, Position, Query, Table, Work, WorkChange, WorkKind};
use factum_store::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Result, Snapshot, Store, StoreError, Unit,
};

type Matches = Box<dyn Fn(&Unit) -> bool + Send + Sync>;
type Pause = Box<dyn Fn(String) -> BoxFuture<()> + Send + Sync>;

/// A `Store` method, to cue a failure on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Call {
    ReadStream,
    Answer,
    Commit,
    ReadLog,
    Claim,
    Renew,
    Work,
    WorkNamed,
    Snapshot,
    KeepSnapshot,
    Checkpoint,
    ResetProjection,
    Query,
    ApplyProjection,
    Rows,
    Row,
    DeadLetters,
}

impl Call {
    pub const ALL: [Call; 17] = [
        Call::ReadStream,
        Call::Answer,
        Call::Commit,
        Call::ReadLog,
        Call::Claim,
        Call::Renew,
        Call::Work,
        Call::WorkNamed,
        Call::Snapshot,
        Call::KeepSnapshot,
        Call::Checkpoint,
        Call::ResetProjection,
        Call::Query,
        Call::ApplyProjection,
        Call::Rows,
        Call::Row,
        Call::DeadLetters,
    ];

    /// The error a call cued to fail answers.
    pub fn error(self) -> StoreError {
        StoreError::Backend(format!("{self:?} failed on cue"))
    }
}

pub struct Flaky<S> {
    inner: Arc<S>,
    crash: Mutex<Option<Matches>>,
    fail: Mutex<Option<Call>>,
    stop: Mutex<Option<u32>>,
    race: Mutex<Option<(Matches, BoxFuture<()>)>>,
    lose: Mutex<Option<Matches>>,
    batch_race: Mutex<Option<(String, BoxFuture<()>)>>,
    pause: Option<Pause>,
}

impl<S: Store> Flaky<S> {
    pub fn new(inner: Arc<S>) -> Flaky<S> {
        Flaky {
            inner,
            crash: Mutex::new(None),
            fail: Mutex::new(None),
            stop: Mutex::new(None),
            race: Mutex::new(None),
            lose: Mutex::new(None),
            batch_race: Mutex::new(None),
            pause: None,
        }
    }

    /// After every commit, and every claim that took a row, awaits
    /// `pause(what it wrote)`: the store's writes, one at a time.
    pub fn pausing(
        mut self,
        pause: impl Fn(String) -> BoxFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.pause = Some(Box::new(pause));
        self
    }

    async fn paused(&self, what: impl FnOnce() -> String) {
        if let Some(pause) = &self.pause {
            pause(what()).await;
        }
    }

    /// Before the next catch-up batch of `projection`, `writer` runs to
    /// completion, as another projector that got there first.
    pub fn race_on_batch(&self, projection: &str, writer: BoxFuture<()>) {
        *self.batch_race.lock().unwrap() = Some((projection.to_string(), writer));
    }

    /// The next call of `call` fails with `call.error()`, doing nothing.
    pub fn fail_on(&self, call: Call) {
        *self.fail.lock().unwrap() = Some(call);
    }

    /// Whether the call cued with `fail_on` has been made and failed.
    pub fn failed(&self) -> bool {
        self.fail.lock().unwrap().is_none()
    }

    /// After `calls` more calls, every call fails, doing nothing: the
    /// runner holding this handle has stopped, between any two of them.
    pub fn stop_after(&self, calls: u32) {
        *self.stop.lock().unwrap() = Some(calls);
    }

    /// Whether a runner cued with `stop_after` has stopped.
    pub fn stopped(&self) -> bool {
        *self.stop.lock().unwrap() == Some(0)
    }

    fn cue(&self, call: Call) -> Result<()> {
        if let Some(left) = self.stop.lock().unwrap().as_mut() {
            if *left == 0 {
                return Err(StoreError::Backend("the runner stopped".into()));
            }
            *left -= 1;
        }
        let mut f = self.fail.lock().unwrap();
        if *f == Some(call) {
            *f = None;
            return Err(call.error());
        }
        Ok(())
    }

    /// The next commit that matches fails, writing nothing, as if the
    /// process died before it.
    pub fn crash_on(&self, matches: impl Fn(&Unit) -> bool + Send + Sync + 'static) {
        *self.crash.lock().unwrap() = Some(Box::new(matches));
    }

    /// Before the next commit that matches, `writer` runs to completion, as
    /// another writer that got there first.
    pub fn race_on(
        &self,
        matches: impl Fn(&Unit) -> bool + Send + Sync + 'static,
        writer: BoxFuture<()>,
    ) {
        *self.race.lock().unwrap() = Some((Box::new(matches), writer));
    }

    /// From now on, every commit that matches loses its race, writing
    /// nothing, as if another writer always got to its stream first.
    pub fn lose_every(&self, matches: impl Fn(&Unit) -> bool + Send + Sync + 'static) {
        *self.lose.lock().unwrap() = Some(Box::new(matches));
    }
}

/// Whether the unit records the outcome of the step `key`.
pub fn records_step(unit: &Unit, key: &str) -> bool {
    unit.appends
        .iter()
        .flat_map(|a| &a.events)
        .any(|e| e.kind == "StepRecorded" && e.data["key"] == key)
}

/// Whether the unit appends an event of type `kind`.
pub fn appends(unit: &Unit, kind: &str) -> bool {
    unit.appends
        .iter()
        .flat_map(|a| &a.events)
        .any(|e| e.kind == kind)
}

/// What a unit writes, in short: each event's type and each work change.
fn written(unit: &Unit) -> String {
    let events = unit.appends.iter().flat_map(|a| {
        a.events.iter().map(move |e| match e.data["key"].as_str() {
            Some(key) => format!(
                "{}/{} {} {key}",
                a.stream.aggregate,
                short(a.stream.id),
                e.kind
            ),
            None => format!("{}/{} {}", a.stream.aggregate, short(a.stream.id), e.kind),
        })
    });
    let work = unit.work.iter().map(|w| match w {
        WorkChange::Put(r) => format!("put {:?} {}", r.kind, short(r.id)),
        WorkChange::Wake { kind, id, .. } => format!("wake {kind:?} {}", short(*id)),
        WorkChange::Done { kind, id } => format!("done {kind:?} {}", short(*id)),
        WorkChange::Release { kind, id, .. } => format!("release {kind:?} {}", short(*id)),
        WorkChange::Pace { name, .. } => format!("pace {name}"),
    });
    events.chain(work).collect::<Vec<_>>().join("; ")
}

/// The first eight hex digits of an id.
pub fn short(id: Uuid) -> String {
    id.simple().to_string()[..8].to_string()
}

impl<S: Store> Store for Flaky<S> {
    async fn read_stream(&self, stream: &StreamId, after: Version) -> Result<Vec<Event>> {
        self.cue(Call::ReadStream)?;
        self.inner.read_stream(stream, after).await
    }
    async fn answer(&self, key: Uuid) -> Result<Option<Answer>> {
        self.cue(Call::Answer)?;
        self.inner.answer(key).await
    }
    async fn commit(&self, unit: Unit) -> Result<Vec<Event>> {
        self.cue(Call::Commit)?;
        let crash = {
            let mut c = self.crash.lock().unwrap();
            if c.as_ref().is_some_and(|m| m(&unit)) {
                c.take().is_some()
            } else {
                false
            }
        };
        if crash {
            return Err(StoreError::Backend("crashed before the commit".into()));
        }
        let writer = {
            let mut r = self.race.lock().unwrap();
            if r.as_ref().is_some_and(|(m, _)| m(&unit)) {
                r.take().map(|(_, w)| w)
            } else {
                None
            }
        };
        if let Some(writer) = writer {
            writer.await;
        }
        if self.lose.lock().unwrap().as_ref().is_some_and(|m| m(&unit)) {
            let stream = unit.appends.first().expect("a unit that loses appends");
            return Err(StoreError::Conflict(stream.stream.clone()));
        }
        let what = written(&unit);
        let result = self.inner.commit(unit).await;
        self.paused(|| match &result {
            Ok(_) => format!("commits {what}"),
            Err(e) => format!("fails to commit {what}: {e}"),
        })
        .await;
        result
    }
    async fn read_log(&self, after: Position, limit: usize) -> Result<Vec<Event>> {
        self.cue(Call::ReadLog)?;
        self.inner.read_log(after, limit).await
    }
    async fn claim(&self, claim: Claim) -> Result<Vec<Work>> {
        self.cue(Call::Claim)?;
        let kind = claim.kind;
        let rows = self.inner.claim(claim).await?;
        if !rows.is_empty() {
            let names: Vec<String> = rows
                .iter()
                .map(|r| format!("{} {}", r.name, short(r.id)))
                .collect();
            self.paused(|| format!("claims {kind:?}: {}", names.join(", ")))
                .await;
        }
        Ok(rows)
    }
    async fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> Result<bool> {
        self.cue(Call::Renew)?;
        self.inner.renew(kind, id, lease, now, until).await
    }
    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        self.cue(Call::Work)?;
        self.inner.work(kind, id).await
    }
    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        self.cue(Call::WorkNamed)?;
        self.inner.work_named(kind, name).await
    }
    async fn snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>> {
        self.cue(Call::Snapshot)?;
        self.inner.snapshot(stream).await
    }
    async fn keep_snapshot(&self, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        self.cue(Call::KeepSnapshot)?;
        self.inner.keep_snapshot(stream, snapshot).await
    }
    async fn checkpoint(&self, projection: &str) -> Result<Option<Checkpoint>> {
        self.cue(Call::Checkpoint)?;
        self.inner.checkpoint(projection).await
    }
    async fn reset_projection(
        &self,
        projection: &str,
        version: u32,
        schema: &str,
        tables: &[Table],
    ) -> Result<()> {
        self.cue(Call::ResetProjection)?;
        self.inner
            .reset_projection(projection, version, schema, tables)
            .await
    }
    async fn query(&self, projection: &str, query: &Query) -> Result<Vec<(String, Value)>> {
        self.cue(Call::Query)?;
        self.inner.query(projection, query).await
    }
    async fn apply_projection(&self, batch: Batch) -> Result<()> {
        self.cue(Call::ApplyProjection)?;
        let writer = {
            let mut r = self.batch_race.lock().unwrap();
            if r.as_ref().is_some_and(|(p, _)| *p == batch.projection) {
                r.take().map(|(_, w)| w)
            } else {
                None
            }
        };
        if let Some(writer) = writer {
            writer.await;
        }
        self.inner.apply_projection(batch).await
    }
    async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>> {
        self.cue(Call::Rows)?;
        self.inner.rows(projection, table).await
    }
    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        self.cue(Call::Row)?;
        self.inner.row(projection, table, key).await
    }
    async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>> {
        self.cue(Call::DeadLetters)?;
        self.inner.dead_letters(projection).await
    }
}
