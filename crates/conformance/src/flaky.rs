//! A store that crashes on cue, and lets another writer in on cue: how a
//! scenario makes a runner die between two writes, or lose a race.

use std::sync::{Arc, Mutex};

use factum::{BoxFuture, Instant, StreamId, Uuid, Value, Version};
use factum_core::{Event, Position, Work, WorkKind};
use factum_store::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Result, Snapshot, Store, StoreError, Unit,
};

type Matches = Box<dyn Fn(&Unit) -> bool + Send + Sync>;

pub struct Flaky<S> {
    inner: Arc<S>,
    crash: Mutex<Option<Matches>>,
    race: Mutex<Option<(Matches, BoxFuture<()>)>>,
    batch_race: Mutex<Option<(String, BoxFuture<()>)>>,
}

impl<S: Store> Flaky<S> {
    pub fn new(inner: Arc<S>) -> Flaky<S> {
        Flaky {
            inner,
            crash: Mutex::new(None),
            race: Mutex::new(None),
            batch_race: Mutex::new(None),
        }
    }

    /// Before the next catch-up batch of `projection`, `writer` runs to
    /// completion, as another projector that got there first.
    pub fn race_on_batch(&self, projection: &str, writer: BoxFuture<()>) {
        *self.batch_race.lock().unwrap() = Some((projection.to_string(), writer));
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

impl<S: Store> Store for Flaky<S> {
    async fn read_stream(&self, stream: &StreamId, after: Version) -> Result<Vec<Event>> {
        self.inner.read_stream(stream, after).await
    }
    async fn answer(&self, key: Uuid) -> Result<Option<Answer>> {
        self.inner.answer(key).await
    }
    async fn commit(&self, unit: Unit) -> Result<Vec<Event>> {
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
        self.inner.commit(unit).await
    }
    async fn read_log(&self, after: Position, limit: usize) -> Result<Vec<Event>> {
        self.inner.read_log(after, limit).await
    }
    async fn claim(&self, claim: Claim) -> Result<Vec<Work>> {
        self.inner.claim(claim).await
    }
    async fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> Result<bool> {
        self.inner.renew(kind, id, lease, now, until).await
    }
    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        self.inner.work(kind, id).await
    }
    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        self.inner.work_named(kind, name).await
    }
    async fn snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>> {
        self.inner.snapshot(stream).await
    }
    async fn keep_snapshot(&self, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        self.inner.keep_snapshot(stream, snapshot).await
    }
    async fn checkpoint(&self, projection: &str) -> Result<Option<Checkpoint>> {
        self.inner.checkpoint(projection).await
    }
    async fn reset_projection(&self, projection: &str, version: u32) -> Result<()> {
        self.inner.reset_projection(projection, version).await
    }
    async fn apply_projection(&self, batch: Batch) -> Result<()> {
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
        self.inner.rows(projection, table).await
    }
    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        self.inner.row(projection, table, key).await
    }
    async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>> {
        self.inner.dead_letters(projection).await
    }
}
