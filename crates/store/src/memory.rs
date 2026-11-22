//! A store in memory: the reference every other store is held to by the
//! conformance suite. One lock; every method is one critical section.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use factum_core::{
    Event, Instant, Position, Query, StreamId, Table, Uuid, Value, Version, Work, WorkChange,
    WorkKind,
};

use crate::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Kept, Result, Snapshot, Store, Unit, apply_op,
    check_batch, check_unit, claim_row, op_target, release_row, wake_row,
};

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }
}

struct Row {
    work: Work,
    seq: u64,
    /// A wake that came while the row was claimed.
    woken: Option<Instant>,
}

#[derive(Default)]
struct Inner {
    streams: HashMap<StreamId, Vec<Event>>,
    log: Vec<Event>,
    answers: HashMap<Uuid, Kept>,
    work: BTreeMap<(WorkKind, Uuid), Row>,
    work_seq: u64,
    paces: HashMap<(String, String), Instant>,
    snapshots: HashMap<StreamId, Snapshot>,
    checkpoints: HashMap<String, Checkpoint>,
    /// The tables each projection was last built with.
    tables: HashMap<String, Vec<String>>,
    rows: BTreeMap<(String, String, String), Value>,
    dead: Vec<DeadLetter>,
}

impl Inner {
    fn version(&self, stream: &StreamId) -> Version {
        Version(self.streams.get(stream).map_or(0, |e| e.len() as u64))
    }

    fn check(&self, unit: &Unit) -> Result<()> {
        check_unit(
            unit,
            |stream| Ok(self.version(stream)),
            |key| Ok(self.answers.contains_key(&key)),
            |kind, id| Ok(self.work.get(&(kind, id)).and_then(|r| r.work.lease)),
        )
    }

    fn change(&mut self, change: WorkChange) {
        match change {
            WorkChange::Put(work) => {
                let key = (work.kind, work.id);
                if !self.work.contains_key(&key) {
                    self.work_seq += 1;
                    self.work.insert(
                        key,
                        Row {
                            work,
                            seq: self.work_seq,
                            woken: None,
                        },
                    );
                }
            }
            WorkChange::Wake { kind, id, due } => {
                if let Some(row) = self.work.get_mut(&(kind, id)) {
                    wake_row(&mut row.work, &mut row.woken, due);
                }
            }
            WorkChange::Done { kind, id } => {
                self.work.remove(&(kind, id));
            }
            WorkChange::Release { kind, id, due, .. } => {
                if let Some(row) = self.work.get_mut(&(kind, id)) {
                    release_row(&mut row.work, &mut row.woken, due);
                }
            }
            WorkChange::Pace {
                name,
                key,
                not_before,
            } => {
                self.paces.insert((name, key), not_before);
            }
        }
    }

    fn claimable(&self, row: &Row, claim: &Claim) -> bool {
        let w = &row.work;
        if w.kind != claim.kind || w.due > claim.now {
            return false;
        }
        if let Some(names) = &claim.names
            && !names.contains(&w.name)
        {
            return false;
        }
        if w.lease_until.is_some_and(|until| until > claim.now) {
            return false;
        }
        if let Some(key) = &w.ordering_key {
            if self
                .paces
                .get(&(w.name.clone(), key.clone()))
                .is_some_and(|p| *p > claim.now)
            {
                return false;
            }
            let first = self
                .work
                .values()
                .filter(|r| {
                    r.work.kind == w.kind
                        && r.work.name == w.name
                        && r.work.ordering_key.as_ref() == Some(key)
                })
                .map(|r| r.seq)
                .min();
            if first != Some(row.seq) {
                return false;
            }
        }
        true
    }
}

impl MemoryStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Store for MemoryStore {
    async fn read_stream(&self, stream: &StreamId, after: Version) -> Result<Vec<Event>> {
        let inner = self.lock();
        Ok(inner
            .streams
            .get(stream)
            .and_then(|e| e.get(after.0 as usize..))
            .map_or(Vec::new(), <[Event]>::to_vec))
    }

    async fn answer(&self, key: Uuid) -> Result<Option<Answer>> {
        let inner = self.lock();
        Ok(inner.answers.get(&key).cloned().map(|kept| {
            let events = match &kept.result {
                Ok(Some(stream)) => inner.streams[stream]
                    .iter()
                    .filter(|e| e.meta.key == Some(key))
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };
            kept.answer(events)
        }))
    }

    async fn commit(&self, unit: Unit) -> Result<Vec<Event>> {
        let mut inner = self.lock();
        inner.check(&unit)?;
        let mut committed = Vec::new();
        for append in unit.appends {
            for mut event in append.events {
                event.position = Position(0, inner.log.len() as u64 + 1);
                inner.log.push(event.clone());
                inner
                    .streams
                    .entry(append.stream.clone())
                    .or_default()
                    .push(event.clone());
                committed.push(event);
            }
        }
        if let Some((key, answer)) = unit.answer {
            inner.answers.insert(key, Kept::of(&answer));
        }
        for change in unit.work {
            inner.change(change);
        }
        Ok(committed)
    }

    async fn read_log(&self, after: Position, limit: usize) -> Result<Vec<Event>> {
        let inner = self.lock();
        let start = (after.1 as usize).min(inner.log.len());
        Ok(inner.log[start..].iter().take(limit).cloned().collect())
    }

    async fn claim(&self, claim: Claim) -> Result<Vec<Work>> {
        let mut inner = self.lock();
        let mut due: Vec<(Instant, u64, (WorkKind, Uuid))> = inner
            .work
            .iter()
            .filter(|(_, r)| inner.claimable(r, &claim))
            .map(|(k, r)| (r.work.due, r.seq, *k))
            .collect();
        due.sort();
        let mut claimed = Vec::new();
        for (_, _, key) in due.into_iter().take(claim.limit) {
            let row = inner.work.get_mut(&key).expect("row just listed");
            claim_row(&mut row.work, &mut row.woken, &claim);
            claimed.push(row.work.clone());
        }
        Ok(claimed)
    }

    async fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> Result<bool> {
        let mut inner = self.lock();
        match inner.work.get_mut(&(kind, id)) {
            Some(row)
                if row.work.lease == Some(lease)
                    && row.work.lease_until.is_some_and(|u| u > now) =>
            {
                row.work.lease_until = Some(until);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        Ok(self.lock().work.get(&(kind, id)).map(|r| r.work.clone()))
    }

    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        let inner = self.lock();
        let mut rows: Vec<&Row> = inner
            .work
            .values()
            .filter(|r| r.work.kind == kind && r.work.name == name)
            .collect();
        rows.sort_by_key(|r| r.seq);
        Ok(rows.into_iter().map(|r| r.work.clone()).collect())
    }

    async fn snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>> {
        Ok(self.lock().snapshots.get(stream).cloned())
    }

    async fn keep_snapshot(&self, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        let mut inner = self.lock();
        let keep = inner
            .snapshots
            .get(stream)
            .is_none_or(|s| s.version < snapshot.version || s.code != snapshot.code);
        if keep {
            inner.snapshots.insert(stream.clone(), snapshot);
        }
        Ok(())
    }

    async fn checkpoint(&self, projection: &str) -> Result<Option<Checkpoint>> {
        Ok(self.lock().checkpoints.get(projection).cloned())
    }

    async fn reset_projection(
        &self,
        projection: &str,
        version: u32,
        schema: &str,
        tables: &[Table],
    ) -> Result<()> {
        let mut inner = self.lock();
        inner.rows.retain(|(p, _, _), _| p != projection);
        let names = tables.iter().map(|t| t.name.clone()).collect();
        inner.tables.insert(projection.to_string(), names);
        inner.dead.retain(|d| d.projection != projection);
        inner.checkpoints.insert(
            projection.to_string(),
            Checkpoint {
                version,
                schema: schema.to_string(),
                position: Position::default(),
                stopped: false,
            },
        );
        Ok(())
    }

    async fn apply_projection(&self, batch: Batch) -> Result<()> {
        let mut inner = self.lock();
        let p = &batch.projection;
        let declared = inner.tables.get(p).map_or(&[][..], Vec::as_slice);
        check_batch(
            &batch,
            inner.checkpoints.get(p).map(|c| (c.version, c.position)),
            |e| {
                Ok(inner
                    .dead
                    .iter()
                    .any(|d| d.projection == *p && d.event == e))
            },
            |table| Ok(declared.iter().any(|d| d == table)),
        )?;
        for op in &batch.ops {
            let (table, key) = op_target(op);
            let k = (p.clone(), table.to_string(), key.to_string());
            match apply_op(inner.rows.remove(&k), op) {
                Some(row) => inner.rows.insert(k, row),
                None => None,
            };
        }
        inner
            .dead
            .retain(|d| d.projection != *p || !batch.resolved.contains(&d.event));
        inner.dead.extend(batch.dead);
        if let Some(c) = inner.checkpoints.get_mut(p) {
            c.position = batch.to;
            c.stopped = batch.stop;
        }
        Ok(())
    }

    async fn query(&self, projection: &str, query: &Query) -> Result<Vec<(String, Value)>> {
        let rows = self.rows(projection, &query.table).await?;
        Ok(factum_core::answer(rows, query))
    }

    async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>> {
        let inner = self.lock();
        Ok(inner
            .rows
            .iter()
            .filter(|((p, t, _), _)| p == projection && t == table)
            .map(|((_, _, k), v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        let k = (projection.to_string(), table.to_string(), key.to_string());
        Ok(self.lock().rows.get(&k).cloned())
    }

    async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>> {
        Ok(self
            .lock()
            .dead
            .iter()
            .filter(|d| d.projection == projection)
            .cloned()
            .collect())
    }
}
