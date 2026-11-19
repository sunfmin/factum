//! The store contract: everything factum's engine needs from a backend, and
//! nothing about how the backend keeps it. The guarantees are stated here
//! and proved by `factum-conformance`, which runs the same scenarios through
//! the engine on every store.

use std::future::Future;

use factum_core::{
    Event, Instant, Position, Query, Rejection, RowOp, StreamId, Table, Uuid, Value, Version, Work,
    WorkChange, WorkKind,
};
use serde::{Deserialize, Serialize};

mod memory;
pub use memory::MemoryStore;

#[derive(Clone, Debug, PartialEq)]
pub enum StoreError {
    /// A stream was not at the version its writer read.
    Conflict(StreamId),
    /// The submission's key already has an answer.
    KeyTaken(Uuid),
    /// A guarded or released row is not held under the given lease.
    LeaseLost,
    /// A projection batch was made from a checkpoint that has moved, or
    /// resolves a dead letter that is gone: another projector got there
    /// first. Nothing of the batch was written.
    Stale(String),
    /// A row op names a table its projection did not declare when it was
    /// last built. Nothing of the batch was written.
    Undeclared { projection: String, table: String },
    /// The data was written by a later version of the store, whose schema
    /// (`found`) this one, which knows schemas up to `known`, cannot read.
    /// Refused when the store is opened, before any call.
    NewerSchema { found: u32, known: u32 },
    /// The backend failed; nothing of the unit was written.
    Backend(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Conflict(s) => write!(f, "conflict on {s}"),
            StoreError::KeyTaken(k) => write!(f, "key {k} already answered"),
            StoreError::LeaseLost => write!(f, "lease lost"),
            StoreError::Stale(p) => write!(f, "{p} moved since the batch began"),
            StoreError::Undeclared { projection, table } => {
                write!(f, "{projection} has no table {table}")
            }
            StoreError::NewerSchema { found, known } => write!(
                f,
                "the store holds schema {found}, newer than this factum knows ({known})"
            ),
            StoreError::Backend(e) => write!(f, "store: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub type Result<T> = std::result::Result<T, StoreError>;

/// What a submission's key answers, every time it is sent again.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Answer {
    /// The command and params it was first sent with.
    pub fingerprint: Uuid,
    pub result: AnswerResult,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AnswerResult {
    Applied(Vec<Event>),
    Rejected(Rejection),
}

/// Events for one stream, appended only if it is still at `expected`.
#[derive(Clone, Debug)]
pub struct Append {
    pub stream: StreamId,
    pub expected: Version,
    pub events: Vec<Event>,
}

/// Everything one commit writes. All of it, or none.
#[derive(Clone, Debug, Default)]
pub struct Unit {
    pub appends: Vec<Append>,
    pub answer: Option<(Uuid, Answer)>,
    pub work: Vec<WorkChange>,
    /// Rows that must still be held under these leases.
    pub guards: Vec<(WorkKind, Uuid, Uuid)>,
}

impl Unit {
    /// Every lease the unit needs still held: its guards, then its releases.
    pub fn leases(&self) -> impl Iterator<Item = (WorkKind, Uuid, Uuid)> + '_ {
        let releases = self.work.iter().filter_map(|c| match c {
            WorkChange::Release {
                kind, id, lease, ..
            } => Some((*kind, *id, *lease)),
            _ => None,
        });
        self.guards.iter().copied().chain(releases)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: Version,
    /// The code that folded it.
    pub code: String,
    pub state: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    pub version: u32,
    /// The tables the projection declared when it was built, as JSON: a
    /// change rebuilds it.
    pub schema: String,
    pub position: Position,
    /// Held at a dead letter, by a projection that stops on failure.
    pub stopped: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub projection: String,
    pub position: Position,
    pub event: Uuid,
    pub stream: StreamId,
    pub version: Version,
    pub error: String,
}

/// One move of a projection: its rows, its dead letters and its checkpoint,
/// together. A catch-up and a retried dead letter are both one.
#[derive(Clone, Debug)]
pub struct Batch {
    pub projection: String,
    pub version: u32,
    /// `Stale` unless the checkpoint is here.
    pub from: Position,
    pub to: Position,
    pub ops: Vec<RowOp>,
    /// New dead letters.
    pub dead: Vec<DeadLetter>,
    /// Dead letters (by event) retried in this batch, removed with it;
    /// `Stale` if one is gone.
    pub resolved: Vec<Uuid>,
    /// Whether the checkpoint is held at a dead letter after this batch.
    pub stop: bool,
}

/// Which work rows to claim.
#[derive(Clone, Debug)]
pub struct Claim {
    pub kind: WorkKind,
    /// Only rows with one of these names, if given.
    pub names: Option<Vec<String>>,
    pub now: Instant,
    pub holder: String,
    pub lease_ms: i64,
    pub limit: usize,
}

/// A backend. Every method is one short transaction; none holds a lock
/// while the caller works.
pub trait Store: Send + Sync + 'static {
    /// The stream's events after `after`, in version order.
    fn read_stream(
        &self,
        stream: &StreamId,
        after: Version,
    ) -> impl Future<Output = Result<Vec<Event>>> + Send;

    fn answer(&self, key: Uuid) -> impl Future<Output = Result<Option<Answer>>> + Send;

    /// Writes the unit, answering its events as committed (with positions);
    /// an applied answer is kept with those positions too.
    /// `Conflict` if a stream moved, `KeyTaken` if the key has an answer,
    /// `LeaseLost` if a guard or release does not hold.
    fn commit(&self, unit: Unit) -> impl Future<Output = Result<Vec<Event>>> + Send;

    /// Up to `limit` events after `after` in commit order, every one final:
    /// no later commit can land before the last one returned.
    fn read_log(
        &self,
        after: Position,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Event>>> + Send;

    /// Claims due rows no live lease holds, oldest due first. Of the rows
    /// sharing a name and ordering key only the first put is claimable, and
    /// not before its pace. Each claim takes a new lease and counts an attempt.
    fn claim(&self, claim: Claim) -> impl Future<Output = Result<Vec<Work>>> + Send;

    /// Extends a live lease to `until`; false if it is no longer held.
    fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> impl Future<Output = Result<bool>> + Send;

    fn work(&self, kind: WorkKind, id: Uuid) -> impl Future<Output = Result<Option<Work>>> + Send;

    fn work_named(
        &self,
        kind: WorkKind,
        name: &str,
    ) -> impl Future<Output = Result<Vec<Work>>> + Send;

    fn snapshot(&self, stream: &StreamId) -> impl Future<Output = Result<Option<Snapshot>>> + Send;

    /// Keeps it unless a later version is kept already.
    fn keep_snapshot(
        &self,
        stream: &StreamId,
        snapshot: Snapshot,
    ) -> impl Future<Output = Result<()>> + Send;

    fn checkpoint(
        &self,
        projection: &str,
    ) -> impl Future<Output = Result<Option<Checkpoint>>> + Send;

    /// Drops the projection's rows and dead letters; its checkpoint starts
    /// over at `version`.
    fn reset_projection(
        &self,
        projection: &str,
        version: u32,
        schema: &str,
        tables: &[Table],
    ) -> impl Future<Output = Result<()>> + Send;

    /// Writes the batch, all of it or none: `Stale` unless the checkpoint is
    /// at the batch's version and `from` and every resolved dead letter is
    /// there; `Undeclared` if an op names a table the projection was not
    /// built with.
    fn apply_projection(&self, batch: Batch) -> impl Future<Output = Result<()>> + Send;

    /// Answers a query over one of the projection's tables. The engine has
    /// checked that it names only declared columns.
    fn query(
        &self,
        projection: &str,
        query: &Query,
    ) -> impl Future<Output = Result<Vec<(String, Value)>>> + Send;

    fn rows(
        &self,
        projection: &str,
        table: &str,
    ) -> impl Future<Output = Result<Vec<(String, Value)>>> + Send;

    fn row(
        &self,
        projection: &str,
        table: &str,
        key: &str,
    ) -> impl Future<Output = Result<Option<Value>>> + Send;

    fn dead_letters(
        &self,
        projection: &str,
    ) -> impl Future<Output = Result<Vec<DeadLetter>>> + Send;
}

/// Decides whether a unit commits, the way every store must: `Conflict` if a
/// stream is not at the version its writer read, then `KeyTaken` if the key
/// has an answer, then `LeaseLost` if a guard or release does not hold. The
/// store supplies what it keeps: a stream's version, whether a key is
/// answered, and a row's lease.
pub fn check_unit(
    unit: &Unit,
    mut version: impl FnMut(&StreamId) -> Result<Version>,
    answered: impl FnOnce(Uuid) -> Result<bool>,
    mut lease: impl FnMut(WorkKind, Uuid) -> Result<Option<Uuid>>,
) -> Result<()> {
    for a in &unit.appends {
        if version(&a.stream)? != a.expected {
            return Err(StoreError::Conflict(a.stream.clone()));
        }
    }
    if let Some((key, _)) = &unit.answer
        && answered(*key)?
    {
        return Err(StoreError::KeyTaken(*key));
    }
    for (kind, id, held) in unit.leases() {
        if lease(kind, id)? != Some(held) {
            return Err(StoreError::LeaseLost);
        }
    }
    Ok(())
}

/// Decides whether a batch applies, the way every store must: `Stale` unless
/// the checkpoint is at the batch's version and `from` and every resolved
/// dead letter is there, then `Undeclared` if an op names a table the
/// projection was not built with. The store supplies its checkpoint's
/// version and position, whether a dead letter (by event) is there, and
/// whether a table is declared.
pub fn check_batch(
    batch: &Batch,
    at: Option<(u32, Position)>,
    mut dead: impl FnMut(Uuid) -> Result<bool>,
    mut declared: impl FnMut(&str) -> Result<bool>,
) -> Result<()> {
    let mut stale = at != Some((batch.version, batch.from));
    for e in &batch.resolved {
        stale = stale || !dead(*e)?;
    }
    if stale {
        return Err(StoreError::Stale(batch.projection.clone()));
    }
    for op in &batch.ops {
        let (table, _) = op_target(op);
        if !declared(table)? {
            return Err(StoreError::Undeclared {
                projection: batch.projection.clone(),
                table: table.to_string(),
            });
        }
    }
    Ok(())
}

/// Gives an applied answer's events the positions they were committed at:
/// a replay answers exactly what the first submission did.
pub fn committed_answer(answer: &mut Answer, committed: &[Event]) {
    if let AnswerResult::Applied(events) = &mut answer.result {
        for e in events.iter_mut() {
            if let Some(c) = committed.iter().find(|c| c.id == e.id) {
                e.position = c.position;
            }
        }
    }
}

/// Applies one row op to a row, the way every store must.
pub fn apply_op(row: Option<Value>, op: &RowOp) -> Option<Value> {
    match op {
        RowOp::Put { row, .. } => Some(row.clone()),
        RowOp::Delete { .. } => None,
        RowOp::Merge { fields, .. } => {
            let mut merged = match row {
                Some(Value::Object(m)) => m,
                _ => serde_json::Map::new(),
            };
            if let Value::Object(fields) = fields {
                for (k, v) in fields {
                    merged.insert(k.clone(), v.clone());
                }
            }
            Some(Value::Object(merged))
        }
    }
}

/// The table and key a row op changes.
pub fn op_target(op: &RowOp) -> (&str, &str) {
    match op {
        RowOp::Put { table, key, .. }
        | RowOp::Merge { table, key, .. }
        | RowOp::Delete { table, key } => (table, key),
    }
}

/// Takes a row for a claim, the way every store must: a new lease for the
/// claim's holder, an attempt counted, whether it was taken from a lease
/// that ran out, and any wake that came meanwhile spent. `woken` is the
/// store's record of that wake. Which rows a claim may take is each store's
/// own query.
pub fn claim_row(work: &mut Work, woken: &mut Option<Instant>, claim: &Claim) {
    work.taken_over = work.lease.is_some();
    work.lease = Some(Uuid::now_v7());
    work.holder = Some(claim.holder.clone());
    work.lease_until = Some(claim.now.plus(claim.lease_ms));
    work.claimed_at = Some(claim.now);
    work.attempts += 1;
    *woken = None;
}

/// Wakes a row at `due`, the way every store must: a row a claim holds
/// keeps the wake in `woken` for its release; any other is due by then.
pub fn wake_row(work: &mut Work, woken: &mut Option<Instant>, due: Instant) {
    if work.lease.is_some() {
        *woken = Some(woken.map_or(due, |w| w.min(due)));
    } else {
        work.due = work.due.min(due);
    }
}

/// Releases a row from its claim, the way every store must: due at `due`,
/// or at a wake that came while it was held if that is sooner.
pub fn release_row(work: &mut Work, woken: &mut Option<Instant>, due: Instant) {
    work.due = woken.take().map_or(due, |w| w.min(due));
    work.lease = None;
    work.holder = None;
    work.lease_until = None;
    work.claimed_at = None;
    work.taken_over = false;
}
