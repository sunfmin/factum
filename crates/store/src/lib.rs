//! The store contract: everything factum's engine needs from a backend, and
//! nothing about how the backend keeps it. The guarantees are stated here
//! and proved by `factum-conformance`, which runs the same scenarios through
//! the engine on every store.

use std::future::Future;

use factum_core::{
    Event, Instant, Position, Rejection, RowOp, StreamId, Uuid, Value, Version, Work, WorkChange,
    WorkKind,
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
    /// The backend failed; nothing of the unit was written.
    Backend(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Conflict(s) => write!(f, "conflict on {s}"),
            StoreError::KeyTaken(k) => write!(f, "key {k} already answered"),
            StoreError::LeaseLost => write!(f, "lease lost"),
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
    pub position: Position,
    /// Held at a dead letter, by a projection that stops on failure.
    pub stopped: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub projection: String,
    pub position: Position,
    pub event: Uuid,
    pub error: String,
}

/// A projection's catch-up: rows, dead letters and checkpoint, together.
#[derive(Clone, Debug)]
pub struct Batch {
    pub projection: String,
    pub version: u32,
    /// Refused as a conflict unless the checkpoint is here.
    pub from: Position,
    pub to: Position,
    pub ops: Vec<RowOp>,
    pub dead: Vec<DeadLetter>,
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
    ) -> impl Future<Output = Result<()>> + Send;

    fn apply_projection(&self, batch: Batch) -> impl Future<Output = Result<()>> + Send;

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
