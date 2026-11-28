//! What a file holds, folded: the ops its records carry, applied in order.
//! Every handle's view is this fold and nothing else, so a handle that
//! reloads, and a file compacted from it, hold the same.

use std::collections::{BTreeMap, HashMap};

use factum_core::{Event, Instant, StreamId, Uuid, Value, Version, Work, WorkKind};
use factum_store::{Checkpoint, Claim, DeadLetter, Snapshot};
use serde::{Deserialize, Serialize};

/// A work row as kept: its put order (`seq`), and a wake that came while it
/// was claimed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub work: Work,
    pub seq: u64,
    pub woken: Option<Instant>,
}

/// A projection's checkpoint and the tables it was last built with.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Built {
    pub checkpoint: Checkpoint,
    pub tables: Vec<String>,
}

/// One change a record carries. Each but `Event` and `Reset` sets one slot
/// whole, so a later op on the slot supersedes the earlier one.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Op {
    /// Appended to the log, at its position. Kept forever.
    Event(Event),
    /// The row as it is now; `None` when it is done.
    Work {
        kind: WorkKind,
        id: Uuid,
        row: Option<Row>,
    },
    Pace {
        name: String,
        key: String,
        not_before: Instant,
    },
    Snapshot {
        stream: StreamId,
        snapshot: Snapshot,
    },
    Projection {
        projection: String,
        built: Built,
    },
    /// Drops the projection's rows and dead letters.
    Reset {
        projection: String,
    },
    Row {
        projection: String,
        table: String,
        key: String,
        row: Option<Value>,
    },
    /// The projection's dead letters, all of them, in the order they came.
    Dead {
        projection: String,
        dead: Vec<DeadLetter>,
    },
}

/// What an op sets: a later op on the same slot makes the earlier waste.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Slot {
    Work(WorkKind, Uuid),
    Pace(String, String),
    Snapshot(StreamId),
    Projection(String),
    Row(String, String, String),
    Dead(String),
}

#[derive(Default)]
pub struct State {
    pub log: Vec<Event>,
    /// Each stream's events, as indexes into the log.
    streams: HashMap<StreamId, Vec<usize>>,
    /// Each key's events, as indexes into the log.
    keyed: HashMap<Uuid, Vec<usize>>,
    pub work: BTreeMap<(WorkKind, Uuid), Row>,
    pub work_seq: u64,
    paces: HashMap<(String, String), Instant>,
    pub snapshots: HashMap<StreamId, Snapshot>,
    pub projections: HashMap<String, Built>,
    pub rows: factum_store::Rows,
    pub dead: HashMap<String, Vec<DeadLetter>>,
    /// The bytes of the op each live slot was last set by.
    live: HashMap<Slot, usize>,
    /// The bytes of every event op and of every live slot's op: about what
    /// a compacted file holds.
    pub held: u64,
}

impl State {
    /// Applies one op, `size` bytes as written.
    pub fn apply(&mut self, op: Op, size: usize) {
        let slot = match op {
            Op::Event(event) => {
                let at = self.log.len();
                self.streams
                    .entry(event.stream.clone())
                    .or_default()
                    .push(at);
                if let Some(key) = event.meta.key {
                    self.keyed.entry(key).or_default().push(at);
                }
                self.log.push(event);
                self.held += size as u64;
                return;
            }
            Op::Reset { projection } => {
                // A row's slot is live exactly while the row is held, so
                // the rows dropped name every live row slot of the projection.
                for (p, t, k) in factum_store::drop_rows(&mut self.rows, &projection) {
                    self.unset(&Slot::Row(p, t, k));
                }
                self.dead.remove(&projection);
                self.unset(&Slot::Dead(projection));
                return;
            }
            Op::Work { kind, id, row } => {
                match row {
                    Some(row) => {
                        self.work_seq = self.work_seq.max(row.seq);
                        self.work.insert((kind, id), row);
                    }
                    None => {
                        self.work.remove(&(kind, id));
                    }
                }
                (Slot::Work(kind, id), self.work.contains_key(&(kind, id)))
            }
            Op::Pace {
                name,
                key,
                not_before,
            } => {
                let slot = Slot::Pace(name.clone(), key.clone());
                self.paces.insert((name, key), not_before);
                (slot, true)
            }
            Op::Snapshot { stream, snapshot } => {
                self.snapshots.insert(stream.clone(), snapshot);
                (Slot::Snapshot(stream), true)
            }
            Op::Projection { projection, built } => {
                self.projections.insert(projection.clone(), built);
                (Slot::Projection(projection), true)
            }
            Op::Row {
                projection,
                table,
                key,
                row,
            } => {
                let k = (projection, table, key);
                let held = row.is_some();
                match row {
                    Some(row) => self.rows.insert(k.clone(), row),
                    None => self.rows.remove(&k),
                };
                (Slot::Row(k.0, k.1, k.2), held)
            }
            Op::Dead { projection, dead } => {
                let held = !dead.is_empty();
                if held {
                    self.dead.insert(projection.clone(), dead);
                } else {
                    self.dead.remove(&projection);
                }
                (Slot::Dead(projection), held)
            }
        };
        let (slot, held) = slot;
        self.unset(&slot);
        if held {
            self.held += size as u64;
            self.live.insert(slot, size);
        }
    }

    fn unset(&mut self, slot: &Slot) {
        if let Some(size) = self.live.remove(slot) {
            self.held -= size as u64;
        }
    }

    /// Every op a compacted file needs to fold into this state: the log,
    /// then each slot's current value.
    pub fn ops(&self) -> impl Iterator<Item = Op> + '_ {
        let events = self.log.iter().cloned().map(Op::Event);
        let work = self.work.iter().map(|(&(kind, id), row)| Op::Work {
            kind,
            id,
            row: Some(row.clone()),
        });
        let paces = self
            .paces
            .iter()
            .map(|((name, key), &not_before)| Op::Pace {
                name: name.clone(),
                key: key.clone(),
                not_before,
            });
        let snapshots = self.snapshots.iter().map(|(stream, s)| Op::Snapshot {
            stream: stream.clone(),
            snapshot: s.clone(),
        });
        let projections = self.projections.iter().map(|(p, built)| Op::Projection {
            projection: p.clone(),
            built: built.clone(),
        });
        let rows = self.rows.iter().map(|((p, t, k), v)| Op::Row {
            projection: p.clone(),
            table: t.clone(),
            key: k.clone(),
            row: Some(v.clone()),
        });
        let dead = self.dead.iter().map(|(p, d)| Op::Dead {
            projection: p.clone(),
            dead: d.clone(),
        });
        events
            .chain(work)
            .chain(paces)
            .chain(snapshots)
            .chain(projections)
            .chain(rows)
            .chain(dead)
    }

    pub fn version(&self, stream: &StreamId) -> Version {
        Version(self.streams.get(stream).map_or(0, |e| e.len() as u64))
    }

    pub fn stream(&self, stream: &StreamId, after: Version) -> Vec<Event> {
        let at = self.streams.get(stream).map_or(&[][..], Vec::as_slice);
        let at = at.get(after.0 as usize..).unwrap_or(&[]);
        at.iter().map(|&i| self.log[i].clone()).collect()
    }

    /// The events committed under the key with a fingerprint.
    pub fn answered(&self, key: Uuid) -> Vec<Event> {
        let at = self.keyed.get(&key).map_or(&[][..], Vec::as_slice);
        let events = at.iter().map(|&i| self.log[i].clone());
        events.filter(|e| e.meta.fingerprint.is_some()).collect()
    }

    /// Whether a claim may take the row: the contract's query (ADR 0010).
    pub fn claimable(&self, row: &Row, claim: &Claim) -> bool {
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
        let Some(key) = &w.ordering_key else {
            return true;
        };
        let paced = self.paces.get(&(w.name.clone(), key.clone()));
        if paced.is_some_and(|p| *p > claim.now) {
            return false;
        }
        let first = (self.work.values())
            .filter(|r| {
                r.work.kind == w.kind
                    && r.work.name == w.name
                    && r.work.ordering_key.as_ref() == Some(key)
            })
            .map(|r| r.seq)
            .min();
        first == Some(row.seq)
    }
}
