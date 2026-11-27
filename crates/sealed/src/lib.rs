//! factum's store on one file of sealed records, appended (ADR 0021).
//!
//! Pure Rust, encrypted at rest with a key the caller derives. Each record
//! is one commit's whole change; each handle holds the fold of the file's
//! records in memory, and every method is one short transaction under an OS
//! file lock, so several handles, in several processes, take turns on one
//! file and see each other's commits. A file whose waste passes half of it
//! is rewritten as what it holds.

mod file;
mod state;

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use factum_core::{
    Event, Instant, Position, Query, StreamId, Table, Uuid, Value, Version, Work, WorkChange,
    WorkKind,
};
use factum_store::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Result, Snapshot, Store, Unit, apply_op,
    check_batch, check_unit, claim_row, op_target, read_answer, release_row, wake_row,
};

use crate::file::{Sealed, Tx};
use crate::state::{Built, Op, Row, State};

pub use crate::file::FORMAT;

pub struct SealedStore {
    sealed: Mutex<Sealed>,
}

impl SealedStore {
    /// Opens the file at `path` with `key`, creating it if needed. A key
    /// that does not open it, a file damaged before its last record, or a
    /// format newer than [`FORMAT`] (`StoreError::NewerSchema`) is refused
    /// here, before any call.
    pub fn open(path: impl AsRef<Path>, key: [u8; 32]) -> Result<SealedStore> {
        Ok(SealedStore {
            sealed: Mutex::new(Sealed::open(path.as_ref(), key)?),
        })
    }

    pub fn path(&self) -> std::path::PathBuf {
        self.handle().path().to_path_buf()
    }

    fn handle(&self) -> MutexGuard<'_, Sealed> {
        self.sealed.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn read<T>(&self, f: impl FnOnce(&State) -> T) -> Result<T> {
        self.handle().read(f)
    }

    fn write<T>(&self, f: impl FnOnce(&mut Tx) -> Result<T>) -> Result<T> {
        self.handle().write(f)
    }
}

/// Puts the work row `key` as `f` leaves it.
fn change_row(tx: &mut Tx, key: (WorkKind, Uuid), f: impl FnOnce(&mut Row)) -> Result<()> {
    let Some(mut row) = tx.state.work.get(&key).cloned() else {
        return Ok(());
    };
    f(&mut row);
    tx.emit(Op::Work {
        kind: key.0,
        id: key.1,
        row: Some(row),
    })
}

fn change(tx: &mut Tx, change: WorkChange) -> Result<()> {
    match change {
        WorkChange::Put(work) => {
            if tx.state.work.contains_key(&(work.kind, work.id)) {
                return Ok(());
            }
            let row = Row {
                seq: tx.state.work_seq + 1,
                woken: None,
                work,
            };
            tx.emit(Op::Work {
                kind: row.work.kind,
                id: row.work.id,
                row: Some(row),
            })
        }
        WorkChange::Wake { kind, id, due } => {
            change_row(tx, (kind, id), |r| wake_row(&mut r.work, &mut r.woken, due))
        }
        WorkChange::Done { kind, id } => {
            if !tx.state.work.contains_key(&(kind, id)) {
                return Ok(());
            }
            tx.emit(Op::Work {
                kind,
                id,
                row: None,
            })
        }
        WorkChange::Release { kind, id, due, .. } => change_row(tx, (kind, id), |r| {
            release_row(&mut r.work, &mut r.woken, due)
        }),
        WorkChange::Pace {
            name,
            key,
            not_before,
        } => tx.emit(Op::Pace {
            name,
            key,
            not_before,
        }),
    }
}

impl Store for SealedStore {
    async fn read_stream(&self, stream: &StreamId, after: Version) -> Result<Vec<Event>> {
        self.read(|s| s.stream(stream, after))
    }

    async fn answer(&self, key: Uuid) -> Result<Option<Answer>> {
        self.read(|s| read_answer(s.answered(key)))
    }

    async fn commit(&self, unit: Unit) -> Result<Vec<Event>> {
        self.write(|tx| {
            let s = &*tx.state;
            check_unit(
                &unit,
                |stream| Ok(s.version(stream)),
                |key| Ok(!s.answered(key).is_empty()),
                |kind, id| Ok(s.work.get(&(kind, id)).and_then(|r| r.work.lease)),
            )?;
            let mut committed = Vec::new();
            for append in unit.appends {
                for mut event in append.events {
                    event.position = Position(0, tx.state.log.len() as u64 + 1);
                    committed.push(event.clone());
                    tx.emit(Op::Event(event))?;
                }
            }
            for c in unit.work {
                change(tx, c)?;
            }
            Ok(committed)
        })
    }

    async fn read_log(&self, after: Position, limit: usize) -> Result<Vec<Event>> {
        self.read(|s| {
            let start = (after.1 as usize).min(s.log.len());
            s.log[start..].iter().take(limit).cloned().collect()
        })
    }

    async fn claim(&self, claim: Claim) -> Result<Vec<Work>> {
        self.write(|tx| {
            let s = &*tx.state;
            let mut due: Vec<(Instant, u64, (WorkKind, Uuid))> = (s.work.iter())
                .filter(|(_, r)| s.claimable(r, &claim))
                .map(|(k, r)| (r.work.due, r.seq, *k))
                .collect();
            due.sort();
            let mut claimed = Vec::new();
            for (_, _, key) in due.into_iter().take(claim.limit) {
                change_row(tx, key, |r| {
                    claim_row(&mut r.work, &mut r.woken, &claim);
                    claimed.push(r.work.clone());
                })?;
            }
            Ok(claimed)
        })
    }

    async fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> Result<bool> {
        self.write(|tx| {
            let held = tx.state.work.get(&(kind, id)).is_some_and(|r| {
                r.work.lease == Some(lease) && r.work.lease_until.is_some_and(|u| u > now)
            });
            if held {
                change_row(tx, (kind, id), |r| r.work.lease_until = Some(until))?;
            }
            Ok(held)
        })
    }

    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        self.read(|s| s.work.get(&(kind, id)).map(|r| r.work.clone()))
    }

    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        self.read(|s| {
            let mut rows: Vec<&Row> = (s.work.values())
                .filter(|r| r.work.kind == kind && r.work.name == name)
                .collect();
            rows.sort_by_key(|r| r.seq);
            rows.into_iter().map(|r| r.work.clone()).collect()
        })
    }

    async fn snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>> {
        self.read(|s| s.snapshots.get(stream).cloned())
    }

    async fn keep_snapshot(&self, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        self.write(|tx| {
            let keep = (tx.state.snapshots.get(stream))
                .is_none_or(|s| s.version < snapshot.version || s.code != snapshot.code);
            if keep {
                let stream = stream.clone();
                tx.emit(Op::Snapshot { stream, snapshot })?;
            }
            Ok(())
        })
    }

    async fn checkpoint(&self, projection: &str) -> Result<Option<Checkpoint>> {
        self.read(|s| s.projections.get(projection).map(Built::checkpoint))
    }

    async fn reset_projection(
        &self,
        projection: &str,
        version: u32,
        schema: &str,
        tables: &[Table],
    ) -> Result<()> {
        self.write(|tx| {
            let projection = projection.to_string();
            tx.emit(Op::Reset {
                projection: projection.clone(),
            })?;
            let built = Built {
                version,
                schema: schema.to_string(),
                position: Position::default(),
                stopped: false,
                tables: tables.iter().map(|t| t.name.clone()).collect(),
            };
            tx.emit(Op::Projection { projection, built })
        })
    }

    async fn apply_projection(&self, batch: Batch) -> Result<()> {
        self.write(|tx| {
            let p = &batch.projection;
            let s = &*tx.state;
            let built = s.projections.get(p);
            let declared = built.map_or(&[][..], |b| b.tables.as_slice());
            let dead = s.dead.get(p).map_or(&[][..], Vec::as_slice);
            check_batch(
                &batch,
                built.map(|b| (b.version, b.position)),
                |e| Ok(dead.iter().any(|d| d.event == e)),
                |table| Ok(declared.iter().any(|d| d == table)),
            )?;
            for op in &batch.ops {
                let (table, key) = op_target(op);
                let k = (p.clone(), table.to_string(), key.to_string());
                let row = apply_op(tx.state.rows.get(&k).cloned(), op);
                let (projection, table, key) = k;
                tx.emit(Op::Row {
                    projection,
                    table,
                    key,
                    row,
                })?;
            }
            if !batch.resolved.is_empty() || !batch.dead.is_empty() {
                let mut dead = tx.state.dead.get(p).cloned().unwrap_or_default();
                dead.retain(|d| !batch.resolved.contains(&d.event));
                dead.extend(batch.dead.iter().cloned());
                tx.emit(Op::Dead {
                    projection: p.clone(),
                    dead,
                })?;
            }
            if let Some(mut built) = tx.state.projections.get(p).cloned() {
                built.position = batch.to;
                built.stopped = batch.stop;
                tx.emit(Op::Projection {
                    projection: p.clone(),
                    built,
                })?;
            }
            Ok(())
        })
    }

    async fn query(&self, projection: &str, query: &Query) -> Result<Vec<(String, Value)>> {
        let rows = self.rows(projection, &query.table).await?;
        Ok(factum_core::answer(rows, query))
    }

    async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>> {
        self.read(|s| {
            (s.rows.iter())
                .filter(|((p, t, _), _)| p == projection && t == table)
                .map(|((_, _, k), v)| (k.clone(), v.clone()))
                .collect()
        })
    }

    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        let k = (projection.to_string(), table.to_string(), key.to_string());
        self.read(|s| s.rows.get(&k).cloned())
    }

    async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>> {
        self.read(|s| s.dead.get(projection).cloned().unwrap_or_default())
    }
}
