//! factum's store on SQLite. Every write is one `BEGIN IMMEDIATE`
//! transaction, so writers take turns and an event's sequence number is its
//! commit order: every committed event is final, and the log is read by it.
//! Several handles (and processes) may open one file; they wait for each
//! other's writes up to the busy timeout.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use factum_core::{
    Event, Instant, Meta, Position, StreamId, Uuid, Value, Version, Work, WorkChange, WorkKind,
};
use factum_store::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Result, Snapshot, Store, StoreError, Unit,
    apply_op, committed_answer, op_target,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

const SCHEMA: &str = "
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    type_version INTEGER NOT NULL,
    data TEXT NOT NULL,
    meta TEXT NOT NULL,
    UNIQUE (stream, version)
);
CREATE TABLE IF NOT EXISTS answers (key TEXT PRIMARY KEY, answer TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS work (
    kind TEXT NOT NULL,
    id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    name TEXT NOT NULL,
    process TEXT,
    step TEXT,
    payload TEXT NOT NULL,
    due INTEGER NOT NULL,
    ordering_key TEXT,
    attempts INTEGER NOT NULL,
    lease TEXT,
    holder TEXT,
    lease_until INTEGER,
    claimed_at INTEGER,
    woken INTEGER,
    PRIMARY KEY (kind, id)
);
CREATE INDEX IF NOT EXISTS work_due ON work (kind, due);
CREATE INDEX IF NOT EXISTS work_name ON work (kind, name, ordering_key, seq);
CREATE TABLE IF NOT EXISTS paces (name TEXT, key TEXT, not_before INTEGER NOT NULL, PRIMARY KEY (name, key));
CREATE TABLE IF NOT EXISTS snapshots (stream TEXT PRIMARY KEY, version INTEGER NOT NULL, code TEXT NOT NULL, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoints (projection TEXT PRIMARY KEY, version INTEGER NOT NULL, position INTEGER NOT NULL, stopped INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS rows (projection TEXT, tbl TEXT, key TEXT, row TEXT NOT NULL, PRIMARY KEY (projection, tbl, key));
CREATE TABLE IF NOT EXISTS dead_letters (projection TEXT NOT NULL, position INTEGER NOT NULL, event TEXT NOT NULL, error TEXT NOT NULL);
";

pub struct SqliteStore {
    conn: Mutex<Connection>,
    path: PathBuf,
}

fn backend(e: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(e.to_string())
}

fn json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(backend)
}

fn parse<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(backend)
}

fn uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(backend)
}

impl SqliteStore {
    /// Opens the database at `path`, creating it and its tables if needed.
    pub fn open(path: impl AsRef<Path>) -> Result<SqliteStore> {
        let conn = Connection::open(path.as_ref()).map_err(backend)?;
        conn.busy_timeout(Duration::from_secs(10))
            .map_err(backend)?;
        conn.execute_batch(SCHEMA).map_err(backend)?;
        Ok(SqliteStore {
            conn: Mutex::new(conn),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Runs `f` in one immediate transaction: a writer, alone.
    fn write<T>(&self, f: impl FnOnce(&Transaction) -> Result<T>) -> Result<T> {
        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let out = f(&tx)?;
        tx.commit().map_err(backend)?;
        Ok(out)
    }
}

const EVENT_COLUMNS: &str = "seq, id, stream, version, kind, type_version, data, meta";

/// An events row as read: seq, id, stream, version, kind, type_version, data, meta.
type EventRow = (i64, String, String, i64, String, i64, String, String);

fn event(row: &Row) -> rusqlite::Result<EventRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
    ))
}

fn to_event((seq, id, stream, version, kind, tv, data, meta): EventRow) -> Result<Event> {
    Ok(Event {
        id: uuid(&id)?,
        stream: StreamId::try_from(stream).map_err(backend)?,
        version: Version(version as u64),
        kind,
        type_version: tv as u32,
        data: parse(&data)?,
        meta: parse::<Meta>(&meta)?,
        position: Position(0, seq as u64),
    })
}

fn events(conn: &Connection, sql: &str, args: impl rusqlite::Params) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(sql).map_err(backend)?;
    let rows = stmt.query_map(args, event).map_err(backend)?;
    rows.map(|r| r.map_err(backend).and_then(to_event))
        .collect()
}

const WORK_COLUMNS: &str = "kind, id, name, process, step, payload, due, ordering_key, attempts, lease, holder, lease_until, claimed_at, seq";

fn work(row: &Row) -> rusqlite::Result<(Work, i64)> {
    let text = |i: usize| -> rusqlite::Result<Option<Uuid>> {
        Ok(row
            .get::<_, Option<String>>(i)?
            .and_then(|s| Uuid::parse_str(&s).ok()))
    };
    let kind: String = row.get(0)?;
    let id: String = row.get(1)?;
    let payload: String = row.get(5)?;
    let w = Work {
        kind: WorkKind::parse(&kind).unwrap_or(WorkKind::Advance),
        id: Uuid::parse_str(&id).unwrap_or_default(),
        name: row.get(2)?,
        process: text(3)?,
        step: row.get(4)?,
        payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
        due: Instant(row.get(6)?),
        ordering_key: row.get(7)?,
        attempts: row.get::<_, i64>(8)? as u32,
        lease: text(9)?,
        holder: row.get(10)?,
        lease_until: row.get::<_, Option<i64>>(11)?.map(Instant),
        claimed_at: row.get::<_, Option<i64>>(12)?.map(Instant),
        taken_over: false,
    };
    Ok((w, row.get(13)?))
}

fn works(conn: &Connection, sql: &str, args: impl rusqlite::Params) -> Result<Vec<(Work, i64)>> {
    let mut stmt = conn.prepare(sql).map_err(backend)?;
    let rows = stmt.query_map(args, work).map_err(backend)?;
    rows.map(|r| r.map_err(backend)).collect()
}

fn lease_of(tx: &Transaction, kind: WorkKind, id: Uuid) -> Result<Option<String>> {
    tx.query_row(
        "SELECT lease FROM work WHERE kind = ?1 AND id = ?2",
        params![kind.as_str(), id.to_string()],
        |r| r.get(0),
    )
    .optional()
    .map_err(backend)
    .map(Option::flatten)
}

fn change(tx: &Transaction, change: &WorkChange) -> Result<()> {
    match change {
        WorkChange::Put(w) => tx.execute(
            "INSERT OR IGNORE INTO work (kind, id, seq, name, process, step, payload, due, ordering_key, attempts)
             VALUES (?1, ?2, (SELECT COALESCE(MAX(seq), 0) + 1 FROM work), ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                w.kind.as_str(),
                w.id.to_string(),
                w.name,
                w.process.map(|p| p.to_string()),
                w.step,
                json(&w.payload)?,
                w.due.0,
                w.ordering_key,
                w.attempts,
            ],
        ),
        WorkChange::Wake { kind, id, due } => tx.execute(
            "UPDATE work SET
                woken = CASE WHEN lease IS NOT NULL THEN MIN(COALESCE(woken, ?3), ?3) ELSE woken END,
                due = CASE WHEN lease IS NULL THEN MIN(due, ?3) ELSE due END
             WHERE kind = ?1 AND id = ?2",
            params![kind.as_str(), id.to_string(), due.0],
        ),
        WorkChange::Done { kind, id } => {
            tx.execute("DELETE FROM work WHERE kind = ?1 AND id = ?2", params![kind.as_str(), id.to_string()])
        }
        WorkChange::Release { kind, id, due, .. } => tx.execute(
            "UPDATE work SET due = CASE WHEN woken IS NULL THEN ?3 ELSE MIN(woken, ?3) END,
                woken = NULL, lease = NULL, holder = NULL, lease_until = NULL, claimed_at = NULL
             WHERE kind = ?1 AND id = ?2",
            params![kind.as_str(), id.to_string(), due.0],
        ),
        WorkChange::Pace { name, key, not_before } => tx.execute(
            "INSERT OR REPLACE INTO paces (name, key, not_before) VALUES (?1, ?2, ?3)",
            params![name, key, not_before.0],
        ),
    }
    .map(|_| ())
    .map_err(backend)
}

impl Store for SqliteStore {
    async fn read_stream(&self, stream: &StreamId, after: Version) -> Result<Vec<Event>> {
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE stream = ?1 AND version > ?2 ORDER BY version"
        );
        events(
            &self.conn(),
            &sql,
            params![stream.to_string(), after.0 as i64],
        )
    }

    async fn answer(&self, key: Uuid) -> Result<Option<Answer>> {
        let found: Option<String> = self
            .conn()
            .query_row(
                "SELECT answer FROM answers WHERE key = ?1",
                [key.to_string()],
                |r| r.get(0),
            )
            .optional()
            .map_err(backend)?;
        found.map(|a| parse(&a)).transpose()
    }

    async fn commit(&self, unit: Unit) -> Result<Vec<Event>> {
        self.write(|tx| {
            for a in &unit.appends {
                let at: i64 = tx
                    .query_row("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream = ?1", [a.stream.to_string()], |r| r.get(0))
                    .map_err(backend)?;
                if at as u64 != a.expected.0 {
                    return Err(StoreError::Conflict(a.stream.clone()));
                }
            }
            if let Some((key, _)) = &unit.answer {
                let taken: Option<i64> = tx
                    .query_row("SELECT 1 FROM answers WHERE key = ?1", [key.to_string()], |r| r.get(0))
                    .optional()
                    .map_err(backend)?;
                if taken.is_some() {
                    return Err(StoreError::KeyTaken(*key));
                }
            }
            let releases = unit.work.iter().filter_map(|c| match c {
                WorkChange::Release { kind, id, lease, .. } => Some((*kind, *id, *lease)),
                _ => None,
            });
            for (kind, id, lease) in unit.guards.iter().copied().chain(releases) {
                if lease_of(tx, kind, id)? != Some(lease.to_string()) {
                    return Err(StoreError::LeaseLost);
                }
            }
            let mut committed = Vec::new();
            for a in &unit.appends {
                for e in &a.events {
                    tx.execute(
                        "INSERT INTO events (id, stream, version, kind, type_version, data, meta) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![e.id.to_string(), e.stream.to_string(), e.version.0 as i64, e.kind, e.type_version, json(&e.data)?, json(&e.meta)?],
                    )
                    .map_err(backend)?;
                    let mut e = e.clone();
                    e.position = Position(0, tx.last_insert_rowid() as u64);
                    committed.push(e);
                }
            }
            if let Some((key, mut answer)) = unit.answer {
                committed_answer(&mut answer, &committed);
                tx.execute("INSERT INTO answers (key, answer) VALUES (?1, ?2)", params![key.to_string(), json(&answer)?])
                    .map_err(backend)?;
            }
            for c in &unit.work {
                change(tx, c)?;
            }
            Ok(committed)
        })
    }

    async fn read_log(&self, after: Position, limit: usize) -> Result<Vec<Event>> {
        let sql =
            format!("SELECT {EVENT_COLUMNS} FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2");
        events(&self.conn(), &sql, params![after.1 as i64, limit as i64])
    }

    async fn claim(&self, claim: Claim) -> Result<Vec<Work>> {
        self.write(|tx| {
            let sql = format!(
                "SELECT {WORK_COLUMNS} FROM work
                 WHERE kind = ?1 AND due <= ?2 AND (lease_until IS NULL OR lease_until <= ?2)
                 ORDER BY due, seq"
            );
            let mut claimed = Vec::new();
            for (mut w, seq) in works(tx, &sql, params![claim.kind.as_str(), claim.now.0])? {
                if claimed.len() >= claim.limit {
                    break;
                }
                if claim.names.as_ref().is_some_and(|n| !n.contains(&w.name)) {
                    continue;
                }
                if let Some(key) = &w.ordering_key {
                    let first: i64 = tx
                        .query_row(
                            "SELECT MIN(seq) FROM work WHERE kind = ?1 AND name = ?2 AND ordering_key = ?3",
                            params![w.kind.as_str(), w.name, key],
                            |r| r.get(0),
                        )
                        .map_err(backend)?;
                    let pace: Option<i64> = tx
                        .query_row("SELECT not_before FROM paces WHERE name = ?1 AND key = ?2", params![w.name, key], |r| r.get(0))
                        .optional()
                        .map_err(backend)?;
                    if first != seq || pace.is_some_and(|p| p > claim.now.0) {
                        continue;
                    }
                }
                w.taken_over = w.lease.is_some();
                w.lease = Some(Uuid::now_v7());
                w.holder = Some(claim.holder.clone());
                w.lease_until = Some(claim.now.plus(claim.lease_ms));
                w.claimed_at = Some(claim.now);
                w.attempts += 1;
                tx.execute(
                    "UPDATE work SET lease = ?3, holder = ?4, lease_until = ?5, claimed_at = ?6, attempts = ?7, woken = NULL
                     WHERE kind = ?1 AND id = ?2",
                    params![
                        w.kind.as_str(),
                        w.id.to_string(),
                        w.lease.map(|l| l.to_string()),
                        w.holder,
                        w.lease_until.map(|i| i.0),
                        w.claimed_at.map(|i| i.0),
                        w.attempts,
                    ],
                )
                .map_err(backend)?;
                claimed.push(w);
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
            let n = tx
                .execute(
                    "UPDATE work SET lease_until = ?5 WHERE kind = ?1 AND id = ?2 AND lease = ?3 AND lease_until > ?4",
                    params![kind.as_str(), id.to_string(), lease.to_string(), now.0, until.0],
                )
                .map_err(backend)?;
            Ok(n == 1)
        })
    }

    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        let sql = format!("SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND id = ?2");
        Ok(
            works(&self.conn(), &sql, params![kind.as_str(), id.to_string()])?
                .into_iter()
                .next()
                .map(|(w, _)| w),
        )
    }

    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        let sql =
            format!("SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND name = ?2 ORDER BY seq");
        Ok(works(&self.conn(), &sql, params![kind.as_str(), name])?
            .into_iter()
            .map(|(w, _)| w)
            .collect())
    }

    async fn snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>> {
        let found: Option<(i64, String, String)> = self
            .conn()
            .query_row(
                "SELECT version, code, state FROM snapshots WHERE stream = ?1",
                [stream.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(backend)?;
        found
            .map(|(v, code, state)| {
                Ok(Snapshot {
                    version: Version(v as u64),
                    code,
                    state: parse(&state)?,
                })
            })
            .transpose()
    }

    async fn keep_snapshot(&self, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "INSERT INTO snapshots (stream, version, code, state) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (stream) DO UPDATE SET version = excluded.version, code = excluded.code, state = excluded.state
                 WHERE snapshots.version < excluded.version OR snapshots.code <> excluded.code",
                params![stream.to_string(), snapshot.version.0 as i64, snapshot.code, json(&snapshot.state)?],
            )
            .map(|_| ())
            .map_err(backend)
        })
    }

    async fn checkpoint(&self, projection: &str) -> Result<Option<Checkpoint>> {
        self.conn()
            .query_row(
                "SELECT version, position, stopped FROM checkpoints WHERE projection = ?1",
                [projection],
                |r| {
                    Ok(Checkpoint {
                        version: r.get::<_, i64>(0)? as u32,
                        position: Position(0, r.get::<_, i64>(1)? as u64),
                        stopped: r.get::<_, i64>(2)? != 0,
                    })
                },
            )
            .optional()
            .map_err(backend)
    }

    async fn reset_projection(&self, projection: &str, version: u32) -> Result<()> {
        self.write(|tx| {
            tx.execute("DELETE FROM rows WHERE projection = ?1", [projection]).map_err(backend)?;
            tx.execute("DELETE FROM dead_letters WHERE projection = ?1", [projection]).map_err(backend)?;
            tx.execute(
                "INSERT OR REPLACE INTO checkpoints (projection, version, position, stopped) VALUES (?1, ?2, 0, 0)",
                params![projection, version],
            )
            .map(|_| ())
            .map_err(backend)
        })
    }

    async fn apply_projection(&self, batch: Batch) -> Result<()> {
        self.write(|tx| {
            let at: Option<(i64, i64)> = tx
                .query_row("SELECT version, position FROM checkpoints WHERE projection = ?1", [&batch.projection], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()
                .map_err(backend)?;
            if at != Some((i64::from(batch.version), batch.from.1 as i64)) {
                return Err(StoreError::Conflict(StreamId::new(format!("projection:{}", batch.projection), Uuid::nil())));
            }
            for op in &batch.ops {
                let (table, key) = op_target(op);
                let old: Option<String> = tx
                    .query_row(
                        "SELECT row FROM rows WHERE projection = ?1 AND tbl = ?2 AND key = ?3",
                        params![batch.projection, table, key],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(backend)?;
                let old = old.map(|s| parse::<Value>(&s)).transpose()?;
                match apply_op(old, op) {
                    Some(row) => tx.execute(
                        "INSERT OR REPLACE INTO rows (projection, tbl, key, row) VALUES (?1, ?2, ?3, ?4)",
                        params![batch.projection, table, key, json(&row)?],
                    ),
                    None => tx.execute(
                        "DELETE FROM rows WHERE projection = ?1 AND tbl = ?2 AND key = ?3",
                        params![batch.projection, table, key],
                    ),
                }
                .map_err(backend)?;
            }
            for d in &batch.dead {
                tx.execute(
                    "INSERT INTO dead_letters (projection, position, event, error) VALUES (?1, ?2, ?3, ?4)",
                    params![d.projection, d.position.1 as i64, d.event.to_string(), d.error],
                )
                .map_err(backend)?;
            }
            tx.execute(
                "UPDATE checkpoints SET position = ?2, stopped = ?3 WHERE projection = ?1",
                params![batch.projection, batch.to.1 as i64, batch.stop],
            )
            .map(|_| ())
            .map_err(backend)
        })
    }

    async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare("SELECT key, row FROM rows WHERE projection = ?1 AND tbl = ?2 ORDER BY key")
            .map_err(backend)?;
        let rows = stmt
            .query_map(params![projection, table], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(backend)?;
        rows.map(|r| {
            let (k, v) = r.map_err(backend)?;
            Ok((k, parse(&v)?))
        })
        .collect()
    }

    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        let found: Option<String> = self
            .conn()
            .query_row(
                "SELECT row FROM rows WHERE projection = ?1 AND tbl = ?2 AND key = ?3",
                params![projection, table, key],
                |r| r.get(0),
            )
            .optional()
            .map_err(backend)?;
        found.map(|s| parse(&s)).transpose()
    }

    async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare("SELECT position, event, error FROM dead_letters WHERE projection = ?1 ORDER BY position")
            .map_err(backend)?;
        let rows = stmt
            .query_map([projection], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(backend)?;
        rows.map(|r| {
            let (position, event, error) = r.map_err(backend)?;
            Ok(DeadLetter {
                projection: projection.to_string(),
                position: Position(0, position as u64),
                event: uuid(&event)?,
                error,
            })
        })
        .collect()
    }
}
