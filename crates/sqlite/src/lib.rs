//! factum's store on SQLite.
//!
//! Every write is one `BEGIN IMMEDIATE` transaction, so writers take turns
//! and an event's sequence number is its commit order: every committed
//! event is final, and the log is read by it. Several handles (and
//! processes) may open one file; they wait for each other's writes up to the
//! busy timeout.
//!
//! Each handle owns one connection on a thread of its own, so the async
//! methods never block the caller's runtime; they need no particular one.
//!
//! A projection's table is a table of its own, `"rows:<projection>:<table>"`:
//! the key, the row as JSON, and each declared column generated from the row
//! (`json_extract`), indexed as declared, so a query filters and orders in SQL.

mod thread;

use std::path::{Path, PathBuf};
use std::time::Duration;

use factum_core::{
    Event, Instant, Meta, Position, Query, RowOp, StreamId, Table, Uuid, Value, Version, Work,
    WorkChange, WorkKind,
};
use factum_store::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Kept, Result, Snapshot, Store, StoreError, Unit,
    apply_op, check_batch, check_unit, claim_row, op_target, read_answer, release_row, wake_row,
};
use rusqlite::types::Value as Sql;
use rusqlite::{
    Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params, params_from_iter,
};

use crate::thread::Worker;

/// The first schema, as factum wrote it before schemas had versions. A
/// database of that time holds it at version 0, so each table is made only
/// if it is not there.
const FIRST: &str = "
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
CREATE TABLE IF NOT EXISTS checkpoints (
    projection TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    schema TEXT NOT NULL,
    position INTEGER NOT NULL,
    stopped INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_tables (projection TEXT, tbl TEXT, PRIMARY KEY (projection, tbl));
CREATE TABLE IF NOT EXISTS dead_letters (
    projection TEXT NOT NULL,
    position INTEGER NOT NULL,
    event TEXT NOT NULL,
    stream TEXT NOT NULL,
    version INTEGER NOT NULL,
    error TEXT NOT NULL
);
";

/// The schema, one step per change, in order: a database's `user_version`
/// is how many it has had. A released step is never edited; a change to the
/// schema is a step added at the end (the fixture of an older schema in
/// `tests/conformance.rs` fails otherwise).
const STEPS: &[fn(&Transaction) -> rusqlite::Result<()>] = &[
    |tx| tx.execute_batch(FIRST),
    // factum#14. Some hosts had it added by hand before this step existed.
    |tx| add_column(tx, "work", "taken_over", "INTEGER NOT NULL DEFAULT 0"),
    // factum#39: a refusal keeps nothing for its key any more.
    |tx| {
        tx.execute_batch(
            "DELETE FROM answers WHERE json_extract(answer, '$.result.Rejected') IS NOT NULL",
        )
    },
    // factum#40: an applied answer is read from its events by key, not kept
    // as a copy; only its fingerprint and stream, or a refusal, are kept.
    |tx| {
        tx.execute_batch(AS_KEPT)?;
        tx.execute_batch("CREATE INDEX events_key ON events (json_extract(meta, '$.key'));")
    },
    // factum#41: an answer that wrote no events keeps nothing for its key
    // any more. A runner's such row goes too: at worst a recurring command
    // runs once more in the period this opens in.
    |tx| tx.execute_batch("DELETE FROM answers WHERE json_type(answer, '$.result.Ok') = 'null'"),
    // factum#42: an answer that wrote events is read from them alone.
    // Rows an older binary wrote in the shape before factum#40 after this
    // database had left it (seen on factorium's host) are first brought to
    // it, as that step did.
    |tx| {
        let old = "json_type(answer, '$.result.Applied') IS NOT NULL OR json_type(answer, '$.result.Rejected') IS NOT NULL";
        tx.execute_batch(&format!("{AS_KEPT} WHERE {old}"))?;
        tx.execute_batch(FINGERPRINT_IN_META)
    },
];

/// Each key's fingerprint, kept beside the events it wrote (`{"Ok":
/// stream}`), is written once into their meta, as the code since writes it;
/// then the row goes. Nothing else of an event changes: the fingerprint is
/// the one fact of it that was kept elsewhere.
const FINGERPRINT_IN_META: &str = "
UPDATE events SET meta = json_set(meta, '$.fingerprint', k.fingerprint)
FROM (SELECT key, json_extract(answer, '$.fingerprint') AS fingerprint,
             json_extract(answer, '$.result.Ok') AS stream
      FROM answers WHERE json_type(answer, '$.result.Ok') = 'text') AS k
WHERE json_extract(events.meta, '$.key') = k.key AND events.stream = k.stream;
DELETE FROM answers WHERE json_type(answer, '$.result.Ok') = 'text';
";

/// Whether a key has an answer: events committed under it with a
/// fingerprint, or a row kept for it.
const ANSWERED: &str = "SELECT EXISTS (SELECT 1 FROM answers WHERE key = ?1) OR EXISTS (
    SELECT 1 FROM events INDEXED BY events_key
    WHERE json_extract(meta, '$.key') = ?1 AND json_extract(meta, '$.fingerprint') IS NOT NULL)";

/// Each answer kept as a copy (before factum#40) becomes what factum#40
/// kept for it, as JSON: its fingerprint and its events' stream (null if
/// none), or its refusal.
const AS_KEPT: &str = "
UPDATE answers SET answer = json_object('fingerprint', json_extract(answer, '$.fingerprint'), 'result',
    CASE WHEN json_extract(answer, '$.result.Rejected') IS NOT NULL
         THEN json_object('Err', json_extract(answer, '$.result.Rejected'))
         ELSE json_object('Ok', json_extract(answer, '$.result.Applied[0].stream')) END)";

fn add_column(tx: &Transaction, table: &str, column: &str, def: &str) -> rusqlite::Result<()> {
    let sql = "SELECT COUNT(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2";
    let there: bool = tx.query_row(sql, params![table, column], |r| r.get(0))?;
    if !there {
        tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {def}"))?;
    }
    Ok(())
}

/// Brings the database up to `STEPS`, all in one transaction, or refuses
/// one whose schema is newer than this code knows.
fn migrate(conn: &mut Connection) -> Result<()> {
    let known = STEPS.len() as u32;
    let found = (|| -> rusqlite::Result<u32> {
        conn.execute_batch("PRAGMA journal_mode = WAL;")?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let found: u32 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if found <= known {
            for step in &STEPS[found as usize..] {
                step(&tx)?;
            }
            tx.execute_batch(&format!("PRAGMA user_version = {known}"))?;
            tx.commit()?;
        }
        Ok(found)
    })()
    .map_err(backend)?;
    if found > known {
        Err(StoreError::NewerSchema { found, known })
    } else {
        Ok(())
    }
}

pub struct SqliteStore {
    worker: Worker,
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
    /// Opens the database at `path`, creating it if needed and bringing its
    /// schema up to this code's (`StoreError::NewerSchema` if it is newer).
    pub fn open(path: impl AsRef<Path>) -> Result<SqliteStore> {
        let mut conn = Connection::open(path.as_ref()).map_err(backend)?;
        conn.busy_timeout(Duration::from_secs(10))
            .map_err(backend)?;
        migrate(&mut conn)?;
        Ok(SqliteStore {
            worker: Worker::start(conn).map_err(backend)?,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs `f` on this handle's connection, on its thread.
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.worker
            .run(f)
            .await
            .unwrap_or_else(|| Err(backend("the store's thread has stopped")))
    }

    /// Runs `f` in one immediate transaction: a writer, alone.
    async fn write<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Transaction) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run(|conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            let out = f(&tx)?;
            tx.commit().map_err(backend)?;
            Ok(out)
        })
        .await
    }
}

// ---- rows ----

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

const WORK_COLUMNS: &str = "kind, id, name, process, step, payload, due, ordering_key, attempts, lease, holder, lease_until, claimed_at, seq, woken, taken_over";

/// A work row as read, before its columns are decoded.
struct WorkRow {
    kind: String,
    id: String,
    process: Option<String>,
    lease: Option<String>,
    name: String,
    step: Option<String>,
    payload: String,
    due: i64,
    ordering_key: Option<String>,
    attempts: i64,
    holder: Option<String>,
    lease_until: Option<i64>,
    claimed_at: Option<i64>,
    seq: i64,
    woken: Option<i64>,
    taken_over: bool,
}

fn work(row: &Row) -> rusqlite::Result<WorkRow> {
    Ok(WorkRow {
        kind: row.get(0)?,
        id: row.get(1)?,
        process: row.get(3)?,
        lease: row.get(9)?,
        name: row.get(2)?,
        step: row.get(4)?,
        payload: row.get(5)?,
        due: row.get(6)?,
        ordering_key: row.get(7)?,
        attempts: row.get(8)?,
        holder: row.get(10)?,
        lease_until: row.get(11)?,
        claimed_at: row.get(12)?,
        seq: row.get(13)?,
        woken: row.get(14)?,
        taken_over: row.get(15)?,
    })
}

/// A work row decoded, with its sequence number and the wake it keeps; a
/// column it cannot decode is an error, never a guess.
fn to_work(r: WorkRow) -> Result<Stored> {
    let w = Work {
        kind: WorkKind::parse(&r.kind)
            .ok_or_else(|| backend(format!("work row of unknown kind {:?}", r.kind)))?,
        id: uuid(&r.id)?,
        name: r.name,
        process: r.process.as_deref().map(uuid).transpose()?,
        step: r.step,
        payload: parse(&r.payload)?,
        due: Instant(r.due),
        ordering_key: r.ordering_key,
        attempts: r.attempts as u32,
        lease: r.lease.as_deref().map(uuid).transpose()?,
        holder: r.holder,
        lease_until: r.lease_until.map(Instant),
        claimed_at: r.claimed_at.map(Instant),
        taken_over: r.taken_over,
    };
    Ok(Stored {
        work: w,
        seq: r.seq,
        woken: r.woken.map(Instant),
    })
}

struct Stored {
    work: Work,
    seq: i64,
    woken: Option<Instant>,
}

fn works(conn: &Connection, sql: &str, args: impl rusqlite::Params) -> Result<Vec<Stored>> {
    let mut stmt = conn.prepare(sql).map_err(backend)?;
    let rows = stmt.query_map(args, work).map_err(backend)?;
    rows.map(|r| r.map_err(backend).and_then(to_work)).collect()
}

/// Writes back what a row transition of the store crate changed.
fn rewrite(tx: &Transaction, w: &Work, woken: Option<Instant>) -> Result<()> {
    tx.execute(
        "UPDATE work SET due = ?3, attempts = ?4, lease = ?5, holder = ?6, lease_until = ?7, claimed_at = ?8,
            taken_over = ?9, woken = ?10
         WHERE kind = ?1 AND id = ?2",
        params![
            w.kind.as_str(),
            w.id.to_string(),
            w.due.0,
            w.attempts,
            w.lease.map(|l| l.to_string()),
            w.holder,
            w.lease_until.map(|i| i.0),
            w.claimed_at.map(|i| i.0),
            w.taken_over,
            woken.map(|i| i.0),
        ],
    )
    .map(|_| ())
    .map_err(backend)
}

/// Moves a row, if it is there, by a transition of the store crate: read,
/// changed, written back, in the one write transaction.
fn transition(
    tx: &Transaction,
    kind: WorkKind,
    id: Uuid,
    f: impl FnOnce(&mut Work, &mut Option<Instant>),
) -> Result<()> {
    let sql = format!("SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND id = ?2");
    match works(tx, &sql, params![kind.as_str(), id.to_string()])?.pop() {
        Some(Stored {
            mut work,
            mut woken,
            ..
        }) => {
            f(&mut work, &mut woken);
            rewrite(tx, &work, woken)
        }
        None => Ok(()),
    }
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
        WorkChange::Wake { kind, id, due } => {
            return transition(tx, *kind, *id, |w, woken| wake_row(w, woken, *due));
        }
        WorkChange::Done { kind, id } => {
            tx.execute("DELETE FROM work WHERE kind = ?1 AND id = ?2", params![kind.as_str(), id.to_string()])
        }
        WorkChange::Release { kind, id, due, .. } => {
            return transition(tx, *kind, *id, |w, woken| release_row(w, woken, *due));
        }
        WorkChange::Pace { name, key, not_before } => tx.execute(
            "INSERT OR REPLACE INTO paces (name, key, not_before) VALUES (?1, ?2, ?3)",
            params![name, key, not_before.0],
        ),
    }
    .map(|_| ())
    .map_err(backend)
}

// ---- projection tables ----

/// Quotes an identifier.
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn table_name(projection: &str, table: &str) -> String {
    ident(&format!("rows:{projection}:{table}"))
}

fn has_table(conn: &Connection, projection: &str, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM projection_tables WHERE projection = ?1 AND tbl = ?2",
        params![projection, table],
        |_| Ok(()),
    )
    .optional()
    .map(|f| f.is_some())
    .map_err(backend)
}

fn create_table(tx: &Transaction, projection: &str, table: &Table) -> Result<()> {
    let name = table_name(projection, &table.name);
    let mut columns = vec![
        "key TEXT PRIMARY KEY".to_string(),
        "row TEXT NOT NULL".to_string(),
    ];
    for c in &table.columns {
        columns.push(format!(
            "{} GENERATED ALWAYS AS (json_extract(row, '$.{c}')) VIRTUAL",
            ident(c)
        ));
    }
    tx.execute_batch(&format!("CREATE TABLE {name} ({})", columns.join(", ")))
        .map_err(backend)?;
    for (n, index) in table.indexes.iter().enumerate() {
        let on: Vec<String> = index.iter().map(|c| ident(c)).collect();
        let index_name = ident(&format!("index:{projection}:{}:{n}", table.name));
        tx.execute_batch(&format!(
            "CREATE INDEX {index_name} ON {name} ({})",
            on.join(", ")
        ))
        .map_err(backend)?;
    }
    tx.execute(
        "INSERT INTO projection_tables (projection, tbl) VALUES (?1, ?2)",
        params![projection, table.name],
    )
    .map(|_| ())
    .map_err(backend)
}

fn read_row(conn: &Connection, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
    if !has_table(conn, projection, table)? {
        return Ok(None);
    }
    let sql = format!(
        "SELECT row FROM {} WHERE key = ?1",
        table_name(projection, table)
    );
    let found: Option<String> = conn
        .query_row(&sql, [key], |r| r.get(0))
        .optional()
        .map_err(backend)?;
    found.map(|s| parse(&s)).transpose()
}

fn apply_ops(tx: &Transaction, projection: &str, ops: &[RowOp]) -> Result<()> {
    for op in ops {
        let (table, key) = op_target(op);
        let name = table_name(projection, table);
        match apply_op(read_row(tx, projection, table, key)?, op) {
            Some(row) => tx.execute(
                &format!("INSERT OR REPLACE INTO {name} (key, row) VALUES (?1, ?2)"),
                params![key, json(&row)?],
            ),
            None => tx.execute(&format!("DELETE FROM {name} WHERE key = ?1"), [key]),
        }
        .map_err(backend)?;
    }
    Ok(())
}

fn keyed_rows(
    conn: &Connection,
    sql: &str,
    args: impl rusqlite::Params,
) -> Result<Vec<(String, Value)>> {
    let mut stmt = conn.prepare(sql).map_err(backend)?;
    let rows = stmt
        .query_map(args, |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(backend)?;
    rows.map(|r| {
        let (k, v) = r.map_err(backend)?;
        Ok((k, parse(&v)?))
    })
    .collect()
}

/// A JSON value as SQLite compares it with what `json_extract` gives.
fn scalar(v: &Value) -> Sql {
    match v {
        Value::Null => Sql::Null,
        Value::Bool(b) => Sql::Integer(i64::from(*b)),
        Value::Number(n) => n
            .as_i64()
            .map_or_else(|| Sql::Real(n.as_f64().unwrap_or(0.0)), Sql::Integer),
        Value::String(s) => Sql::Text(s.clone()),
        other => Sql::Text(other.to_string()),
    }
}

impl Store for SqliteStore {
    async fn read_stream(&self, stream: &StreamId, after: Version) -> Result<Vec<Event>> {
        let stream = stream.to_string();
        self.run(move |c| {
            let sql = format!("SELECT {EVENT_COLUMNS} FROM events WHERE stream = ?1 AND version > ?2 ORDER BY version");
            events(c, &sql, params![stream, after.0 as i64])
        })
        .await
    }

    async fn answer(&self, key: Uuid) -> Result<Option<Answer>> {
        self.run(move |c| {
            let key = key.to_string();
            let sql = format!("SELECT {EVENT_COLUMNS} FROM events INDEXED BY events_key WHERE json_extract(meta, '$.key') = ?1 ORDER BY seq");
            read_answer(events(c, &sql, [&key])?, || {
                let sql = "SELECT answer FROM answers WHERE key = ?1";
                let found: Option<String> = c.query_row(sql, [&key], |r| r.get(0)).optional().map_err(backend)?;
                found.map(|a| parse::<Kept>(&a)).transpose()
            })
        })
        .await
    }

    async fn commit(&self, unit: Unit) -> Result<Vec<Event>> {
        self.write(move |tx| {
            check_unit(
                &unit,
                |stream| {
                    tx.query_row("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream = ?1", [stream.to_string()], |r| r.get::<_, i64>(0))
                        .map(|v| Version(v as u64))
                        .map_err(backend)
                },
                |key| {
                    tx.query_row(ANSWERED, [key.to_string()], |r| r.get(0)).map_err(backend)
                },
                |kind, id| Ok(lease_of(tx, kind, id)?.and_then(|l| l.parse().ok())),
            )?;
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
            if let Some((key, kept)) = unit.answer.and_then(|(k, a)| Some((k, Kept::of(&a)?))) {
                tx.execute("INSERT INTO answers (key, answer) VALUES (?1, ?2)", params![key.to_string(), json(&kept)?])
                .map_err(backend)?;
            }
            for c in &unit.work {
                change(tx, c)?;
            }
            Ok(committed)
        })
        .await
    }

    async fn read_log(&self, after: Position, limit: usize) -> Result<Vec<Event>> {
        self.run(move |c| {
            let sql =
                format!("SELECT {EVENT_COLUMNS} FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2");
            events(c, &sql, params![after.1 as i64, limit as i64])
        })
        .await
    }

    async fn claim(&self, claim: Claim) -> Result<Vec<Work>> {
        self.write(move |tx| {
            let sql = format!(
                "SELECT {WORK_COLUMNS} FROM work
                 WHERE kind = ?1 AND due <= ?2 AND (lease_until IS NULL OR lease_until <= ?2)
                 ORDER BY due, seq"
            );
            let mut claimed = Vec::new();
            for Stored { work: mut w, seq, mut woken } in works(tx, &sql, params![claim.kind.as_str(), claim.now.0])? {
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
                claim_row(&mut w, &mut woken, &claim);
                rewrite(tx, &w, woken)?;
                claimed.push(w);
            }
            Ok(claimed)
        })
        .await
    }

    async fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> Result<bool> {
        self.write(move |tx| {
            let n = tx
                .execute(
                    "UPDATE work SET lease_until = ?5 WHERE kind = ?1 AND id = ?2 AND lease = ?3 AND lease_until > ?4",
                    params![kind.as_str(), id.to_string(), lease.to_string(), now.0, until.0],
                )
                .map_err(backend)?;
            Ok(n == 1)
        })
        .await
    }

    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        self.run(move |c| {
            let sql = format!("SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND id = ?2");
            Ok(works(c, &sql, params![kind.as_str(), id.to_string()])?
                .into_iter()
                .next()
                .map(|s| s.work))
        })
        .await
    }

    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        let name = name.to_string();
        self.run(move |c| {
            let sql = format!(
                "SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND name = ?2 ORDER BY seq"
            );
            Ok(works(c, &sql, params![kind.as_str(), name])?
                .into_iter()
                .map(|s| s.work)
                .collect())
        })
        .await
    }

    async fn snapshot(&self, stream: &StreamId) -> Result<Option<Snapshot>> {
        let stream = stream.to_string();
        self.run(move |c| {
            let found: Option<(i64, String, String)> = c
                .query_row(
                    "SELECT version, code, state FROM snapshots WHERE stream = ?1",
                    [stream],
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
        })
        .await
    }

    async fn keep_snapshot(&self, stream: &StreamId, snapshot: Snapshot) -> Result<()> {
        let stream = stream.to_string();
        self.write(move |tx| {
            tx.execute(
                "INSERT INTO snapshots (stream, version, code, state) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (stream) DO UPDATE SET version = excluded.version, code = excluded.code, state = excluded.state
                 WHERE snapshots.version < excluded.version OR snapshots.code <> excluded.code",
                params![stream, snapshot.version.0 as i64, snapshot.code, json(&snapshot.state)?],
            )
            .map(|_| ())
            .map_err(backend)
        })
        .await
    }

    async fn checkpoint(&self, projection: &str) -> Result<Option<Checkpoint>> {
        let projection = projection.to_string();
        self.run(move |c| {
            c.query_row(
                "SELECT version, schema, position, stopped FROM checkpoints WHERE projection = ?1",
                [projection],
                |r| {
                    Ok(Checkpoint {
                        version: r.get::<_, i64>(0)? as u32,
                        schema: r.get(1)?,
                        position: Position(0, r.get::<_, i64>(2)? as u64),
                        stopped: r.get::<_, i64>(3)? != 0,
                    })
                },
            )
            .optional()
            .map_err(backend)
        })
        .await
    }

    async fn reset_projection(
        &self,
        projection: &str,
        version: u32,
        schema: &str,
        tables: &[Table],
    ) -> Result<()> {
        let (projection, schema, tables) =
            (projection.to_string(), schema.to_string(), tables.to_vec());
        self.write(move |tx| {
            let old: Vec<String> = {
                let mut stmt = tx.prepare("SELECT tbl FROM projection_tables WHERE projection = ?1").map_err(backend)?;
                let rows = stmt.query_map([&projection], |r| r.get(0)).map_err(backend)?;
                rows.collect::<rusqlite::Result<_>>().map_err(backend)?
            };
            for t in old {
                tx.execute_batch(&format!("DROP TABLE IF EXISTS {}", table_name(&projection, &t))).map_err(backend)?;
            }
            tx.execute("DELETE FROM projection_tables WHERE projection = ?1", [&projection]).map_err(backend)?;
            tx.execute("DELETE FROM dead_letters WHERE projection = ?1", [&projection]).map_err(backend)?;
            for t in &tables {
                create_table(tx, &projection, t)?;
            }
            tx.execute(
                "INSERT OR REPLACE INTO checkpoints (projection, version, schema, position, stopped) VALUES (?1, ?2, ?3, 0, 0)",
                params![projection, version, schema],
            )
            .map(|_| ())
            .map_err(backend)
        })
        .await
    }

    async fn apply_projection(&self, batch: Batch) -> Result<()> {
        self.write(move |tx| {
            let at: Option<(i64, i64)> = tx
                .query_row("SELECT version, position FROM checkpoints WHERE projection = ?1", [&batch.projection], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()
                .map_err(backend)?;
            check_batch(
                &batch,
                at.map(|(v, p)| (v as u32, Position(0, p as u64))),
                |e| {
                    tx.query_row("SELECT 1 FROM dead_letters WHERE projection = ?1 AND event = ?2", params![batch.projection, e.to_string()], |_| Ok(()))
                        .optional()
                        .map(|f| f.is_some())
                        .map_err(backend)
                },
                |table| has_table(tx, &batch.projection, table),
            )?;
            for e in &batch.resolved {
                tx.execute("DELETE FROM dead_letters WHERE projection = ?1 AND event = ?2", params![batch.projection, e.to_string()])
                    .map_err(backend)?;
            }
            apply_ops(tx, &batch.projection, &batch.ops)?;
            for d in &batch.dead {
                tx.execute(
                    "INSERT INTO dead_letters (projection, position, event, stream, version, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![d.projection, d.position.1 as i64, d.event.to_string(), d.stream.to_string(), d.version.0 as i64, d.error],
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
        .await
    }

    async fn query(&self, projection: &str, query: &Query) -> Result<Vec<(String, Value)>> {
        let (projection, query) = (projection.to_string(), query.clone());
        self.run(move |c| {
            if !has_table(c, &projection, &query.table)? {
                return Ok(Vec::new());
            }
            let mut sql = format!(
                "SELECT key, row FROM {}",
                table_name(&projection, &query.table)
            );
            let mut args = Vec::new();
            for (n, f) in query.filters.iter().enumerate() {
                sql.push_str(if n == 0 { " WHERE " } else { " AND " });
                sql.push_str(&format!("{} {} ?{}", ident(&f.column), f.op.sql(), n + 1));
                args.push(scalar(&f.value));
            }
            sql.push_str(" ORDER BY ");
            for o in &query.order {
                sql.push_str(&format!(
                    "{} {}, ",
                    ident(&o.column),
                    if o.descending { "DESC" } else { "ASC" }
                ));
            }
            sql.push_str("key");
            if let Some(limit) = query.limit {
                sql.push_str(&format!(" LIMIT {limit}"));
            }
            keyed_rows(c, &sql, params_from_iter(args))
        })
        .await
    }

    async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>> {
        let (projection, table) = (projection.to_string(), table.to_string());
        self.run(move |c| {
            if !has_table(c, &projection, &table)? {
                return Ok(Vec::new());
            }
            keyed_rows(
                c,
                &format!(
                    "SELECT key, row FROM {} ORDER BY key",
                    table_name(&projection, &table)
                ),
                [],
            )
        })
        .await
    }

    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        let (projection, table, key) = (projection.to_string(), table.to_string(), key.to_string());
        self.run(move |c| read_row(c, &projection, &table, &key))
            .await
    }

    async fn dead_letters(&self, projection: &str) -> Result<Vec<DeadLetter>> {
        let projection = projection.to_string();
        self.run(move |c| {
            let mut stmt = c
                .prepare("SELECT position, event, stream, version, error FROM dead_letters WHERE projection = ?1 ORDER BY position")
                .map_err(backend)?;
            let rows = stmt
                .query_map([&projection], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?, r.get::<_, String>(4)?))
                })
                .map_err(backend)?;
            rows.map(|r| {
                let (position, event, stream, version, error) = r.map_err(backend)?;
                Ok(DeadLetter {
                    projection: projection.clone(),
                    position: Position(0, position as u64),
                    event: uuid(&event)?,
                    stream: StreamId::try_from(stream).map_err(backend)?,
                    version: Version(version as u64),
                    error,
                })
            })
            .collect()
        })
        .await
    }
}
