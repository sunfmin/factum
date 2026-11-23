//! The database file as this code keeps it: its schema, brought up to date
//! at open one step per change, and its free pages given back as writes
//! delete. The store asks for a connection and steps the free pages after
//! each write; how the file got its shape is held here alone.

use std::path::Path;
use std::time::Duration;

use factum_store::{Result, StoreError};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};

use crate::backend;

/// Opens the database at `path`, creating it if needed: writers wait for
/// each other up to the busy timeout, and its schema is brought up to this
/// code's (`StoreError::NewerSchema` if it is newer) and its `auto_vacuum`
/// made incremental.
pub(crate) fn open(path: &Path) -> Result<Connection> {
    let mut conn = Connection::open(path).map_err(backend)?;
    conn.busy_timeout(Duration::from_secs(10))
        .map_err(backend)?;
    migrate(&mut conn)?;
    make_incremental(&conn, path).map_err(backend)?;
    Ok(conn)
}

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
    // factum#43: a runner's outcome is an event committed with its submit,
    // so nothing is kept per key beside the log. A row left from a runner
    // that died between its two commits goes: that step or schedule decides
    // again, that recurring command may run once more in the period this
    // opens in.
    |tx| tx.execute_batch("DROP TABLE answers"),
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
        // A new file is incremental from its first table; an older one is
        // made so by `make_incremental`.
        conn.execute_batch(
            "PRAGMA auto_vacuum = INCREMENTAL; PRAGMA journal_mode = WAL; PRAGMA journal_size_limit = 67108864;",
        )?;
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

/// Pages one step gives back (`incremental_vacuum`): 4 MiB of 4 KiB pages,
/// 28 ms on factorium's host, far inside the busy timeout a waiting writer
/// has; with none free, 20 µs.
const STEP: i64 = 1024;

/// Switches a file written before factum#44 to incremental `auto_vacuum`,
/// which takes one full `VACUUM`. It runs at open, before the store serves
/// anyone, and says how long it took.
fn make_incremental(conn: &Connection, path: &Path) -> rusqlite::Result<()> {
    if conn.query_row("PRAGMA auto_vacuum", [], |r| r.get::<_, i64>(0))? != 2 {
        let began = std::time::Instant::now();
        conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL; VACUUM;")?;
        let took = began.elapsed().as_secs_f64();
        eprintln!(
            "factum-sqlite: {}: auto_vacuum made incremental by VACUUM in {took:.2}s",
            path.display()
        );
    }
    Ok(())
}

/// Gives back up to `STEP` free pages, its own short write. It only frees
/// space, so a step that fails (another writer held the file past the busy
/// timeout) is left to the next write.
pub(crate) fn give_back(conn: &Connection) {
    // It answers a row for each page it frees, and frees one per row read.
    let _ = (|| -> rusqlite::Result<()> {
        let mut step = conn.prepare(&format!("PRAGMA incremental_vacuum({STEP})"))?;
        let mut pages = step.query([])?;
        while pages.next()?.is_some() {}
        Ok(())
    })();
}
