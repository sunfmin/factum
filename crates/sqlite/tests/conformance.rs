//! The SQLite store, held to the conformance suite: each scenario on its own
//! database file, each runner on its own connection to it. Each file starts
//! as factum before #14 wrote it, a schema copied from that release, never
//! derived from the code here; the tests below hold such a database, once
//! opened, to the shape of a new one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use factum::StoreError;
use factum_conformance::Backend;
use factum_sqlite::SqliteStore;
use rusqlite::Connection;

/// The schema before factum#14, as released.
const BEFORE_14: &str = "
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

/// How factorium's host was repaired by hand on 2026-11-20.
const BY_HAND: &str = "ALTER TABLE work ADD COLUMN taken_over INTEGER NOT NULL DEFAULT 0";

static NEXT: AtomicU64 = AtomicU64::new(0);

fn new_path() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("factum-sqlite-schema-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.db", NEXT.fetch_add(1, Ordering::SeqCst)));
    let _ = std::fs::remove_file(&path);
    path
}

/// A database file as factum before #14 left it, then `extra`.
fn old_database(extra: &str) -> PathBuf {
    let path = new_path();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(BEFORE_14).unwrap();
    conn.execute_batch(extra).unwrap();
    path
}

/// Every table's columns and every index's columns, and the data in `work`.
fn shape(path: &Path) -> Vec<String> {
    let conn = Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT m.type || ' ' || m.name || ': ' || group_concat(
                 p.name || ' ' || p.type || ' ' || p.\"notnull\" || ' ' || ifnull(p.dflt_value, '-') || ' ' || p.pk, ', ')
             FROM sqlite_master m, pragma_table_info(m.name) p
             WHERE m.type = 'table' AND m.name <> 'sqlite_sequence' GROUP BY m.name
             UNION ALL
             SELECT m.type || ' ' || m.name || ' on ' || m.tbl_name || ': ' || group_concat(i.name, ', ')
             FROM sqlite_master m, pragma_index_info(m.name) i
             WHERE m.type = 'index' GROUP BY m.name
             UNION ALL
             SELECT 'row ' || kind || ' ' || id || ' ' || taken_over FROM work
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn opened(path: &Path) -> Vec<String> {
    drop(SqliteStore::open(path).unwrap());
    shape(path)
}

#[test]
fn an_old_database_is_brought_to_the_schema_a_new_one_has() {
    let fresh = opened(&new_path());
    assert_eq!(opened(&old_database("")), fresh);
}

#[test]
fn a_database_given_taken_over_by_hand_opens_unchanged() {
    let path = old_database(&format!(
        "{BY_HAND}; INSERT INTO work (kind, id, seq, name, payload, due, attempts, taken_over)
         VALUES ('task', 'x', 1, 'pack', '{{}}', 0, 1, 1);"
    ));
    let before = shape(&path);
    assert_eq!(opened(&path), before);
    assert_eq!(
        opened(&path),
        before,
        "opening again changes nothing either"
    );
}

#[test]
fn a_database_with_kept_refusals_keeps_none_once_opened() {
    let refused = r#"{"fingerprint":"00000000-0000-0000-0000-000000000001","result":{"Rejected":{"code":"wait","message":"later"}}}"#;
    let applied =
        r#"{"fingerprint":"00000000-0000-0000-0000-000000000002","result":{"Applied":[]}}"#;
    let path = old_database(&format!(
        "INSERT INTO answers (key, answer) VALUES ('r', '{refused}'), ('a', '{applied}');"
    ));
    drop(SqliteStore::open(&path).unwrap());
    let conn = Connection::open(&path).unwrap();
    let mut stmt = conn
        .prepare("SELECT key FROM answers ORDER BY key")
        .unwrap();
    let keys: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(keys, ["a"]);
}

#[test]
fn a_database_of_a_newer_schema_is_refused_at_open() {
    let path = new_path();
    drop(SqliteStore::open(&path).unwrap());
    let known: u32 = Connection::open(&path)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    Connection::open(&path)
        .unwrap()
        .execute_batch(&format!("PRAGMA user_version = {}", known + 1))
        .unwrap();
    match SqliteStore::open(&path) {
        Err(e @ StoreError::NewerSchema { found, known: k }) => {
            assert_eq!((found, k), (known + 1, known));
            let said = e.to_string();
            assert!(
                said.contains(&found.to_string()) && said.contains(&k.to_string()),
                "{said}"
            );
        }
        Err(e) => panic!("refused with {e}, not NewerSchema"),
        Ok(_) => panic!("a newer schema was opened"),
    }
}

struct Sqlite;

impl Backend for Sqlite {
    type S = SqliteStore;
    fn open(&self) -> Arc<SqliteStore> {
        Arc::new(SqliteStore::open(old_database("")).unwrap())
    }
    fn reopen(&self, store: &Arc<SqliteStore>) -> Arc<SqliteStore> {
        Arc::new(SqliteStore::open(store.path()).unwrap())
    }
}

factum_conformance::suite!(Sqlite);
