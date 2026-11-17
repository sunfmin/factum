//! The SQLite store, held to the conformance suite: each scenario on its own
//! database file, each runner on its own connection to it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use factum_conformance::Backend;
use factum_sqlite::SqliteStore;

struct Sqlite;

static NEXT: AtomicU64 = AtomicU64::new(0);

impl Backend for Sqlite {
    type S = SqliteStore;
    fn open(&self) -> Arc<SqliteStore> {
        let dir = std::env::temp_dir().join(format!("factum-sqlite-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.db", NEXT.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_file(&path);
        Arc::new(SqliteStore::open(path).unwrap())
    }
    fn reopen(&self, store: &Arc<SqliteStore>) -> Arc<SqliteStore> {
        Arc::new(SqliteStore::open(store.path()).unwrap())
    }
}

factum_conformance::suite!(Sqlite);
