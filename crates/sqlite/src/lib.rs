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
//! The events log is read and written here; the work table (`work`) and a
//! projection's tables (`tables`) keep their columns and SQL to themselves.

mod schema;
mod tables;
mod thread;
mod work;

use std::path::{Path, PathBuf};

use factum_core::{
    Event, Instant, Meta, Position, Query, StreamId, Table, Uuid, Value, Version, Work, WorkKind,
};
use factum_store::{
    Answer, Batch, Checkpoint, Claim, DeadLetter, Result, Snapshot, Store, StoreError, Unit,
    check_batch, check_unit, read_answer,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

use crate::thread::Worker;

/// Whether a key has an answer: events committed under it with a
/// fingerprint.
const ANSWERED: &str = "SELECT EXISTS (SELECT 1 FROM events INDEXED BY events_key
    WHERE json_extract(meta, '$.key') = ?1 AND json_extract(meta, '$.fingerprint') IS NOT NULL)";

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
        let conn = schema::open(path.as_ref())?;
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
            schema::give_back(conn);
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
            Ok(read_answer(events(c, &sql, [&key])?))
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
                |kind, id| work::lease(tx, kind, id),
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
            for c in &unit.work {
                work::change(tx, c)?;
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
        self.write(move |tx| work::claim(tx, &claim)).await
    }

    async fn renew(
        &self,
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        now: Instant,
        until: Instant,
    ) -> Result<bool> {
        self.write(move |tx| work::renew(tx, kind, id, lease, now, until))
            .await
    }

    async fn work(&self, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
        self.run(move |c| work::get(c, kind, id)).await
    }

    async fn work_named(&self, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
        let name = name.to_string();
        self.run(move |c| work::named(c, kind, &name)).await
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
            tables::reset(tx, &projection, &tables)?;
            tx.execute("DELETE FROM dead_letters WHERE projection = ?1", [&projection]).map_err(backend)?;
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
                |table| tables::has(tx, &batch.projection, table),
            )?;
            for e in &batch.resolved {
                tx.execute("DELETE FROM dead_letters WHERE projection = ?1 AND event = ?2", params![batch.projection, e.to_string()])
                    .map_err(backend)?;
            }
            tables::apply(tx, &batch.projection, &batch.ops)?;
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
        self.run(move |c| tables::query(c, &projection, &query))
            .await
    }

    async fn rows(&self, projection: &str, table: &str) -> Result<Vec<(String, Value)>> {
        let (projection, table) = (projection.to_string(), table.to_string());
        self.run(move |c| tables::query(c, &projection, &Query::table(&table)))
            .await
    }

    async fn row(&self, projection: &str, table: &str, key: &str) -> Result<Option<Value>> {
        let (projection, table, key) = (projection.to_string(), table.to_string(), key.to_string());
        self.run(move |c| tables::row(c, &projection, &table, &key))
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
