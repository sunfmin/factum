//! A projection's state: its checkpoint, its dead letters and its tables.
//! Each table is `"rows:<projection>:<table>"`: the key, the row as JSON,
//! and each declared column generated from the row (`json_extract`),
//! indexed as declared, so a query filters and orders in SQL.

use factum_core::{Position, Query, RowOp, StreamId, Table, Value, Version};
use factum_store::{Batch, Checkpoint, DeadLetter, Result, apply_op, check_batch, op_target};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use crate::{backend, json, parse, uuid};

/// Where `projection` has got to, if it was ever reset.
pub(crate) fn checkpoint(conn: &Connection, projection: &str) -> Result<Option<Checkpoint>> {
    conn.query_row(
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
}

/// Starts `projection` over at `version` and `schema`: `tables`, empty, no
/// dead letters, at the log's start.
pub(crate) fn reset(
    tx: &Transaction,
    projection: &str,
    version: u32,
    schema: &str,
    tables: &[Table],
) -> Result<()> {
    reset_tables(tx, projection, tables)?;
    tx.execute(
        "DELETE FROM dead_letters WHERE projection = ?1",
        [projection],
    )
    .map_err(backend)?;
    tx.execute(
        "INSERT OR REPLACE INTO checkpoints (projection, version, schema, position, stopped) VALUES (?1, ?2, ?3, 0, 0)",
        params![projection, version, schema],
    )
    .map(|_| ())
    .map_err(backend)
}

/// Applies `batch` if its preconditions hold: its row ops, its resolved and
/// new dead letters, and its checkpoint moved to where it ends.
pub(crate) fn apply(tx: &Transaction, batch: &Batch) -> Result<()> {
    let projection = &batch.projection;
    let at: Option<(i64, i64)> = tx
        .query_row(
            "SELECT version, position FROM checkpoints WHERE projection = ?1",
            [projection],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(backend)?;
    check_batch(
        batch,
        at.map(|(v, p)| (v as u32, Position(0, p as u64))),
        |e| {
            tx.query_row(
                "SELECT 1 FROM dead_letters WHERE projection = ?1 AND event = ?2",
                params![projection, e.to_string()],
                |_| Ok(()),
            )
            .optional()
            .map(|f| f.is_some())
            .map_err(backend)
        },
        |table| has(tx, projection, table),
    )?;
    for e in &batch.resolved {
        tx.execute(
            "DELETE FROM dead_letters WHERE projection = ?1 AND event = ?2",
            params![projection, e.to_string()],
        )
        .map_err(backend)?;
    }
    apply_ops(tx, projection, &batch.ops)?;
    for d in &batch.dead {
        tx.execute(
            "INSERT INTO dead_letters (projection, position, event, stream, version, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![d.projection, d.position.1 as i64, d.event.to_string(), d.stream.to_string(), d.version.0 as i64, d.error],
        )
        .map_err(backend)?;
    }
    tx.execute(
        "UPDATE checkpoints SET position = ?2, stopped = ?3 WHERE projection = ?1",
        params![projection, batch.to.1 as i64, batch.stop],
    )
    .map(|_| ())
    .map_err(backend)
}

/// `projection`'s dead letters, in log order.
pub(crate) fn dead_letters(conn: &Connection, projection: &str) -> Result<Vec<DeadLetter>> {
    let mut stmt = conn
        .prepare("SELECT position, event, stream, version, error FROM dead_letters WHERE projection = ?1 ORDER BY position")
        .map_err(backend)?;
    let rows = stmt
        .query_map([projection], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
            ))
        })
        .map_err(backend)?;
    rows.map(|r| {
        let (position, event, stream, version, error) = r.map_err(backend)?;
        Ok(DeadLetter {
            projection: projection.to_string(),
            position: Position(0, position as u64),
            event: uuid(&event)?,
            stream: StreamId::try_from(stream).map_err(backend)?,
            version: Version(version as u64),
            error,
        })
    })
    .collect()
}

/// Quotes an identifier.
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn table_name(projection: &str, table: &str) -> String {
    ident(&format!("rows:{projection}:{table}"))
}

/// Whether `projection` declared `table` at its last reset.
fn has(conn: &Connection, projection: &str, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM projection_tables WHERE projection = ?1 AND tbl = ?2",
        params![projection, table],
        |_| Ok(()),
    )
    .optional()
    .map(|f| f.is_some())
    .map_err(backend)
}

/// Drops every table `projection` has and makes `tables`, empty.
fn reset_tables(tx: &Transaction, projection: &str, tables: &[Table]) -> Result<()> {
    let old: Vec<String> = {
        let mut stmt = tx
            .prepare("SELECT tbl FROM projection_tables WHERE projection = ?1")
            .map_err(backend)?;
        let rows = stmt
            .query_map([projection], |r| r.get(0))
            .map_err(backend)?;
        rows.collect::<rusqlite::Result<_>>().map_err(backend)?
    };
    for t in old {
        tx.execute_batch(&format!(
            "DROP TABLE IF EXISTS {}",
            table_name(projection, &t)
        ))
        .map_err(backend)?;
    }
    tx.execute(
        "DELETE FROM projection_tables WHERE projection = ?1",
        [projection],
    )
    .map_err(backend)?;
    for t in tables {
        create(tx, projection, t)?;
    }
    Ok(())
}

fn create(tx: &Transaction, projection: &str, table: &Table) -> Result<()> {
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

/// The row at `key`, if the table and the row are there.
pub(crate) fn row(
    conn: &Connection,
    projection: &str,
    table: &str,
    key: &str,
) -> Result<Option<Value>> {
    if !has(conn, projection, table)? {
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

/// Applies `ops` in order, each to the row as the ops before it left it.
fn apply_ops(tx: &Transaction, projection: &str, ops: &[RowOp]) -> Result<()> {
    for op in ops {
        let (table, key) = op_target(op);
        let name = table_name(projection, table);
        match apply_op(row(tx, projection, table, key)?, op) {
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

/// The rows `query` asks for, filtered, ordered and limited in SQL; none if
/// its table is not there.
pub(crate) fn query(
    conn: &Connection,
    projection: &str,
    query: &Query,
) -> Result<Vec<(String, Value)>> {
    if !has(conn, projection, &query.table)? {
        return Ok(Vec::new());
    }
    let mut sql = format!(
        "SELECT key, row FROM {}",
        table_name(projection, &query.table)
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
    keyed_rows(conn, &sql, params_from_iter(args))
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
