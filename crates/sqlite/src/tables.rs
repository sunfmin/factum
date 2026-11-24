//! A projection's tables, each a SQL table of its own,
//! `"rows:<projection>:<table>"`: the key, the row as JSON, and each declared
//! column generated from the row (`json_extract`), indexed as declared, so a
//! query filters and orders in SQL. How a table is named, made and queried
//! is held here alone; the store passes projections, tables and keys.

use factum_core::{Query, RowOp, Table, Value};
use factum_store::{Result, apply_op, op_target};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use crate::{backend, json, parse};

/// Quotes an identifier.
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn table_name(projection: &str, table: &str) -> String {
    ident(&format!("rows:{projection}:{table}"))
}

/// Whether `projection` declared `table` at its last reset.
pub(crate) fn has(conn: &Connection, projection: &str, table: &str) -> Result<bool> {
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
pub(crate) fn reset(tx: &Transaction, projection: &str, tables: &[Table]) -> Result<()> {
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
pub(crate) fn apply(tx: &Transaction, projection: &str, ops: &[RowOp]) -> Result<()> {
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

/// Every row of a table, by key; none if the table is not there.
pub(crate) fn rows(
    conn: &Connection,
    projection: &str,
    table: &str,
) -> Result<Vec<(String, Value)>> {
    if !has(conn, projection, table)? {
        return Ok(Vec::new());
    }
    keyed_rows(
        conn,
        &format!(
            "SELECT key, row FROM {} ORDER BY key",
            table_name(projection, table)
        ),
        [],
    )
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
