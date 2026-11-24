//! The work table: its columns, decoded as `Work` with the wake it keeps,
//! moved by the store crate's row transitions in the caller's transaction,
//! and claimed in due order by ordering key and pace.

use factum_core::{Instant, Uuid, Work, WorkChange, WorkKind};
use factum_store::{Claim, Result, claim_row, release_row, wake_row};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};

use crate::{backend, json, parse, uuid};

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

/// The lease a row holds, if the row is there and holds one.
pub(crate) fn lease(tx: &Transaction, kind: WorkKind, id: Uuid) -> Result<Option<Uuid>> {
    tx.query_row(
        "SELECT lease FROM work WHERE kind = ?1 AND id = ?2",
        params![kind.as_str(), id.to_string()],
        |r| r.get::<_, Option<String>>(0),
    )
    .optional()
    .map_err(backend)
    .map(|l| l.flatten().and_then(|l| l.parse().ok()))
}

pub(crate) fn change(tx: &Transaction, change: &WorkChange) -> Result<()> {
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

/// Claims up to `claim.limit` due rows of its kind, in due order: a row
/// with an ordering key only when it is its key's first and its pace allows.
pub(crate) fn claim(tx: &Transaction, claim: &Claim) -> Result<Vec<Work>> {
    let sql = format!(
        "SELECT {WORK_COLUMNS} FROM work
                 WHERE kind = ?1 AND due <= ?2 AND (lease_until IS NULL OR lease_until <= ?2)
                 ORDER BY due, seq"
    );
    let mut claimed = Vec::new();
    for Stored {
        work: mut w,
        seq,
        mut woken,
    } in works(tx, &sql, params![claim.kind.as_str(), claim.now.0])?
    {
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
                .query_row(
                    "SELECT not_before FROM paces WHERE name = ?1 AND key = ?2",
                    params![w.name, key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(backend)?;
            if first != seq || pace.is_some_and(|p| p > claim.now.0) {
                continue;
            }
        }
        claim_row(&mut w, &mut woken, claim);
        rewrite(tx, &w, woken)?;
        claimed.push(w);
    }
    Ok(claimed)
}

/// Extends a lease still held and not run out; whether it was.
pub(crate) fn renew(
    tx: &Transaction,
    kind: WorkKind,
    id: Uuid,
    lease: Uuid,
    now: Instant,
    until: Instant,
) -> Result<bool> {
    let n = tx
                .execute(
                    "UPDATE work SET lease_until = ?5 WHERE kind = ?1 AND id = ?2 AND lease = ?3 AND lease_until > ?4",
                    params![kind.as_str(), id.to_string(), lease.to_string(), now.0, until.0],
                )
                .map_err(backend)?;
    Ok(n == 1)
}

pub(crate) fn get(conn: &Connection, kind: WorkKind, id: Uuid) -> Result<Option<Work>> {
    let sql = format!("SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND id = ?2");
    Ok(works(conn, &sql, params![kind.as_str(), id.to_string()])?
        .into_iter()
        .next()
        .map(|s| s.work))
}

/// The rows of a kind with a name, in the order they were put.
pub(crate) fn named(conn: &Connection, kind: WorkKind, name: &str) -> Result<Vec<Work>> {
    let sql = format!("SELECT {WORK_COLUMNS} FROM work WHERE kind = ?1 AND name = ?2 ORDER BY seq");
    Ok(works(conn, &sql, params![kind.as_str(), name])?
        .into_iter()
        .map(|s| s.work)
        .collect())
}
