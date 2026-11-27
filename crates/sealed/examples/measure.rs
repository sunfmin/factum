//! The evidence ADR 0021 records, measured on the host it runs on:
//! `cargo run --release -p factum-sealed --example measure [dir]`.
//!
//! A file of 10,000 events; the median of 100 commits of one event each on
//! it; the median of 10 opens of it; then one work row renewed until a write
//! compacts the file, that write's time, and the file's size against the
//! bytes of what it holds.

use std::time::{Duration, Instant as Clock};

use factum_core::{
    By, Event, Instant, Meta, Position, StreamId, Uuid, Version, Work, WorkChange, WorkKind,
};
use factum_sealed::SealedStore;
use factum_store::{Append, Claim, Store, Unit};
use serde_json::json;

const KEY: [u8; 32] = [7; 32];

fn event(stream: &StreamId, version: u64) -> Event {
    Event {
        id: Uuid::now_v7(),
        stream: stream.clone(),
        version: Version(version),
        kind: "OrderPlaced".into(),
        type_version: 1,
        data: json!({ "order": stream.id, "total": version, "lines": ["tea", "cake"] }),
        meta: Meta {
            now: Instant(1_767_225_600_000),
            by: By::Principal("person:ann".into()),
            key: Some(Uuid::now_v7()),
            command: "order.place".into(),
            fingerprint: Some(Uuid::now_v7()),
        },
        position: Position::default(),
    }
}

/// Appends `n` events to a new stream, in one commit.
fn append(n: u64) -> Unit {
    let s = StreamId::new("order", Uuid::now_v7());
    Unit {
        appends: vec![Append {
            stream: s.clone(),
            expected: Version(0),
            events: (1..=n).map(|v| event(&s, v)).collect(),
        }],
        ..Unit::default()
    }
}

fn median(mut d: Vec<Duration>) -> Duration {
    d.sort();
    d[d.len() / 2]
}

fn size(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dir = std::env::args()
        .nth(1)
        .map_or_else(std::env::temp_dir, Into::into);
    let path = dir.join(format!(
        "factum-sealed-measure-{}.sealed",
        std::process::id()
    ));
    let store = SealedStore::open(&path, KEY).unwrap();
    for _ in 0..99 {
        store.commit(append(100)).await.unwrap();
    }
    let mut commits = Vec::new();
    for _ in 0..100 {
        let t = Clock::now();
        store.commit(append(1)).await.unwrap();
        commits.push(t.elapsed());
    }
    println!("commit of 1 event on 10,000: median {:?}", median(commits));

    let opens = (0..10).map(|_| {
        let t = Clock::now();
        drop(SealedStore::open(&path, KEY).unwrap());
        t.elapsed()
    });
    let open = median(opens.collect());
    println!("open of {} bytes: median of 10 {open:?}", size(&path));

    let id = Uuid::now_v7();
    let mut row = Work::new(WorkKind::Task, id, "pack", Instant(0));
    row.payload = json!({ "pad": "x".repeat(300) });
    let put = Unit {
        work: vec![WorkChange::Put(row)],
        ..Unit::default()
    };
    store.commit(put).await.unwrap();
    let claim = Claim {
        kind: WorkKind::Task,
        names: None,
        now: Instant(0),
        holder: "a".into(),
        lease_ms: 1,
        limit: 1,
    };
    let lease = store.claim(claim).await.unwrap()[0].lease.unwrap();
    let mut writes = Vec::new();
    for i in 1.. {
        let before = size(&path);
        let t = Clock::now();
        let renew = store.renew(WorkKind::Task, id, lease, Instant(0), Instant(i));
        assert!(renew.await.unwrap());
        let took = t.elapsed();
        if size(&path) < before {
            println!(
                "compaction: {took:?} for the write that did it (a write without: median {:?}); \
                 {before} bytes before, {} after",
                median(writes),
                size(&path)
            );
            break;
        }
        writes.push(took);
    }

    let log = store
        .read_log(Position::default(), usize::MAX)
        .await
        .unwrap();
    let work = store.work(WorkKind::Task, id).await.unwrap().unwrap();
    let held: usize = log
        .iter()
        .map(|e| serde_json::to_vec(e).unwrap().len())
        .sum::<usize>()
        + serde_json::to_vec(&work).unwrap().len();
    println!(
        "holds {} events and 1 work row, {held} bytes as JSON; the file is {} bytes",
        log.len(),
        size(&path)
    );
    drop(store);
    std::fs::remove_file(&path).unwrap();
    let mut lock = path.into_os_string();
    lock.push(".lock");
    std::fs::remove_file(lock).unwrap();
}
