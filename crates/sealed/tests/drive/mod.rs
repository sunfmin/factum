//! What the tests and the measuring example both drive a sealed store with.

use factum_core::WorkChange;
use factum_core::{By, Event, Instant, Meta, Position, StreamId, Uuid, Version, Work, WorkKind};
use factum_sealed::SealedStore;
use factum_store::{Claim, Store, Unit};
use serde_json::{Value, json};

pub const KEY: [u8; 32] = [7; 32];

/// An event of `stream` at `version`, as a submission writes it.
pub fn event(stream: &StreamId, version: u64, data: Value) -> Event {
    Event {
        id: Uuid::now_v7(),
        stream: stream.clone(),
        version: Version(version),
        kind: "Noted".into(),
        type_version: 1,
        data,
        meta: Meta {
            now: Instant(1_767_225_600_000),
            by: By::Principal("person:ann".into()),
            key: Some(Uuid::now_v7()),
            command: "note".into(),
            fingerprint: Some(Uuid::now_v7()),
        },
        position: Position::default(),
    }
}

/// A task row of over `pad` bytes, put and claimed: its id and its lease.
/// Each renew of it writes the row again, all waste but the last.
pub async fn claimed_row(store: &SealedStore, pad: usize) -> (Uuid, Uuid) {
    let id = Uuid::now_v7();
    let mut row = Work::new(WorkKind::Task, id, "pack", Instant(0));
    row.payload = json!({ "pad": "x".repeat(pad) });
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
    (id, lease)
}
