//! Derived ids. Each is a UUIDv5, the same on every run and every server,
//! which is what lets a repeated step, schedule or delivery be replayed
//! instead of repeated. The derivations are bizkernel's, byte for byte, so
//! a log bizkernel wrote keeps its keys.

use serde_json::Value;
use uuid::{Uuid, uuid};

/// The namespace of recurring commands' keys (bizkernel `recurring::KEYS`).
const RECURRING: Uuid = uuid!("7d1f2b6e-4c3a-5e8b-9a0d-2f6c1e4b8a73");
/// The namespace of submission fingerprints.
const FINGERPRINTS: Uuid = uuid!("3b0c7a52-9d1e-5f4a-8c6b-1e2d3f4a5b6c");

/// The key of step `step` of process `process`.
pub fn step_key(process: Uuid, step: &str) -> Uuid {
    Uuid::new_v5(&process, step.as_bytes())
}

/// The id of the run of process `process` that the event `event` starts.
pub fn start_key(event: Uuid, process: &str) -> Uuid {
    Uuid::new_v5(&event, format!("{process}.start").as_bytes())
}

/// The key of the answer to the delivery whose key is `action`.
pub fn result_key(action: Uuid) -> Uuid {
    Uuid::new_v5(&action, b"result")
}

/// The key the command scheduled by event `event` runs under.
pub fn schedule_key(event: Uuid) -> Uuid {
    Uuid::new_v5(&event, b"scheduled")
}

/// The key the recurring command `name` runs under in the period starting
/// `period_start_seconds` after the epoch.
pub fn recurring_key(name: &str, period_start_seconds: i64) -> Uuid {
    Uuid::new_v5(
        &RECURRING,
        format!("{name}@{period_start_seconds}").as_bytes(),
    )
}

/// What a key was first sent with: the command and its params. JSON
/// objects serialize with sorted keys, so equal params give equal prints.
pub fn fingerprint(command: &str, params: &Value) -> Uuid {
    Uuid::new_v5(&FINGERPRINTS, format!("{command}\u{1f}{params}").as_bytes())
}
