# factum

A minimal event-sourced core for Rust. Business facts go into an
append-only log; the rules that decide them are pure functions; and the
work that outlives one command — multi-step processes, calls to other
systems, tasks for workers, scheduled and recurring commands, waits for
events, read models — is driven from the same log, exactly once where it
can be and visibly "unknown" where it cannot.

```rust
use std::sync::Arc;
use factum::{Engine, MemoryStore, Model, Submission, SystemClock, By, Uuid};

let model = Arc::new(Model::new().command::<PlaceOrder>().process::<Checkout>());
let engine = Engine::new(Arc::new(MemoryStore::new()), model, Arc::new(SystemClock), "runner-1");

engine.submit(Submission::new("order.place", params, Uuid::now_v7(), By::Principal("person:ann".into()))?).await?;
engine.tick().await?; // advance processes, deliver actions, run what is due, catch projections up
```

| Crate | |
|---|---|
| `factum` | The engine. Start here. |
| `factum-core` | The pure half: events, streams, keys, `decide`, `next`. |
| `factum-store` | The store contract, and an in-memory store. |
| `factum-sqlite` | The store on SQLite. |
| `factum-conformance` | The scenarios every store must pass. |

How it works, what it guarantees and what it leaves out: [SPEC.md](SPEC.md).
The scenarios in `crates/conformance/src/scenarios.rs` are its executable
documentation: each states one guarantee and proves it through the public
API.

Run every check: `./gate.sh`.
