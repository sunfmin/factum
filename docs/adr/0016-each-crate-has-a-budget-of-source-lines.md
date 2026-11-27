# 16. Each crate has a budget of source lines

## Decision

Each crate holds one part, and its `src/` stays within a budget of lines:

| Crate | Holds | Budget |
|---|---|---|
| `factum-core` | The pure half: events, streams, keys, the traits a program implements, process folding and `advance`, work rows and `effects`, retry arithmetic. No IO, no async, no clock. | 5,000 |
| `factum-store` | The `Store` trait (ADR 0010, ADR 0011) and the in-memory store, the reference every other store is held to. | 3,000 |
| `factum` | The engine: submissions, reads, ticks, the worker API, queries and dead letters, samples, the clock. Generic over `Store`; async, on no particular runtime; its futures are `Send`. | 5,000 |
| `factum-sqlite` | `Store` on SQLite: one file, writers take turns (`BEGIN IMMEDIATE`); each handle's connection on a thread of its own; each projection table a table of its own, declared columns generated from the row and indexed. | 3,000 |
| `factum-sealed` | `Store` on one file of sealed records, appended (ADR 0021): pure Rust, no `cc` for any target; several processes under an OS file lock; compacted when its waste passes half the file. | 2,500 |
| `factum-postgres` | `Store` on Postgres, extracted from bizkernel, many writers. Not built yet. | 3,000 |
| `factum-conformance` | The scenarios every store must pass (ADR 0014). Not published. | — |

`gate.sh` checks the budgets: a crate over its budget is red. Raising one
is a decision recorded here, not a side effect of a change.

## Why

A budget makes growth a choice someone names. The pure half and the
engine get the most room; a store is only its backend's half of the
contract.
