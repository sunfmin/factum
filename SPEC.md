# factum — specification

factum is a minimal event-sourced core: an append-only log of facts, the
pure functions that decide what to record, and the few kinds of work that
outlive one command (processes, actions, tasks, schedules, waits,
projections). It is extracted from bizkernel, which will depend on it, and
it is the foundation afkconvoy v2 and later programs are built on.

factum is a library, not a server. It has no HTTP, no identity, no policy
language, no plug-in format and no UI. A program built on it adds those or
does without.

## 1. Concepts

factum has these concepts and no others. Adding one needs the owner's
approval and an entry here.

| # | Concept | What it is |
|---|---|---|
| 1 | **Event** | A fact: stream, version, stored type name, type version, data (JSON), meta (`now`, `by`, idempotency `key`, `command`), an id (UUIDv7) and, once committed, its position in the log. Never edited. |
| 2 | **Stream** | `<aggregate>/<uuid>`: the events of one thing, numbered 1, 2, 3… with no gap. An aggregate may declare **ending** event types; a stream whose last domain event ends it refuses every later command (`ended`); the engine's own `CommandScheduled`, appended after a command's domain events, is not a domain event and is skipped. |
| 3 | **Command** | A named request to change one stream. Its rules are `decide(state, params, by, now) → events \| Rejection`, pure; it may also schedule commands. State is folded from the stream by a pure `evolve`. Older event shapes are **upcast** on read, so decide, evolve and projections see only the current shape. |
| 4 | **Submission** | A command sent under an idempotency key. The same key with the same command and params answers the first answer, a refusal included; with others it is refused (`key_reused`). A writer that read an old version decides again on fresh state at the same `now`. **Plan** is decide without commit. |
| 5 | **Process** | Work across streams. Its progress is its own stream, `process/<id>` (`ProcessStarted`, `StepRequested`, `StepRecorded`, `ProcessFinished`, which ends it). `next(input, progress) → Run(command) \| Call(action) \| Queue(task) \| Await(event, within) \| Finish`, pure. A step's key is `UUIDv5(process, step)`, so a step re-run after a crash is replayed, not repeated. A process may start by itself on an event. One runner advances a process at a time, under a lease. |
| 6 | **Action** | A call to another system, requested by a step through an **outbox**: the request and its delivery row commit together. Delivered at least once under the step's key, or **at most once**, in which case a call whose answer was lost ends `Unknown` for `next` to reconcile. Each action declares its retry schedule (attempts, backoff, factor, cap, jitter) and timeout. |
| 7 | **Task** | Long work a step queues and a **worker** pulls: claim under a lease, heartbeat to keep it (never past the type's timeout), result or failure. Declared attempts and backoff; may be done at most once; may be cancelled; may carry an **ordering key** (one at a time per key, in order, paced). |
| 8 | **Schedule** | A decide asks for a command later by returning a `CommandScheduled`; it runs once its time comes, under `UUIDv5(event, "scheduled")`, and its outcome, a refusal included, is recorded and not retried. A **recurring** command runs once per period, under a key derived from its name and the period. |
| 9 | **Projection** | Pure code that turns each event into row changes (put, merge, delete) of the tables it **declares**: rows are JSON objects under a key; declared columns are what **queries** filter (`= <> < <= > >=`) and order by, and declared indexes make them fast. It reads only **final** events in commit order; rows and checkpoint move together. A new version, or any change to its tables, rebuilds it from the whole log. An event it fails on, or that writes an undeclared table, becomes a **dead letter**: skipped, or (declared `stop`) held there. A dead letter is **retried** with the code running now; a projection stopped at it then goes on. |
| 10 | **Snapshot** | A stream's state at a version, kept by the code that folded it and read only by that code. Derived: no answer depends on it. |
| 11 | **Sample** | A run of a process as some version left it — input, progress — and what `next` answered for it then. Taken from a running process, kept beside the code, and checked against a new version: a `next` that answers otherwise would make old runs take a step twice or wait forever. |

`by` (who applies a command) is factum's own for the work it originates —
`Process(stream)`, `Schedule(id)`, `Recurring(name)`, `Action(name)`,
`Worker(name)` — and the program's for everything else:
`Principal(text)`, as the program names its people, agents and systems.

`now` is read once per submission and once per tick and passed down.
Nothing in factum reads a clock anywhere else, so tests move time by hand.

**Work rows.** Everything pending — a process to advance, an action to
deliver, a task to hand out, a schedule to run, a wait to end — is one row
kind in one table beside the log. Every row is implied by an event
committed with it (one pure function, `effects`), so the rows never say
what the log does not. Every lease in factum is a claim on such a row.

**Values compare as SQLite compares them**, in every store: null before numbers before text; numbers by value; booleans as 0 and 1; objects and arrays as their JSON text; a comparison with null never holds. Results are ordered as asked, then by key.

**Not supported: branches.** The log has one line. A program that wants to
rehearse on a copy of the log builds it on top of factum (e.g. a second
store seeded from the first).

## 2. Crates

| Crate | Holds | Budget (source lines) |
|---|---|---|
| `factum-core` | The pure half: events, streams, keys, the typed and erased traits a program implements (aggregate, command, process, projection), process folding and `advance`, work rows and `effects`, retry arithmetic. No IO, no async, no clock. | 5,000 |
| `factum-store` | The `Store` trait (§3) and the in-memory store, the reference every other store is held to. | 3,000 |
| `factum` | The engine: `submit`, `plan`, `state`, `history`, `start`, `process`; `tick` / `settle` (dispatch, advance, deliver, run due, end waits, project); the worker API (`claim_task`, `heartbeat`, `finish_task`, `cancel_task`); `query`, `retry_dead_letter`; `sample` and `Model::check_samples`; the clock. Generic over `Store`; async, on no particular runtime; its futures are `Send`. | 5,000 |
| `factum-sqlite` | `Store` on SQLite: one file, writers take turns (`BEGIN IMMEDIATE`); each handle's connection on a thread of its own, so no call blocks the caller's runtime; each projection table a table of its own, declared columns generated from the row and indexed. | 3,000 |
| `factum-postgres` | `Store` on Postgres, extracted from bizkernel, many writers. Not built yet. | 3,000 |
| `factum-conformance` | The scenarios every store must pass (§4). Not published. | — |

`gate.sh` checks the budgets: a crate over its budget is red. Raising one
is a decision recorded here, not a side effect of a change.

## 3. The store contract

The store is the only place that knows the backend. Every method is one
short transaction; none holds a lock while the caller works.

- `read_stream(stream, after)`; `read_log(after, limit)` — only final
  events, in commit order: no later commit lands before the last returned.
- `commit(unit)` — events appended to streams at the versions their writer
  read, the submission's key and answer (kept with the events' positions),
  the work changes the events imply, and lease guards. All of it or none:
  `Conflict`, `KeyTaken` or `LeaseLost` otherwise.
- `answer(key)`.
- `claim(kind, names, now, holder, lease, limit)` — due rows no live lease
  holds, oldest due first; of rows sharing a name and ordering key only the
  first put, and not before its pace. A claim takes a new lease, counts an
  attempt, and says whether it took the row from a lease that ran out.
- `renew(...)` — only a lease that has not run out; `work(kind, id)`;
  `work_named(kind, name)`.
- A wake that comes while a row is claimed survives the claim's release.
- `snapshot` / `keep_snapshot`; `checkpoint` / `reset_projection` /
  `apply_projection` (refused unless the checkpoint is where the batch
  began); `resolve_dead_letter` (a retried dead letter's rows, and a
  stopped projection moved past it); `query`, `rows`, `row`, `dead_letters`.
  A checkpoint keeps the tables it was built with; `reset_projection` drops
  a projection's rows and dead letters and makes its declared tables anew.

Each store implements these as its backend allows: SQLite's single writer
makes commit order the sequence order, and Postgres will need an
appending-transaction horizon and `SKIP LOCKED` claims. Those details stay
inside each store.

## 4. Tests: only at the outermost layer

Every test drives the engine's public API, as a program would: submit,
start, tick, claim, read. No store, pure function or engine internal has
tests of its own. `factum-conformance` holds one scenario per guarantee,
over a small shop domain (orders, counters, checkout with a payment
stand-in, receipts, packing, deliveries, pickups, an order list), and
`factum_conformance::suite!(backend)` runs all of them on a store.

Scenarios run on a multi-threaded runtime; those about runners in parallel
spawn them as tasks of their own. To make a runner die between two writes,
or lose a race, a scenario wraps its store in `Flaky`: the next matching
commit fails, or another runner (or projector) writes first. Time moves
only when a scenario moves it.

Each scenario is held to account by mutation: breaking the rule it guards
must make it fail. Measured 2026-11-17 on 46 scenarios: 19 of 19
mutations of the engine, core and in-memory store, and 13 of 14 of the
SQLite store, are caught. The one not caught (a snapshot replaced by an
older one) changes no answer, only how long a fold takes. The SQLite suite
passed 30 runs of 30 in a row.

## 5. Migration: bizkernel onto factum

bizkernel's own gate and conformance scenarios are the oracle: every step
lands with both green, and is a commit that can be reverted alone.

1. bizkernel's key derivations, stream versions and process step types are
   replaced by `factum-core`'s (the derivations are already byte for byte
   bizkernel's, so its logs keep their keys). Its `Actor` becomes the text
   of `By::Principal`.
2. bizkernel's store traits are generalized to `factum-store`'s, with
   bizkernel's own types (callers, delegation, modules, policies) taken out
   of the signatures. bizkernel's branches, which every one of its store
   signatures carries today, are rebuilt on top of factum or dropped.
3. `factum-postgres`: the matching part of bizkernel's Postgres store moves
   behind the trait and passes the conformance suite.
4. bizkernel's submission and delivery loops move onto the engine;
   bizkernel keeps what wraps them (grants, approval, modules, HTTP).
5. afkconvoy v2 starts on factum + factum-sqlite.

## 6. Not in factum

Identity and sign-in; grants and policies; approval of risky commands;
plug-in modules and their ABI; HTTP and wire formats; live watches;
files, blobs and secrets; personal-data sealing; webhooks and intakes;
pages, apps and UI; hosted workers; bulk runs, sequences, reference data,
rate limits; archiving old months of the log; branches; read replicas.

## 7. Decisions, and why

- **`By::Principal` is text**, not a type parameter: every engine type would
  otherwise carry the program's principal type, and bizkernel already
  records its actors as text.
- **Every pending thing is one kind of work row** (§1): one claim, one lease
  and one wake rule serve processes, actions, tasks, schedules and waits.
- **Async, on no particular runtime.** The engine never spawns or sleeps; a
  writer that loses a race yields once before deciding again, up to 1000
  times (every loss is another writer's win, so the log always moves; the
  bound keeps one writer from losing forever). The SQLite store keeps its
  connection on its own thread.
- **Projection tables are declared**, and a change to them rebuilds the
  projection as a new version does: no one has to remember to bump the
  version. Queries take only declared columns, so every store can answer
  them with an index.
- **A dead letter is retried in place** with the code running now; a
  rebuild still clears them all.
- **Samples are taken from running processes** and checked by the model,
  without a store: holding a new `next` to old progress is a pure check.

## 8. Open

1. **bizkernel's branches.** factum has one line. When bizkernel moves onto
   factum (§5 step 2), its branches are either rebuilt on top of factum or
   dropped: the owner decides.
