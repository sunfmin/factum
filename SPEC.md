# factum — specification (draft 1, 2026-11-17)

factum is a minimal event-sourced core: an append-only log of facts, the
pure functions that decide what to record, and the few kinds of work that
outlive one command (processes, actions, tasks, schedules, projections).
It is extracted from bizkernel, which will depend on it, and it is the
foundation afkconvoy v2 and later programs are built on.

factum is a library, not a server. It has no HTTP, no identity, no
policy language, no plug-in format and no UI. A program built on it adds
those or does without.

Status: draft for review. Nothing here is built. Every guarantee below is
already built and tested in bizkernel; the ADR numbers point at where.

## 1. Concepts

factum has these concepts and no others. Adding one needs the owner's
approval and an entry here.

| # | Concept | What it is | bizkernel |
|---|---|---|---|
| 1 | **Event** | A fact: stream, version, stored type name, type version, data (JSON), meta (`id` UUIDv7, `now`, `by`, idempotency `key`). Never edited. | ADR 1, 8, 14 |
| 2 | **Stream** | `<aggregate>/<uuid>`: the events of one thing, numbered 1, 2, 3… with no gap. An aggregate may declare **ending** event types; a stream whose last event ends it refuses every later command (`ended`). | ADR 8, 15 |
| 3 | **Command** | A named request to change one stream. Its rules are `decide(state, params, by, now) → events \| Rejection`, pure. State is `fold(evolve, events)`, also pure. Older event versions are **upcast** on read, so decide/evolve see only the current shape. | ADR 2, 3, 14, 41 |
| 4 | **Submission** | A command sent with an idempotency key. The same key with the same params is answered with the first answer (replay); with other params it is refused (`key_reused`). A version conflict decides again on fresh state at the same `now`. **Plan** is decide without commit. | ADR 2, 8 |
| 5 | **Process** | Work across streams. Its progress is its own stream (`ProcessStarted`, `StepRecorded`…, `ProcessFinished`, which ends it). `next(input, progress) → Run(command) \| Call(action) \| Queue(task) \| Await(event, within) \| Finish`, pure. Each step's key is `StepKey(process, step) = UUIDv5`, so a step re-run after a crash is replayed, not repeated. A process may start on an event (`starts_on`). One runner advances a process at a time, under a lease. | ADR 6, 44, 48 |
| 6 | **Action** | A call to another system, requested by a process step through an **outbox**: the request event and the outbox row commit together. Delivered at least once with the step's key, or declared **at most once**, in which case an unanswered call is answered `unknown` for `next` to reconcile. Retry schedule (attempts, backoff, factor, max, jitter) is declared per action. | ADR 11, 42, 63 |
| 7 | **Task** | Long work a process queues and a **worker** pulls: claim under a lease, heartbeat to keep it, result or failure as a command. Declared timeout, attempts, backoff; may be cancelled; may carry an **ordering key** (one at a time per key, paced). | ADR 31, 34, 42, 43 |
| 8 | **Schedule** | `CommandScheduled{at, command, params}` returned by a decide; run once its time comes, with `ScheduleKey(event id)`. A **recurring** command runs once per period with `UUIDv5(name@period start)`. | ADR 13, 45 |
| 9 | **Projection** | Code that keeps read-model rows from the log. Reads only **final** events in commit order; rows and checkpoint move in one transaction. A versioned projection rebuilds from the log. An event it fails on becomes a **dead letter**: skipped, or (declared `stop`) held there. | ADR 7, 65 |
| 10 | **Snapshot** | A stream's folded state at a version, kept by the code (build hash) that folded it. Derived: deleting every snapshot changes no answer. | ADR 12 |

`by` (who applies a command) is generic. factum defines the kinds it
originates itself: `Process(stream)`, `Schedule(id)`, `Recurring(name)`,
`Worker(name)`, `Action(name)`. Everything else is the program's own
principal type `P` (`By::Principal(P)`); bizkernel's person/agent/webhook
kinds live in its `P`.

`now` is read once per submission by the engine's clock and passed in.
Nothing in factum reads a clock anywhere else, so tests move time by hand.

## 2. Crates

| Crate | Holds | Depends on | Budget (non-test lines) |
|---|---|---|---|
| `factum-core` | Concepts 1–10 as types and pure functions: event envelope, stream version (checked), key derivations, decide/evolve/next/upcast signatures, step and outcome types, retry/backoff arithmetic, ordering-key pacing, ending rules. No IO, no async, no clock. Checked by Flux. | serde, serde_json | 5,000 |
| `factum-store` | The `Store` trait (§3), the in-memory store (reference implementation, used by tests), and the **conformance suite** every store must pass. | core | 3,000 |
| `factum` | The engine: `submit`, `plan`, `state`, `history`; the loops `resume` (processes), `deliver` (actions), `run_due` (schedules, recurring), `project`; the task claim/heartbeat/result API; the clock and the `Action` trait. Generic over `Store`. | core, store | 5,000 |
| `factum-sqlite` | `Store` on SQLite (one writer per database). | store | 3,000 |
| `factum-postgres` | `Store` on Postgres, extracted from bk-server, many writers. | store | 3,000 |

The budgets are checked by the gate: a crate over its budget is red.
Raising one is a decision recorded here, not a side effect of a change.

## 3. The store contract

The store is the only place that knows the backend. Each guarantee has
exactly one home: the conformance test that states it. A backend is done
when the whole suite passes on it; no backend has tests of its own for
these guarantees.

Operations (shape, not final signatures):

- `read_stream(stream, after_version)` → events and the stream's version;
  `archived` if events the version promises are gone.
- `commit(unit)` → committed position, or `Conflict(stream)`. A unit is:
  events appended to one or more streams, each at the version its decide
  read; the submission's key and answer; the **side rows** the events
  imply (outbox requests, queued tasks, pending schedules, process starts).
  All of it commits, or none of it.
- `answer_for(key)` → the recorded answer of a key, for replay.
- `read_final(after: Position, limit)` → events no later commit can come
  before, in commit order. `Position` is opaque to callers.
- `claim(kind, now, lease, limit)` / `renew` / `complete` for outbox rows,
  tasks and due schedules. A claim moves the row's due time one lease ahead
  in one short statement; no lock is held while the work runs.
- `lease_process(process, holder, until)` / `release_process`.
- `checkpoint(projection)` and `apply_projection(projection, position, row_ops)`
  in one transaction.
- `snapshot(stream, build)` / `keep_snapshot(...)`.

Guarantees, each one conformance test (names after bizkernel's tests that
prove them today):

| Guarantee | Conformance test | bizkernel |
|---|---|---|
| A stream's versions are 1..n with no gap and no duplicate, under concurrent writers | `versions_stay_unique` | `TestPartition_VersionsStayUnique` |
| A writer that read an old version appends nothing and gets `Conflict` | `stale_version_conflicts` | ADR 2 |
| Same key, same params → first answer; other params → `key_reused` | `key_replays_and_refuses_reuse` | ADR 39 |
| Events, key and side rows commit together or not at all | `unit_is_atomic` | ADR 11, 31 |
| A step run but not recorded is replayed, not repeated | `crash_between_step_and_record` | `TestProcess_CrashBetweenStepAndRecord` |
| Two runners advance each step once | `two_runners_each_step_once` | `TestProcess_TwoServersResumeEachStepOnce` |
| A runner that dies loses its lease; another finishes | `lease_expires_when_runner_dies` | `TestProcess_LeaseExpiresWhenItsServerDies` |
| An action delivered twice is done once (same key) | `crash_while_calling` | `TestCheckout_CrashWhileCharging` |
| An at-most-once call with no answer becomes `unknown`, never retried | `at_most_once_ends_unknown` | ADR 42 |
| A late commit with an earlier position is not skipped by a reader | `late_commit_is_not_skipped` | `TestList_LateCommitIsNotSkipped` |
| A schedule found due by two runners runs once | `schedule_runs_once` | ADR 13 |
| Tasks with one ordering key are claimed one at a time, in order | `ordering_key_one_at_a_time` | ADR 43 |
| An ended stream refuses every command | `ended_stream_refuses` | ADR 15 |
| Deleting snapshots changes no answer | `deleting_snapshots_changes_nothing` | `TestSnapshot_DeletingChangesNothing` |
| Rows and checkpoint move together; a rebuild yields the same rows | `projection_exact_and_rebuildable` | `TestListProjection_Rebuild` |

Backend notes, not contract: SQLite has one writer, so commit order is
append order and "final" is simply "committed". Postgres has many writers
and needs bizkernel's appending-transaction horizon (ADR 65) and
`SKIP LOCKED` claims (ADR 64); those stay inside `factum-postgres`.

## 4. Not in factum

Each stays in bizkernel (or the program that needs it), built on factum:

- Identity, sign-in, delegation, tokens (ADR 16, 38, 54, 60, 118).
- Grants and Cedar policies, row-level filters (ADR 17, 23, 46, 57).
- Approval of high-risk commands, risk assessment (ADR 29, 55).
- WebAssembly modules, the ABI, the registry, `describe` (ADR 9, 66, 80).
- HTTP routes, the wire format, `bk` (ADR 5, 91, 93); watch/SSE (ADR 20, 96).
- Files, blobs, secrets, personal-data sealing (ADR 22, 24, 25, 28).
- Webhooks, intakes, connectors (ADR 27, 40, 58, 70, 71).
- Pages, Apps, brand, UI, LiveKit, asks (ADR 19, 26, 115–127).
- Hosted workers and their call-outs (ADR 33, 35, 51, 59, 87, 88).
- Bulk runs, sequences, reference data, rate limits (ADR 47, 49, 52, 99).
- Month partitions and archiving (ADR 15's mechanics): a `factum-postgres`
  extension at most. Ending streams is in factum; moving their events is not.
- Branches (ADR 56): open question 1.
- Read replicas and cells (ADR 64's scaling half).

## 5. Where it is in bizkernel today

Measured at bizkernel `main` on 2026-11-17.

- **Size.** `cli/crates/kernel/src` (pure) 47,374 lines; `cli/crates/server/src`
  (effect) 49,366 lines in 87 files, 32 of which use sqlx. Server tests
  38,639 lines; `tests/kernel.rs` alone 29,355 (523 tests).
- **A store seam already exists.** `EventStore` (`server/src/store.rs:122`),
  `WorkStore` (`server/src/work.rs:179`) and `KernelStore`
  (`server/src/kernel_store.rs:45`), each with a Postgres and an in-memory
  implementation (`server/src/memory.rs` + `memory/`, 3,337 lines), behind
  `enum Store`. `server/src/store_contract.rs` (545 lines) already runs
  scenarios against both: the seed of the conformance suite.
- **But the seam is bizkernel's.** Its signatures carry `Line` (branches),
  `Caller` (actor, on-behalf-of, delegation chain, Cedar attributes), module
  origin facts and policy history; `needs_postgres(` is called 9 times.
- **The pure side is coupled too.** `process.rs` (2,630), `task.rs` (1,293),
  `schedule.rs`, `recurring.rs` import `Actor`/`PersonId`; decide, fold and
  upcast go through `bk_abi` (WASM request types, `abi.rs` 2,976 lines);
  `task.rs` pulls in `bk_route` (the worker wire protocol); event meta is
  built with `delegation::Link`. Clean already: `horizon.rs` (120) and
  `projection.rs` (141).
- **Keys live on the wrong side.** `step_key`, `run_id`, `start_key`,
  `result_key` are in `server/src/kernel/delivery.rs:420-476`; they are pure
  and move to `factum-core`.
- **SQL is one file pair** shared by every query: `kernel/sql/schema.sql`
  (1,473 lines) and `kernel/sql/query.sql` (1,643), read by name through
  `server/src/db.rs`. The read-model code in the pure crate
  (`readmodel.rs`, 3,188 lines) generates Postgres SQL strings.
- **Single-transaction invariants on Postgres:** event + `bk_streams` bump +
  unfinished-process index + schedule note; `ActionRequested` + `bk_outbox`
  row; `TaskQueued` + `bk_tasks` row + `key_seq`; projection rows +
  checkpoint; process-start checkpoint + lease. Each becomes a conformance
  test (§3).

So extraction is mostly **generalizing a seam that exists**, not cutting a
new one. The work is in three places: replacing `Caller`/`Actor` with
factum's generic `By<P>`, deciding branches (open question 1, now the first
decision, since every store signature carries `Line`), and moving read-model
SQL generation behind the store.

## 6. Migration: bizkernel onto factum

bizkernel's own gate and `conformance.sh` are the oracle: every step below
lands with both green, and is a commit that can be reverted alone.

1. Create factum with `factum-core` types: event envelope, `StreamVersion`,
   key derivations (moved from `delivery.rs`), step/outcome, `Rejection`,
   `By<P>`. bk-kernel re-exports them instead of defining them; its `Actor`
   becomes the `P` of `By<P>`.
2. `factum-store`: the trait, generalized from `EventStore`/`WorkStore`/
   `KernelStore` with bizkernel's types taken out of the signatures; the
   in-memory store moved from `server/src/memory*`; the conformance suite
   grown from `store_contract.rs` and the tests named in §3.
3. `factum-postgres`: the matching part of `store.rs`, `work.rs`,
   `schema.sql` and `query.sql` moved out of bk-server behind the trait. It
   passes the conformance suite. bk-server calls it through `Store`.
4. `factum` engine: submit/plan and the loops move from bk-server; bk-server
   keeps what wraps them (grants, approval, modules, HTTP).
5. `factum-sqlite`: written fresh, passes the conformance suite.
6. afkconvoy v2 starts on factum + factum-sqlite.

Steps 1–4 shrink bizkernel by the lines moved; that difference is reported
at each step.

## 7. Open questions

1. **Branches** (ADR 56) fork the log and send a branch's actions to
   stand-ins. Every bizkernel store signature carries `Line`, and Postgres
   has `bk_branch_*` mirrors of the core tables. Either factum has a line
   (main or a branch) as a store concept, or bizkernel rebuilds branches on
   top of factum (e.g. a branch as a separate store instance seeded from
   the main log). Decide before step 2: it shapes every trait signature.
2. **Projection rows.** bizkernel's modules declare tables and the kernel
   writes them (ADR 10), which keeps projection code backend-free; today
   the SQL for them is generated in the pure crate (`readmodel.rs`), for
   Postgres only. factum either adopts declared tables with row ops, each
   store turning them into its own SQL, or hands the projection a backend
   transaction (simpler, but ties projection code to one backend).
   Proposal: declared tables, SQL generation moved into each store.
3. **Sync or async store.** bk-server is async (sqlx); rusqlite is sync.
   Proposal: async trait; SQLite runs on one writer thread.
4. **Process samples** (ADR 94) as a test kit in `factum-store`, or left to
   bizkernel?
5. **Plug-in boundary.** factum calls decide/next as Rust trait methods;
   bizkernel's WASM modules implement those traits by calling into the
   module. Confirm no ABI concept leaks into `factum-core`.
