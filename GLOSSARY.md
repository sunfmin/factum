# Glossary

factum's words, and only these name its concepts; a new concept is a decision (ADR 0001). What each one promises is in the ADR it names.

- **Event**: a fact: stream, version, stored type name, type version, data (JSON), meta (`now`, `by`, idempotency `key`, `command` and, on the events a submission wrote, its `fingerprint`), an id (UUIDv7) and, once committed, its position in the log (ADR 0002).
- **Log**: every event, in commit order. It has one line: no branches (ADR 0001).
- **Stream**: `<aggregate>/<uuid>`: the events of one thing, numbered 1, 2, 3… with no gap (ADR 0002).
- **Ending event**: an event type an aggregate declares to end its streams (ADR 0002).
- **Command**: a named request to change one stream. Its rules are `decide(state, params, by, now) → events | Rejection`, pure; it may also schedule commands.
- **Evolve**: the pure fold of a stream's events into its state.
- **Upcast**: an older event shape turned into the current one on read (ADR 0002).
- **Submission**: a command sent under an idempotency key (ADR 0003).
- **Answer** (of a key): what the events written under that key say (ADR 0003).
- **Fingerprint**: a submission's command and params, in the meta of the events it wrote.
- **Plan**: decide without commit.
- **Process**: work across streams. Its progress is its own stream, `process/<id>`; `next(input, progress) → Run(command) | Call(action) | Queue(task) | Await(event, within) | Finish`, pure (ADR 0004).
- **Step**: one answer of a process's `next`, run under the key `UUIDv5(process, step)`.
- **Recorded**: an event as a process sees it: `id`, `stream`, `type`, `data`.
- **Action**: a call to another system, requested by a step through an outbox (ADR 0005).
- **Outbox**: an action's request and its delivery row, committed together.
- **Unknown**: how a call ends when it may have done its work but no answer says so (ADR 0005).
- **Task**: long work a step queues and a worker pulls (ADR 0005).
- **Worker**: a program's code that claims tasks, heartbeats and finishes them.
- **Ordering key**: what a task may carry to run one at a time per key, in order, paced.
- **Schedule**: a command a decide asks for later, by returning a `CommandScheduled` (ADR 0006).
- **Recurring command**: a command run once per period, under a key derived from its name and the period (ADR 0006).
- **Projection**: pure code that turns each event into row changes of the tables it declares (ADR 0007).
- **Current** (projection): built by the code running now: same version, same tables (ADR 0008).
- **Query**: a read of a projection's rows, filtered and ordered by declared columns (ADR 0007).
- **Dead letter**: an event a projection, or the dispatch checkpoint `factum.dispatch`, failed on (ADR 0007, ADR 0009).
- **Snapshot**: a stream's state at a version, a derived shortcut for a fold (ADR 0002).
- **Sample**: a run of a process as some version left it, and what `next` answered for it then (ADR 0019).
- **Work row**: one pending thing (a process to advance, an action to deliver, a task to hand out, a schedule to run, a wait to end), in one table beside the log (ADR 0004).
- **Lease**: a claim on a work row, held until it runs out.
- **Tick**: one pass of every loop: dispatch, advance, deliver, run due, end waits, project. `settle` ticks until nothing is left to do.
- **By**: who applies a command: factum's own runners, or `Principal(text)` (ADR 0018).
- **Now**: the time a submission or a tick reads once and passes down (ADR 0018).
- **Store**: the only code that knows the backend (ADR 0010).
- **Scenario**: one guarantee, driven through the engine's public API on every store (ADR 0014).
- **Flaky**: the wrapper a scenario puts around a store to make a runner die or lose a race (ADR 0014).
