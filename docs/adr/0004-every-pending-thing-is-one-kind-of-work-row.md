# 4. Every pending thing is one kind of work row

## Decision

Everything pending — a process to advance, an action to deliver, a task to
hand out, a schedule to run, a wait to end — is one row kind in one table
beside the log. Every row is implied by an event committed with it (one pure function, `effects`), so the rows never say what the log does not. Every lease in factum is a claim on such a row.

A process is work across streams. Its progress is its own stream,
`process/<id>` (`ProcessStarted`, `StepRequested`, `StepRecorded`,
`ProcessFinished`, which ends it), and its `next` is pure. A step's key is
`UUIDv5(process, step)`, so a step re-run after a crash is replayed, not
repeated.

A process may start by itself on an event; its input is that event as
`Recorded` (`id`, `stream`, `type`, `data`), the shape a `Run` or `Await`
step's outcome gives events in too. One runner advances a process at a
time, under a lease.

## Why

One claim, one lease and one wake rule serve processes, actions, tasks,
schedules and waits, and every store implements them once.
