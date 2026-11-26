# 3. A submission's answer is its events

## Decision

A submission is a command sent under an idempotency key.

- The same key with the same command and params answers the first answer
  that wrote events; with others it is refused (`key_reused`).
- The answer of a key is what its events say, and nothing else: the events
  whose meta carries the key and a fingerprint (command and params);
  nothing is kept per key beside the log.
- An event the engine writes under a command's key as a runner's outcome
  (`ScheduleRan`, `RecurringRan`) carries no fingerprint and is no part of
  the answer.
- A refusal, or an applied answer with no events, like a fault (code that
  cannot read its state or write an event), leaves the key with no answer:
  it wrote nothing, so a re-send decides again on fresh state at its own
  `now`, and can apply nothing twice.
- factum's own runners commit their outcome as an event in the same commit
  as the command's events, or alone for a refusal or a run with no events:
  a step's `StepRecorded` in `process/<id>`, a schedule's `ScheduleRan`, a
  recurring period's `RecurringRan` in `recurring/<key>`.
- A runner that takes one over after a crash finds that outcome and does
  not decide again, or finds nothing of the run and decides it anew.
- A writer that read an old version decides again on fresh state at the
  same `now`.

**Plan** is decide without commit.

## Why

With nothing kept per key beside the log, an answer cannot disagree with
the events that make it, and a store holds no second table to keep in
step with them.
