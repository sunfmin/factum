# 6. A scheduled command runs once and is not retried

## Decision

A decide asks for a command later by returning a `CommandScheduled`; it
runs once its time comes, under `UUIDv5(event, "scheduled")`, and its
outcome, a refusal included, is recorded and not retried.

A **recurring** command runs once per period, under a key derived from its
name and the period; its `RecurringRan`, a refusal or a run with no events
included, marks the period run, so such a run runs again next period, not
next tick.

## Why

A schedule's key is derived from the event that asked for it, and a
period's from the name and the period, so a runner that takes one over
after a crash finds the outcome under that key (ADR 0003) instead of
running it twice. A refusal is an answer too: retrying it every tick
would only refuse again.
