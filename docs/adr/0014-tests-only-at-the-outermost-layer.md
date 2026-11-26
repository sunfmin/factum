# 14. Tests only at the outermost layer

## Decision

Every test drives the engine's public API, as a program would: submit,
start, tick, claim, read. No store, pure function or engine internal has
tests of its own. `factum-conformance` holds one scenario per guarantee,
over a small shop domain (orders, counters, checkout with a payment
stand-in, receipts, packing, deliveries, pickups, an order list), and
`factum_conformance::suite!(backend)` runs all of them on a store.

Scenarios run on a multi-threaded runtime; those about runners in parallel
spawn them as tasks of their own. To make a runner die between two writes,
or lose a race, a scenario wraps its store in `Flaky`: the next matching
commit fails, the next call of a cued store method fails (a broken
store), or another runner (or projector) writes first. Time moves only
when a scenario moves it.

Each scenario is held to account by mutation: breaking the rule it guards
must make it fail.

The one test beside a store is of its file format (ADR 0012).

## Evidence

Measured 2026-11-17 on 46 scenarios, by mutating the rules and running the
suite on each: 19 of 19 mutations of the engine, core and in-memory
store, and 13 of 14 of the SQLite store, are caught. The one not caught (a
snapshot replaced by an older one) changes no answer, only how long a fold
takes. The SQLite suite passed 30 runs of 30 in a row.

## Why

A test of an internal pins its shape, and a refactor breaks it with no
guarantee broken. A test through the public API breaks only when a
promise does, and the same suite holds every store to the same promises.
