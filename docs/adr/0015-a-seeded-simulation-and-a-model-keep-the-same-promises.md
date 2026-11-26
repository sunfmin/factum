# 15. A seeded simulation and a model keep the same promises

## Decision

Beside the scenarios, a **seeded simulation** (`factum_conformance::sim`,
run by `truefit sim` from `truefit.toml`, in the gate) drives the same
public API on the in-memory store with what no scenario lines up by hand.

From one seed: orders placed, ticks of three runners interleaved store
write by store write (a runner's step is one commit, one claim that took
rows, or one call to another system), clock moves, a runner stopping
between any two store calls of a tick, a store write failing, and calls to
another system answering at once, "try later" (never for a key it already
did: it answers that again), late (after a takeover answered too) or
never.

After every step it checks that no order is charged twice or paid twice, an at-most-once call is made at most once, no step is recorded done whose call was not made nor failed whose call was made, and every unfinished process keeps a work row; after settling, every process has finished.

A seed reruns step for step; a failing one is kept in truefit's seeds, under .truefit/seeds.

The same promises, by the same names, are the invariants of a **model**
(`models/ExactlyOnce.tla`, TLA+): one action per store write and per call,
time left out so any lease may run out at any step, a runner may stop
between any two steps. `truefit model` has TLC check it in the gate, every
order within its bounds (one order, attempts as declared).

Each of the simulation's steps prints the state the model has, so a run is
checked as a behaviour of the model (`truefit model --trace`;
`models/traces.sh`, in the gate, does seeds 1-10 and every kept seed): a
run the model does not allow names the step where the code and the design
part.

## Why

A scenario lines up one order of events by hand. The simulation tries
many orders of the code; the model tries every order of the design; the
traces tie the two.
