# 18. `By::Principal` is text, and `now` is read once

## Decision

`by` (who applies a command) is factum's own for the work it originates —
`Process(stream)`, `Schedule(id)`, `Recurring(name)`, `Action(name)`,
`Worker(name)` — and the program's for everything else:
`Principal(text)`, as the program names its people, agents and systems.

`now` is read once per submission and once per tick and passed down;
a worker's call reads it once, and so does each action call's renewal of
its lease. Nothing in factum reads a clock anywhere else, so tests move
time by hand.

## Why

**`By::Principal` is text**, not a type parameter: every engine type would
otherwise carry the program's principal type, and bizkernel already
records its actors as text.

One read of the clock per submission or tick keeps decide pure and lets a
scenario move time only when it means to (ADR 0014).
