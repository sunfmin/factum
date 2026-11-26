# 11. The store contract for projections and snapshots

## Decision

- `snapshot` / `keep_snapshot`; `checkpoint` / `reset_projection` /
  `apply_projection`; `query`, `rows`, `row`, `dead_letters`. A batch is
  one move of a projection — rows, new dead letters, retried dead letters
  removed, checkpoint — all of it or none: `Stale` unless the checkpoint is
  where the batch began and every dead letter it removes is there,
  `Undeclared` if a row op names a table the projection was not built
  with.
- A checkpoint keeps the tables it was built with; `reset_projection`
  drops a projection's rows and dead letters and makes its declared tables
  anew.
- Whether a projection is current, and where a retry moves a stopped one,
  the engine decides, not the store.

A row a store cannot decode is an error, never a guessed value.

## Why

A batch that moved rows without its checkpoint, or the checkpoint without
its rows, would apply an event twice or never. Keeping currency in the
engine (ADR 0008) leaves the store one rule for every backend.
