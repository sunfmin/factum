# 7. Projection tables are declared

## Decision

A projection is pure code that turns each event into row changes (put,
merge, delete) of the tables it **declares**: rows are JSON objects under a
key; declared columns are what **queries** filter (`= <> < <= > >=`) and
order by, and declared indexes make them fast. It reads only **final** events in commit order; rows and checkpoint move together.

**Projection tables are declared**, and a change to them rebuilds the
projection as a new version does: no one has to remember to bump the
version. Queries take only declared columns, so every store can answer them with an index.

**Values compare as SQLite compares them**, in every store: null before numbers before text; numbers by value; booleans as 0 and 1; objects and arrays as their JSON text; a comparison with null never holds. Results are ordered as asked, then by key.

A dead letter (ADR 0008) of a current projection is
**retried** with the code running now: its rows and its removal are one
batch, like a catch-up, and a projection stopped at it then goes on.

**A dead letter is retried in place** with the code running now; a
rebuild still clears them all.

## Why

A query on an undeclared column could not use an index in every store,
and a table changed without a new version would keep rows the old code
wrote. Comparing as SQLite does gives every store one order to match.
