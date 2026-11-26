# 10. The store contract for the log and the work

## Decision

The store is the only place that knows the backend. Every method is one
short transaction; none holds a lock while the caller works.

- `read_stream(stream, after)`; `read_log(after, limit)` — only final
  events, in commit order: no later commit lands before the last returned.
  An `after` past the stream's end reads nothing.
- `commit(unit)` — events appended to streams at the versions their writer
  read, the submission's key (which must have no answer yet), the work
  changes the events imply, and lease guards. All of it or none:
  `Conflict`, `KeyTaken` or `LeaseLost` otherwise.
- `answer(key)` — read from the log alone: the events whose meta carries
  the key and a fingerprint, as committed (with positions), found by an
  index on the key; the fingerprint is theirs. A store keeps nothing per
  key beside the log (ADR 0003).
- `claim(kind, names, now, holder, lease, limit)` — due rows no live lease
  holds, oldest due first; of rows sharing a name and ordering key only the
  first put, and not before its pace. A claim takes a new lease, counts an
  attempt, and says whether it took the row from a lease that ran out; the
  row says so too, for as long as that claim holds it.
- `renew(...)` — only a lease that has not run out; `work(kind, id)`;
  `work_named(kind, name)`.
- A wake that comes while a row is claimed survives the claim's release.
- What a claim, a wake and a release do to a row is the store crate's
  (`claim_row`, `wake_row`, `release_row`), one for every store; which rows
  a claim may take is each store's own query.

Each store implements these as its backend allows: SQLite's single writer
makes commit order the sequence order, and Postgres will need an
appending-transaction horizon and `SKIP LOCKED` claims. Those details stay
inside each store.

## Why

Behind this one contract, the engine and its scenarios are the same on
every backend; the in-memory store is the reference every other store is
held to.
