# 13. A SQLite file stays near the size of what it holds

## Decision

SQLite's file uses incremental `auto_vacuum`: a new file from its first
table; a file written before incremental `auto_vacuum` was turned on, by
one `VACUUM` at open, before the store serves anyone, which says on stderr
how long it took. An open whose `VACUUM` fails fails.

Then each write, right after its commit, gives back up to 1,024 free pages
(a schema step's deletes, or rows gone) in a write of its own. The WAL is
cut back to 64 MiB after a checkpoint (`journal_size_limit`).

## Evidence

On factorium's host, the one `VACUUM` at open took 0.78s and took the file
from 389 MB to 40.5 MB; a fresh `VACUUM INTO` of the same data was 40.7 MB.
The open prints the time it took on stderr (`auto_vacuum made incremental
by VACUUM in …s`). A write that gives back 1,024 pages took 28 ms on that
host, far inside the 10 s another writer waits for the lock, and 20 µs
with none free.

## Why

Without it, the file kept every page a step's deletes or gone rows freed:
the host's file was ten times what it held. Giving pages back in small
writes of their own keeps any one write short for the writers waiting.
