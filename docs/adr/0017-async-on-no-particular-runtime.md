# 17. Async, on no particular runtime

## Decision

**Async, on no particular runtime.** The engine never spawns or sleeps; a writer that loses a race yields once before deciding again, up to 1000 times (every loss is another writer's win, so the log always moves; the bound keeps one writer from losing forever). The SQLite store keeps its connection on its own thread, so no call blocks the caller's runtime.

## Why

A program picks its own runtime. An engine that spawned or slept would
pick one for it; a blocking SQLite call on the caller's thread would stall
every other task the runtime holds.
