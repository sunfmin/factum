# 19. Samples hold a new `next` to old runs

## Decision

A sample is a run of a process as some version left it — input, progress
— and what `next` answered for it then. Taken from a running process, kept
beside the code, and checked against a new version: a `next` that answers
otherwise would make old runs take a step twice or wait forever.

**Samples are taken from running processes** and checked by the model,
without a store: holding a new `next` to old progress is a pure check.

## Why

A process outlives the code that started it. Its progress in the log was
written by the old `next`; a new `next` must answer that progress as the
old one did, and since `next` is pure, checking it needs no store.
