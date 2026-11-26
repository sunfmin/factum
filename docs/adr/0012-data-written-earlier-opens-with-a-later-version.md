# 12. Data written by an earlier version opens with a later one

## Decision

A store knows the schema version it finds and brings it up to its own when
it is opened, one step per schema change, all in one transaction; a schema
newer than it knows is refused at open (`NewerSchema`), before any call.
A released step is never edited: a change to the schema is a new step.

A step never changes what an event says. Once, it added to events' meta
what the store already held of them elsewhere: SQLite's step 6 wrote each
kept `{fingerprint, stream}` row's fingerprint into the meta of the key's
events in that stream, then dropped the row. A step may drop what no
answer needs any more: step 7 dropped the `answers` table, whose rows were
runners' refusals and runs with no events, now events of their own
(ADR 0003).

A store's file format is the one thing the engine's API cannot reach, so it
is tested beside the store: `crates/sqlite/tests/conformance.rs` starts
every scenario's database from the schema as an earlier release wrote it
(copied, not derived from the code) and holds its migrated shape to a new
database's, so a schema change without a step fails.

## Why

A program upgrades factum under a log it already has. A step edited after
release would leave files that ran the old one in a shape no code expects;
a newer schema opened by older code would be written in a shape it does
not know.
