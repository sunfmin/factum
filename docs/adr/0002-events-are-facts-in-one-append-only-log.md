# 2. Events are facts in one append-only log

## Decision

An event is a fact, written once. Never edited. It carries its stream, its
version, its stored type name and type version, its data, its meta and an
id (`GLOSSARY.md`, Event); once committed it has a position in the log.

A stream is the events of one thing, numbered 1, 2, 3… with no gap. An aggregate may declare **ending** event types; a stream whose last domain event ends it refuses every later command (`ended`); the engine's own `CommandScheduled`, appended after a command's domain events, is not a domain event and is skipped.

A command's rules are pure: `decide` answers events or a rejection. State is folded from the stream by a pure `evolve`. Older event shapes are **upcast** on read, so decide, evolve and projections see only the current shape.

A stream's state at a version, kept by the code that folded it and read only by that code. Derived: no answer depends on it. That is a snapshot: a
shortcut for a fold, never a second home of what the stream says.

## Why

A log that is only appended to is the one home of what happened: every
state, row and answer is derived from it, so none can say what it does
not.
