# 1. factum is a library with a closed set of concepts

## Context

factum is a minimal event-sourced core: an append-only log of facts, the
pure functions that decide what to record, and the few kinds of work that
outlive one command (processes, actions, tasks, schedules, waits,
projections). It is extracted from bizkernel, which will depend on it, and
it is the foundation afkconvoy v2 and later programs are built on.

## Decision

factum is a library, not a server. It has no HTTP, no identity, no policy
language, no plug-in format and no UI. A program built on it adds those or
does without.

Its concepts are the words of `GLOSSARY.md`, and no others. Adding one
needs the owner's approval, an entry in the glossary and an ADR.

Not in factum: identity and sign-in; grants and policies; approval of risky
commands; plug-in modules and their ABI; HTTP and wire formats; live
watches; files, blobs and secrets; personal-data sealing; webhooks and
intakes; pages, apps and UI; hosted workers; bulk runs, sequences,
reference data, rate limits; archiving old months of the log; branches;
read replicas.

**Not supported: branches.** The log has one line. A program that wants to
rehearse on a copy of the log builds it on top of factum (e.g. a second
store seeded from the first).

## Consequences

Every concept a program asks for beyond these is built on top of factum,
or is a new decision here. bizkernel's branches are such a case (ADR 0020).
