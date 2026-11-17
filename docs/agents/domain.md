# Domain Docs

How the engineering skills consume this repo's domain documentation.

## Before exploring, read this

- **`SPEC.md`** at the repo root. It is the one home of what factum is: its
  concepts, crates, budgets, store contract, the decisions behind them, and
  what factum leaves out. It plays the part of both `GLOSSARY.md` and
  `docs/adr/`; this repo has neither, and does not create them.

## Use SPEC.md's vocabulary

When your output names a concept (an issue title, a refactor proposal, a
test name), use the term as `SPEC.md` defines it. A concept SPEC.md lacks is
either invented language (reconsider) or a real gap: adding a concept
changes `SPEC.md` in the same commit and needs the owner's approval first
(`CLAUDE.md`).

## Flag SPEC conflicts

If your output contradicts a decision in `SPEC.md`, say so explicitly rather
than silently overriding:

> _Contradicts SPEC.md "Store contract", but worth reopening because…_
