# Working on factum

- `SPEC.md` is the one home of what factum is: its concepts, crates,
  budgets, store contract and what it leaves out. A change that adds a
  concept, a crate or a store guarantee changes `SPEC.md` in the same
  commit, and a new concept needs the owner's approval first.
- Tests live at the outermost layer only: a scenario in
  `crates/conformance/src/scenarios.rs`, driven through the engine's public
  API and registered in `suite!` (`crates/conformance/src/lib.rs`). Do not
  add unit tests to a store, the engine or `factum-core`. A guarantee that
  needs a crash or a race uses `Flaky`.
- A new scenario must fail when the rule it guards is broken: break the
  rule, see it fail, restore.
- `./gate.sh` must pass before every commit: line budgets, `cargo fmt`,
  clippy with warnings denied, every scenario on every store.
- Key derivations in `crates/core/src/keys.rs` are bizkernel's byte for
  byte; changing one breaks the keys already in logs.

## Agent skills

### Issue tracker

Issues live in factorium's local tracker (`factorium issue … factum`). See `docs/agents/issue-tracker.md`.

### Domain docs

single-context, with `SPEC.md` as the glossary and the record of decisions. See `docs/agents/domain.md`.
