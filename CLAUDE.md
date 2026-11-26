# Working on factum

- factum's words are `GLOSSARY.md`'s, and its decisions and promises are
  `docs/adr/`'s, one decision per ADR. A change that adds a concept, a
  crate or a store guarantee adds or changes an ADR in the same commit (a
  new word in the glossary too), and a new concept needs the owner's
  approval first (ADR 0001). A promise an ADR makes is registered in
  `truefit.toml`; output that contradicts an ADR says so, naming it.
- Tests live at the outermost layer only: a scenario in
  `crates/conformance/src/scenarios.rs`, driven through the engine's public
  API and registered in `suite!` (`crates/conformance/src/lib.rs`). Do not
  add unit tests to a store, the engine or `factum-core`. A guarantee that
  needs a crash or a race uses `Flaky`.
- A new scenario must fail when the rule it guards is broken: break the
  rule, see it fail, restore.
- `./gate.sh` must pass before every commit: line budgets, `cargo fmt`,
  clippy with warnings denied, every scenario on every store (`slimfit check`).
- Key derivations in `crates/core/src/keys.rs` are bizkernel's byte for
  byte; changing one breaks the keys already in logs.
- Each change follows `slimfit next`'s loop and the rule for its mutants
  in `slimfit help factorium` (run `slimfit help` for the rest).
