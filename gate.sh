#!/bin/sh
# The checks every change passes. Fast enough to run before each commit.
set -eu
cd "$(dirname "$0")"

# Each crate's source stays within its budget (ADR 0016).
# Raising one is a decision recorded there, not a side effect.
budget() {
  lines=$(find "crates/$1/src" -name '*.rs' -exec cat {} + | wc -l | tr -d ' ')
  printf '%-12s %5s / %5s lines\n' "$1" "$lines" "$2"
  if [ "$lines" -gt "$2" ]; then
    echo "gate: crates/$1 is over its budget of $2 lines" >&2
    exit 1
  fi
}
budget core 5000
budget store 3000
budget engine 5000
budget sqlite 3000

# Each Markdown file in its one home, within its budget (docfit.toml,
# measured against .docfit/baseline.json); no file cites the local tracker anew.
docfit check
cargo fmt --all --check
cargo clippy --workspace --all-targets --quiet -- -D warnings
# Every scenario on every store, measured against .slimfit/baseline.json.
slimfit check
# The seeded simulation of the engine (truefit.toml): kept failing seeds
# first, then 100 new ones, ~35s. A seed that fails is kept in .truefit/seeds/.
truefit sim
# The model of exactly-once and unknown (models/ExactlyOnce.tla), checked by
# TLC within its bounds (~2s), and the simulation's runs as its traces:
# seeds 1-10 and every kept seed (~25s).
truefit model
models/traces.sh
