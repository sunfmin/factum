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
budget sealed 2500

# factum-sealed builds with no C compiler, for every target (ADR 0021):
# nothing in its tree is `cc`, and it builds for Windows with none.
if ! cargo tree -q -p factum-sealed -e build,normal --target all -i cc 2>&1 |
  grep -q 'did not match any packages'; then
  echo "gate: factum-sealed's tree has cc (cargo tree -p factum-sealed -i cc)" >&2
  exit 1
fi

# Each Markdown file in its one home, within its budget (docfit.toml,
# measured against .docfit/baseline.json); no file cites the local tracker anew.
docfit check
cargo fmt --all --check
cargo clippy --workspace --all-targets --quiet -- -D warnings
CC=false cargo clippy -p factum-sealed --target x86_64-pc-windows-msvc --quiet -- -D warnings
# Every scenario on every store, measured against .slimfit/baseline.json.
slimfit check
# Each stand-in answers every case of its contract (truefit.toml).
truefit contract
# The seeded simulation of the engine (truefit.toml): kept failing seeds
# first, then 100 new ones, ~35s. A seed that fails is kept in .truefit/seeds/.
truefit sim
# The model of exactly-once and unknown (models/ExactlyOnce.tla), checked by
# TLC within its bounds (~2s), and the simulation's runs as its traces:
# seeds 1-10 and every kept seed (~25s).
truefit model
models/traces.sh
