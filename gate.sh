#!/bin/sh
# The checks every change passes. Fast enough to run before each commit.
set -eu
cd "$(dirname "$0")"

# Each crate's source stays within its budget (SPEC.md, "Crates").
# Raising one is a decision recorded in SPEC.md, not a side effect.
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

cargo fmt --all --check
cargo clippy --workspace --all-targets --quiet -- -D warnings
# Every scenario on every store, measured against .slimfit/baseline.json.
slimfit check
