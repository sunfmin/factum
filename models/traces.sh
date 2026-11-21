#!/bin/sh
# Checks the simulation's runs as traces of models/ExactlyOnce.tla
# (`truefit model --trace`): seeds 1..N (default 10), and every seed kept in
# .truefit/seeds/, each run by the code as it is now. A run the model does
# not allow is printed with the record it disallows and the one before.
set -eu
cd "$(dirname "$0")/.."
cargo build -q -p factum-conformance --bin sim
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
kept=$(ls .truefit/seeds 2>/dev/null | sed -n 's/\.json$//p')
for seed in $(seq 1 "${1:-10}") $kept; do
  target/debug/sim "$seed" > "$dir/run.jsonl"
  if ! truefit model --trace "$dir/run.jsonl" > "$dir/trace.json"; then
    echo "traces: seed $seed's run is not a behaviour of the model:" >&2
    cat "$dir/trace.json" >&2
    echo "rerun: target/debug/sim $seed > run.jsonl && truefit model --trace run.jsonl" >&2
    exit 1
  fi
done
echo "traces: $(seq 1 "${1:-10}" | wc -l | tr -d ' ') seeds and $(echo $kept | wc -w | tr -d ' ') kept, each allowed by the model"
