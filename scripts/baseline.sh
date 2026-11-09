#!/usr/bin/env bash
# Reproduces the baseline benchmark numbers from a local recording.
#
#   scripts/baseline.sh RECORDING_DIRECTORY [HOT_CORE]
set -euo pipefail

recording="${1:?usage: baseline.sh RECORDING_DIRECTORY [HOT_CORE]}"
hot_core="${2:-}"
config="${CONFIG:-configs/demo.toml}"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

pin=()
if [ -n "$hot_core" ]; then
    pin=(--hot-core "$hot_core")
fi

echo "== environment"
uname -srm
echo "cpus: $(getconf _NPROCESSORS_ONLN)"
echo "pinning: ${hot_core:-none}"
echo "recording: $recording"
du -sh "$recording" | cut -f1
cat "$recording"/*.rbr | shasum -a 256 | cut -c1-16

echo
echo "== zero allocations on the hot path"
cargo test --release -p ringbook-ring -p ringbook-book -p ringbook-engine --test no_alloc 2>&1 |
    grep -E '^test |test result'

echo
echo "== microbenchmarks"
cargo bench -p ringbook-ring -p ringbook-book 2>&1 | grep -E '^[a-z0-9_/]+( +time:.*)?$|  time:'

cargo build --release -p ringbook-app >/dev/null 2>&1

echo
echo "== throughput (max-speed replay)"
target/release/ringbook replay --config "$config" --recording "$recording" \
    --journal "$work/max.rbj" ${pin[@]+"${pin[@]}"} | grep -E 'events|throughput|batches'

echo
echo "== latency (paced replay)"
target/release/ringbook replay --config "$config" --recording "$recording" --pacing recorded \
    --journal "$work/paced.rbj" ${pin[@]+"${pin[@]}"} | grep -E 'tick-to-decision|batch latency'

echo
echo "== determinism"
cmp "$work/max.rbj" "$work/paced.rbj" && echo "journals are byte-identical"
