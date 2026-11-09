#!/usr/bin/env bash
# Reproduces the baseline benchmark numbers from a local recording.
#
#   scripts/baseline.sh RECORDING_DIRECTORY [HOT_CORE]
#
# Set BENCH_JSON to a path to also write the headline numbers as JSON, for
# example frontend/public/benchmarks.json for the demo site.
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
recording_hash=$(cat "$recording"/*.rbr | shasum -a 256 | cut -c1-16)
echo "$recording_hash"

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
throughput=$(target/release/ringbook replay --config "$config" --recording "$recording" \
    --journal "$work/max.rbj" ${pin[@]+"${pin[@]}"} | grep -E 'events|throughput|batches')
echo "$throughput"

echo
echo "== latency (paced replay)"
latency=$(target/release/ringbook replay --config "$config" --recording "$recording" \
    --pacing recorded --journal "$work/paced.rbj" ${pin[@]+"${pin[@]}"} |
    grep -E 'tick-to-decision|batch latency')
echo "$latency"

echo
echo "== determinism"
cmp "$work/max.rbj" "$work/paced.rbj"
echo "journals are byte-identical"

if [ -n "${BENCH_JSON:-}" ]; then
    decision=$(printf '%s\n' "$latency" | grep '^tick-to-decision')
    # Reads a number like "p99=    1234ns" from the tick-to-decision line.
    field() {
        printf '%s\n' "$decision" | grep -oE "$1= *[0-9]+" | grep -oE '[0-9]+$'
    }
    events_per_sec=$(printf '%s\n' "$throughput" | awk '$1 == "throughput" { print $2 }')
    cat >"$BENCH_JSON" <<JSON
{
  "measured_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "environment": {
    "kernel": "$(uname -srm)",
    "cpus": $(getconf _NPROCESSORS_ONLN),
    "pinning": "${hot_core:-none}",
    "recording_hash": "$recording_hash"
  },
  "hot_path_allocations": 0,
  "throughput_events_per_sec": $events_per_sec,
  "tick_to_decision": {
    "p50_ns": $(field p50),
    "p99_ns": $(field p99),
    "p999_ns": $(field 'p99\.9'),
    "max_ns": $(field max)
  },
  "replays_identical": true
}
JSON
    echo
    echo "wrote $BENCH_JSON"
fi
