#!/usr/bin/env bash
# Enforces the crate boundaries described in SPECS.md.
set -euo pipefail

failed=0

forbid() {
    local crate="$1" pattern="$2" reason="$3" matches
    matches=$(cargo tree -p "$crate" -e normal --prefix none | grep -E "$pattern" || true)
    if [ -n "$matches" ]; then
        echo "error: $crate $reason:" >&2
        echo "$matches" | sort -u >&2
        failed=1
    fi
}

network='^(tokio|axum|hyper|reqwest|tokio-tungstenite|sqlx|mio|socket2) '
venues='^ringbook-venue-'

for crate in ringbook-core ringbook-ring ringbook-book ringbook-engine ringbook-paper-exchange; do
    forbid "$crate" "$network" "is a hot-path crate and must not use networking or a database"
    forbid "$crate" "$venues" "is a hot-path crate and must not know about venues"
done

if cargo tree -p ringbook-core -e normal --prefix none | tail -n +2 | grep -q '^ringbook-'; then
    echo "error: ringbook-core must not depend on another workspace crate" >&2
    failed=1
fi

forbid ringbook-venue-binance '^ringbook-venue-hyperliquid ' "must not depend on another adapter"
forbid ringbook-venue-hyperliquid '^ringbook-venue-binance ' "must not depend on another adapter"

for crate in ringbook-feeds ringbook-replay ringbook-gateway ringbook-telemetry ringbook-api ringbook-ctl; do
    forbid "$crate" "$venues" "must not know the concrete venues; only ringbook-app does"
done

exit "$failed"
