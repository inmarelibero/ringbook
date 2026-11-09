#!/bin/sh
# Starts the engine, recovering if the previous run didn't stop cleanly.
# A clean stop archives the journal, so a restart is what rotates it.
set -eu

journal=/data/current.rbj
mkdir -p /data/archive

if [ -f "$journal" ]; then
    recover="--recover"
else
    recover=""
fi

# shellcheck disable=SC2086 # recover and the core options are optional words
ringbook live \
    --config "${RINGBOOK_CONFIG:-/etc/ringbook/demo.toml}" \
    --journal "$journal" \
    --venue "${RINGBOOK_VENUE:-mock-venue:9100}" \
    --api 0.0.0.0:8080 \
    --metrics 0.0.0.0:9000 \
    --ctl-socket /run/ringbook/ctl.sock \
    --behind-proxy \
    ${RINGBOOK_CORE_OPTIONS:-} \
    $recover &
engine=$!

trap 'kill -TERM "$engine"' TERM INT
wait "$engine" || status=$?
status=${status:-0}
if [ "$status" -ge 128 ]; then
    # The first wait was interrupted by the signal; wait for the real exit.
    wait "$engine" && status=0 || status=$?
fi

if [ "$status" -eq 0 ] && [ -f "$journal" ]; then
    mv "$journal" "/data/archive/$(date -u +%Y%m%dT%H%M%SZ).rbj"
fi
exit "$status"
