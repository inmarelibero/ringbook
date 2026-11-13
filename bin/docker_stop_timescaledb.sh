#!/usr/bin/env bash
# Stops the timescaledb service.
exec "$(dirname "$0")/docker_stop.sh" timescaledb
