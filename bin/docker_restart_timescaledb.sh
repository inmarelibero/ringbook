#!/usr/bin/env bash
# Restarts the timescaledb service.
exec "$(dirname "$0")/docker_restart.sh" timescaledb
