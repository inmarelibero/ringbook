#!/usr/bin/env bash
# Restarts the engine service.
exec "$(dirname "$0")/docker_restart.sh" engine
