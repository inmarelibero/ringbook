#!/usr/bin/env bash
# Stops the engine service.
exec "$(dirname "$0")/docker_stop.sh" engine
