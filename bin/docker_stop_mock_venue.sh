#!/usr/bin/env bash
# Stops the mock-venue service.
exec "$(dirname "$0")/docker_stop.sh" mock-venue
