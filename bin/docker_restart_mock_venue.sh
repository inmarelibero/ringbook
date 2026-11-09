#!/usr/bin/env bash
# Restarts the mock-venue service.
exec "$(dirname "$0")/docker_restart.sh" mock-venue
