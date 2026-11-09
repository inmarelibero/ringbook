#!/usr/bin/env bash
# Restarts every service in the Docker stack.
exec "$(dirname "$0")/docker_restart.sh"
