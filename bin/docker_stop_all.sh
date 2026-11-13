#!/usr/bin/env bash
# Stops every service in the Docker stack.
exec "$(dirname "$0")/docker_stop.sh"
