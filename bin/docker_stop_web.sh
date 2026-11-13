#!/usr/bin/env bash
# Stops the web service.
exec "$(dirname "$0")/docker_stop.sh" web
