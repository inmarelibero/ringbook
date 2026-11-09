#!/usr/bin/env bash
# Restarts the web service.
exec "$(dirname "$0")/docker_restart.sh" web
