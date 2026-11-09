#!/usr/bin/env bash
# Restarts the prometheus service.
exec "$(dirname "$0")/docker_restart.sh" prometheus
