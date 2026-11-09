#!/usr/bin/env bash
# Restarts the grafana service.
exec "$(dirname "$0")/docker_restart.sh" grafana
