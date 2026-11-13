#!/usr/bin/env bash
# Stops the grafana service.
exec "$(dirname "$0")/docker_stop.sh" grafana
