#!/usr/bin/env bash
# Stops the prometheus service.
exec "$(dirname "$0")/docker_stop.sh" prometheus
