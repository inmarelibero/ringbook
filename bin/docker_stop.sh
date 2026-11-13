#!/usr/bin/env bash
# Stops the Docker stack, or only the services named. Containers and data are
# kept; bin/docker_restart_all.sh brings everything back.
#
#   bin/docker_stop.sh                    # every service
#   bin/docker_stop.sh web                # just this one
#
# Works from any directory.
set -euo pipefail

# shellcheck source=bin/_compose.sh
source "$(dirname "$0")/_compose.sh"

if [ "$#" -eq 0 ]; then
    # Compose stops dependents first, so the engine stops before the venue.
    "${compose[@]}" stop
else
    for service in "$@"; do
        if [ "$service" = mock-venue ]; then
            # A venue that comes back under a running engine halts it with
            # VenueStateLost, so the engine stops with it.
            "${compose[@]}" stop engine mock-venue
        else
            "${compose[@]}" stop "$service"
        fi
    done
fi

"${compose[@]}" ps --all
