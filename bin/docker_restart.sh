#!/usr/bin/env bash
# Restarts the Docker stack, or only the services named.
#
#   bin/docker_restart.sh                 # every service
#   bin/docker_restart.sh web engine      # just these
#
# Works from any directory.
set -euo pipefail

# shellcheck source=bin/_compose.sh
source "$(dirname "$0")/_compose.sh"

if [ "$#" -eq 0 ]; then
    # Stopping and starting, rather than restarting, keeps the dependency
    # order: the engine stops before the mock venue and starts after it.
    "${compose[@]}" stop
    "${compose[@]}" start
else
    for service in "$@"; do
        if [ "$service" = mock-venue ]; then
            # A venue restart under a running engine halts it with
            # VenueStateLost, so the engine stops first and starts a new run.
            "${compose[@]}" stop engine
            "${compose[@]}" restart mock-venue
            "${compose[@]}" start engine
        else
            "${compose[@]}" restart "$service"
        fi
    done
fi

"${compose[@]}" ps
