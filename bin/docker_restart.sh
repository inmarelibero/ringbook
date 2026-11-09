#!/usr/bin/env bash
# Restarts the Docker stack, or only the services named.
#
#   bin/docker_restart.sh                 # every service
#   bin/docker_restart.sh web engine      # just these
#
# Works from any directory. The VPS compose file is added when infra/.env
# sets SITE_ADDRESS, so the same script serves a laptop and the server.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

compose=(docker compose)
if ! docker info >/dev/null 2>&1; then
    compose=(sudo docker compose)
fi
if [ -f infra/.env ]; then
    compose+=(--env-file infra/.env)
fi
compose+=(-f infra/docker-compose.yml)
if [ -f infra/.env ] && grep -q '^SITE_ADDRESS=' infra/.env; then
    compose+=(-f infra/docker-compose.vps.yml)
fi

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
