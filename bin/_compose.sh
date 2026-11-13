# Sourced by the docker_* scripts. Moves to the repository root and sets
# `compose` to the docker compose command for this machine.
#
# The VPS compose file is added when infra/.env sets SITE_ADDRESS, so the same
# scripts serve a laptop and the server.

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
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
