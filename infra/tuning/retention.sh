#!/usr/bin/env bash
# Keeps closed journals and recordings under a disk quota, oldest first.
#
#   retention.sh DIRECTORY QUOTA_MEGABYTES
#
# Entries changed in the last ten minutes are treated as active and kept.
set -euo pipefail

directory="${1:?usage: retention.sh DIRECTORY QUOTA_MEGABYTES}"
quota_mb="${2:?usage: retention.sh DIRECTORY QUOTA_MEGABYTES}"

used_mb() {
    du -sm "$directory" | cut -f1
}

while [ "$(used_mb)" -gt "$quota_mb" ]; do
    oldest=$(find "$directory" -mindepth 1 -maxdepth 1 -mmin +10 -printf '%T@ %p\n' |
        sort -n | head -n 1 | cut -d' ' -f2-)
    if [ -z "$oldest" ]; then
        echo "over quota, but only active entries remain" >&2
        exit 1
    fi
    echo "removing $oldest"
    rm -rf -- "$oldest"
done
