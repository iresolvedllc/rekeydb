#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."

source_dir="${1:-KeyDB}"
jobs="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
malloc="${MALLOC:-libc}"

make -C "$source_dir" -j"$jobs" MALLOC="$malloc"
printf 'built oracle in %s/src/keydb-server\n' "$source_dir"
