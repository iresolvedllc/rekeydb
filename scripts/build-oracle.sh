#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."

source_dir="${1:-KeyDB}"
jobs="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
malloc="${MALLOC:-libc}"
make_args="-j$jobs MALLOC=$malloc NO_MOTD=yes"

if [ -f /etc/NIXOS ] && command -v nix-shell >/dev/null 2>&1; then
    exec nix-shell -p libuuid openssl zlib --run \
        "make -C '$source_dir' $make_args"
fi

# Debian/Ubuntu: libuuid1 + uuid-dev, libssl-dev, zlib1g-dev.
make -C "$source_dir" $make_args
printf 'built oracle in %s/src/keydb-server\n' "$source_dir"
