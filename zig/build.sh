#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"

mkdir -p zig-out/bin
exec zig build-exe src/main.zig \
    -O "${ZIG_OPTIMIZE:-ReleaseSafe}" \
    -femit-bin=zig-out/bin/rekeydb-cli
