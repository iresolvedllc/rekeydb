#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
exec zig test src/main.zig
