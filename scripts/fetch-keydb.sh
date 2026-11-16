#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"

tag="${1:-v6.3.4}"
target="reference/keydb"

rm -rf "$target"
mkdir -p reference
git clone --depth 1 --branch "$tag" https://github.com/Snapchat/KeyDB.git "$target"
printf 'fetched KeyDB %s into %s\n' "$tag" "$target"
