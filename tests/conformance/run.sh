#!/usr/bin/env sh
# Run the upstream KeyDB Tcl suite against rekeydb.
#
# The suite is copied into tests/tmp/conformance so it can create temporary
# files without touching the reference tree. `src/keydb-server` is symlinked to
# the rekeydb binary; the stock KeyDB CLI is used when available.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"

reference="${REKEYDB_REFERENCE:-KeyDB}"
if [ ! -f "$reference/tests/test_helper.tcl" ]; then
    if [ -f reference/keydb/tests/test_helper.tcl ]; then
        reference=reference/keydb
    else
        echo "cannot find KeyDB tests; run scripts/fetch-keydb.sh" >&2
        exit 1
    fi
fi

cargo build -q -p rekeydb-server --bin rekeydb
binary="${REKEYDB_BINARY:-$root/target/debug/rekeydb}"
run_dir="$root/tests/tmp/conformance-$$"

rm -rf "$run_dir"
mkdir -p "$run_dir/src"
cp -a "$reference/tests" "$run_dir/tests"
# The upstream helpers stop background load with /bin/kill, which does not
# exist on NixOS (and some other systems). Use the PATH-resolved kill so the
# writers actually stop before digests and offset checks.
find "$run_dir/tests" -name '*.tcl' -print0 |
    xargs -0 sed -i 's#exec /bin/kill#exec kill#g'
rm -rf "$run_dir/tests/tmp"
ln -s "$binary" "$run_dir/src/keydb-server"

if [ -x "$reference/src/keydb-cli" ]; then
    ln -s "$root/$reference/src/keydb-cli" "$run_dir/src/keydb-cli"
    [ -x "$reference/src/keydb-check-aof" ] && ln -s "$root/$reference/src/keydb-check-aof" "$run_dir/src/keydb-check-aof"
    [ -x "$reference/src/keydb-check-rdb" ] && ln -s "$root/$reference/src/keydb-check-rdb" "$run_dir/src/keydb-check-rdb"
else
    cat > "$run_dir/src/keydb-cli" <<EOF
#!/usr/bin/env sh
exec "$root/zig/zig-out/bin/rekeydb-cli" "\$@"
EOF
    chmod +x "$run_dir/src/keydb-cli"
fi

cd "$run_dir"

if command -v tclsh >/dev/null 2>&1; then
    exec tclsh tests/test_helper.tcl "$@"
fi
if command -v tclsh8.6 >/dev/null 2>&1; then
    exec tclsh8.6 tests/test_helper.tcl "$@"
fi
if command -v nix-shell >/dev/null 2>&1; then
    exec nix-shell -p tcl --run "tclsh tests/test_helper.tcl $*"
fi

echo "tclsh not found; install Tcl 8.6 or use nix-shell" >&2
exit 1
