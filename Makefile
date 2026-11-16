.PHONY: build test check spec conformance diff oracle clean

build:
	cargo build --workspace
	./zig/build.sh

test:
	cargo test --workspace
	./zig/test.sh

check:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo run -p xtask -- check

spec:
	cargo run -p xtask -- spec

conformance:
	./tests/conformance/run.sh --single unit/protocol --clients 1

diff:
	@echo "usage: ./tests/diff/diff.py --a-port <keydb> --b-port <rekeydb> --trace tests/diff/traces/core.txt"

oracle:
	./scripts/build-oracle.sh

clean:
	cargo clean
	rm -rf zig/zig-out zig/.zig-cache tests/tmp
