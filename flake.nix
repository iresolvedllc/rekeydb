{
  description = "rekeydb — Rust and Zig port of KeyDB";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs { inherit system; }));
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            # Rust toolchain, pinned by rust-toolchain.toml (currently 1.99.1).
            cargo
            clippy
            rustc
            rustfmt
            rust-analyzer

            # Zig 0.17.x for the CLI, tools, and FLASH substrate.
            zig

            # Conformance harness and differential tooling.
            tcl
            python3

            # KeyDB oracle build.
            gcc
            gnumake
            pkg-config
            git
            curl
            libuuid
            openssl
            zlib
            jemalloc

            # Debugging and profiling the oracle.
            gdb
            valgrind

            # Nix formatting.
            nixfmt
          ];

          shellHook = ''
            echo "rekeydb development shell"
            echo "  rustc: $(rustc --version)"
            echo "  cargo: $(cargo --version)"
            echo "  zig:   $(zig version)"
            echo "  tcl:   $(echo 'puts $tcl_version' | tclsh)"
            echo ""
            echo "  make build     # cargo build + zig CLI"
            echo "  make test      # Rust + Zig tests"
            echo "  make check     # fmt, clippy, spec manifests"
            echo "  ./scripts/build-oracle.sh"
            echo ""
            export REKEYDB_ROOT="$PWD"
            export RUST_BACKTRACE="''${RUST_BACKTRACE:-1}"
          '';
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
