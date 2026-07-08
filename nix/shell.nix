{
  mkShell,
  rustPlatform,
  rustc,
  cargo,
  rust-analyzer,
  rustfmt,
  clippy,
  taplo,
  pkg-config,
  capnproto,
  nixVersions,
  nixForBindings ? nixVersions.nix_2_34,
  glibc,
  cargo-nextest,
  jq,
  hyperfine,
  nix-eval-jobs,
}: let
  inherit (rustc) llvmPackages;
in
  mkShell {
    name = "evix";

    strictDeps = true;
    nativeBuildInputs = [
      pkg-config
      cargo
      rustc
      llvmPackages.lld
      capnproto # remote protocol

      (rustfmt.override {asNightly = true;})
      clippy
      taplo
      rust-analyzer

      # Additional Cargo tooling
      cargo-nextest

      # Benchmarking
      jq
      hyperfine
      nix-eval-jobs
    ];

    buildInputs = [
      nixForBindings.dev
      glibc.dev
    ];

    env = {
      RUST_SRC_PATH = "${rustPlatform.rustLibSrc}";
      LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
      BINDGEN_EXTRA_CLANG_ARGS = "--sysroot=${glibc.dev}";
    };
  }
