{
  lib,
  rustPlatform,
  glibc,
  nixVersions,
  capnproto,
  pkg-config,
  rustc,
}: let
  inherit (rustc) llvmPackages;
  nixForBindings = nixVersions.latest;
in
  rustPlatform.buildRustPackage (finalAttrs: {
    pname = "evix";
    version = "0.3.3";

    src = let
      fs = lib.fileset;
      s = ../.;
    in
      fs.toSource {
        root = s;
        fileset = fs.unions [
          (s + /crates)
          (s + /Cargo.lock)
          (s + /Cargo.toml)
        ];
      };

    cargoLock.lockFile = "${finalAttrs.src}/Cargo.lock";
    cargoBuildFlags = ["-p" "evix-cli" "-p" "evix-daemon"];
    cargoTestFlags =
      ["-p" "evix" "-p" "evix-cli" "-p" "evix-daemon" "--lib" "--bins"];
    useNextest = true;

    enableParallelBuilding = true;
    strictDeps = true;
    nativeBuildInputs = [
      capnproto
      pkg-config
    ];

    buildInputs = [
      nixForBindings.dev
      glibc.dev
    ];

    env = {
      LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
      BINDGEN_EXTRA_CLANG_ARGS = "--sysroot=${glibc.dev}";
    };

    meta = {
      description = "Evaluate a Nix expression and stream derivation info as JSON lines";
      homepage = "https://github.com/manic-systems/evix";
      mainProgram = "evix";
      license = lib.licenses.eupl12;
      maintainers = with lib.maintainers; [NotAShelf];
    };
  })
