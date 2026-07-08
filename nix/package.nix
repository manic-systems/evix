{
  lib,
  rustPlatform,
  glibc,
  nixVersions,
  nixForBindings ? nixVersions.nix_2_34,
  capnproto,
  pkg-config,
  rustc,
}: let
  inherit (rustc) llvmPackages;
  workspace = lib.importTOML ../Cargo.toml;
in
  rustPlatform.buildRustPackage (finalAttrs: {
    pname = "evix";
    version = workspace.workspace.package.version;

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

    cargoLock.lockFile = ../Cargo.lock;
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
