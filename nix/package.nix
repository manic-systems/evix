{
  lib,
  stdenv,
  buildPackages,
  rustPlatform,
  installShellFiles,
  glibc,
  nixVersions,
  nixForBindings ? nixVersions.nix_2_35,
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
          (s + /.clippy.toml)
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
      installShellFiles
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

    postInstall = lib.optionalString (stdenv.hostPlatform.emulatorAvailable buildPackages) (
      let
        evix = "${stdenv.hostPlatform.emulator buildPackages} $out/bin/evix";
      in ''
        installShellCompletion --cmd evix \
          --bash    <(COMPLETE=bash ${evix}) \
          --zsh     <(COMPLETE=zsh  ${evix}) \
          --fish    <(COMPLETE=fish ${evix}) \
          --nushell <(COMPLETE=nu   ${evix})
      ''
    );

    meta = {
      description = "Evaluate a Nix expression and stream derivation info as JSON lines";
      homepage = "https://github.com/manic-systems/evix";
      mainProgram = "evix";
      license = lib.licenses.eupl12;
      maintainers = with lib.maintainers; [NotAShelf];
    };
  })
