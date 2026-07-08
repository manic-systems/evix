{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs?ref=nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forEachSystem = nixpkgs.lib.genAttrs systems;
    pkgsForEach = system: nixpkgs.legacyPackages.${system};

    # nix-bindings links against Nix C++ headers; package and dev shell must
    # agree on this version until the crate compatibility window changes.
    nixForBindingsFor = pkgs: pkgs.nixVersions.nix_2_34;
  in {
    packages = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      evix = pkgs.callPackage ./nix/package.nix {
        nixForBindings = nixForBindingsFor pkgs;
      };
      default = self.packages.${system}.evix;
    });

    nixosModules = {
      default = ./nix/module.nix;
      evix = self.nixosModules.default;
    };

    devShells = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      default = pkgs.callPackage ./nix/shell.nix {
        nixForBindings = nixForBindingsFor pkgs;
      };
    });

    checks = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      eval = pkgs.callPackage ./nix/tests/eval.nix {
        evix = self.packages.${system}.evix;
      };
    });

    hydraJobs = self.packages;
  };
}
