{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs?ref=nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forEachSystem = nixpkgs.lib.genAttrs systems;
    pkgsForEach = system:
      import nixpkgs {
        localSystem.system = system;
        overlays = [self.overlays.evix];
      };
  in {
    overlays = {
      evix = final: _: {
        evix = final.callPackage ./nix/package.nix {};
      };
    };

    packages = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      evix = pkgs.callPackage ./nix/package.nix {};
      default = self.packages.${system}.evix;
    });

    nixosModules = {
      default = ./nix/module.nix;
      evix = self.nixosModules.default;
    };

    devShells = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      default = pkgs.callPackage ./nix/shell.nix {};
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
