{
  config,
  pkgs,
  lib,
  ...
}: let
  inherit (lib.modules) mkIf;
  inherit (lib.options) mkOption mkEnableOption mkPackageOption;
  inherit (lib.types) nullOr str;

  cfg = config.services.evix;
in {
  options.services.evix = {
    enable = mkEnableOption "evix Nix evaluator service and CLI";

    package = mkPackageOption pkgs "evix" {};

    daemon = {
      enable = mkEnableOption "the evix daemon user service";

      socket = mkOption {
        type = nullOr str;
        default = null;
        example = "%t/evix.sock";
        description = "Optional `EVIX_SOCKET` path for the user daemon.";
      };
    };
  };

  config = mkIf cfg.enable {
    environment.systemPackages = [cfg.package]; # provides evix-cli and evixd

    systemd.user.services.evixd = mkIf cfg.daemon.enable {
      description = "evix daemon";
      wantedBy = ["default.target"];
      serviceConfig = {
        ExecStart = "${lib.getExe' cfg.package "evixd"} --foreground";
        Restart = "on-failure";
        Environment = lib.optionals (cfg.daemon.socket != null) [
          "EVIX_SOCKET=${cfg.daemon.socket}"
        ];

        WorkingDirectory = "";
        PrivateDevices = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        RestrictSUIDSGID = true;
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ReadWritePaths = [
          "%t"
          "/tmp"
          "/var/tmp"
          "-%h/.cache/nix"
          "-%h/.local/share/nix"
          "-/nix/var/nix/gcroots"
        ];
      };
    };
  };
}
