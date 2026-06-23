{
  evix,
  pkgs,
  lib,
  ...
}: let
  system = pkgs.stdenv.hostPlatform.system;
  localFlake = ''
    {
      outputs = { self }: {
        hydraJobs.${system} = {
          recurseForDerivations = true;
          smoke = derivation {
            name = "evix-smoke";
            system = "${system}";
            builder = "${pkgs.runtimeShell}";
            args = [ "-c" "echo ok > $out" ];
          };
        };
      };
    }
  '';
  remoteExpr = ''
    {
      recurseForDerivations = true;
      remote = {
        recurseForDerivations = true;
        smoke = derivation {
          name = "evix-remote-smoke";
          system = "${system}";
          builder = "${pkgs.runtimeShell}";
          args = [ "-c" "echo ok > $out" ];
        };
      };
    }
  '';
in
  pkgs.testers.runNixOSTest {
    name = "evix-eval";

    nodes = {
      host = {pkgs, ...}: {
        virtualisation.vlans = [1];
        environment.systemPackages = [
          evix
          pkgs.jq
          pkgs.netcat-openbsd
        ];

        nix.settings.experimental-features = [
          "nix-command"
          "flakes"
        ];
      };

      worker = {
        virtualisation.vlans = [1];

        environment.systemPackages = [
          evix
        ];
        nix.settings.experimental-features = [
          "nix-command"
          "flakes"
        ];
        networking.firewall.enable = false;
      };
    };

    testScript = ''
      import os
      import shlex

      SYSTEM = ${builtins.toJSON system}
      LOCAL_FLAKE = ${builtins.toJSON localFlake}
      REMOTE_EXPR = ${builtins.toJSON remoteExpr}
      CLIENT_EXPR = ${builtins.toJSON ''
        { label }: {
          recurseForDerivations = true;
          client = (derivation {
            name = "evix-''${label}";
            system = "${system}";
            builder = "${pkgs.runtimeShell}";
            args = [ "-c" "echo ok > $out" ];
          }) // {
            meta.description = "client fixture";
          };
        }
      ''}

      def q(value):
          return shlex.quote(value)

      def write_text(machine, path, contents):
          parent = os.path.dirname(path)
          if parent:
              machine.succeed(f"mkdir -p {q(parent)}")
          machine.succeed(f"printf %s {q(contents)} > {q(path)}")

      def assert_derivation(path, attr, name):
          host.succeed(
              "jq -e --arg attr "
              + q(attr)
              + " --arg name "
              + q(name)
              + " 'select(.attr == $attr and .name == $name and (.drvPath? | strings | endswith(\".drv\")))' "
              + q(path)
              + " >/dev/null"
          )

      def assert_meta(path, attr, description):
          host.succeed(
              "jq -e --arg attr "
              + q(attr)
              + " --arg description "
              + q(description)
              + " 'select(.attr == $attr and .meta.description == $description)' "
              + q(path)
              + " >/dev/null"
          )

      def start_daemon():
          host.succeed("rm -f /tmp/evix.sock /tmp/evixd.log /tmp/evixd.pid")
          host.succeed(
              "evix daemon --socket /tmp/evix.sock --foreground > /tmp/evixd.log 2>&1 & echo $! > /tmp/evixd.pid"
          )
          host.wait_until_succeeds("test -S /tmp/evix.sock")

      def stop_daemon():
          host.succeed("test ! -e /tmp/evixd.pid || kill $(cat /tmp/evixd.pid) 2>/dev/null || true")

      def start_worker():
          worker.succeed("rm -f /tmp/evix-worker.log /tmp/evix-worker.pid")
          worker.succeed(
              "evix worker --listen 0.0.0.0:7357 > /tmp/evix-worker.log 2>&1 & echo $! > /tmp/evix-worker.pid"
          )
          host.wait_until_succeeds("nc -z worker 7357")

      def stop_worker():
          worker.succeed("test ! -e /tmp/evix-worker.pid || kill $(cat /tmp/evix-worker.pid) 2>/dev/null || true")

      with subtest("boot VMs and start remote worker"):
          host.start()
          worker.start()
          host.wait_for_unit("multi-user.target")
          worker.wait_for_unit("multi-user.target")
          start_worker()

      with subtest("client local evaluation"):
          write_text(host, "/tmp/evix-fixture/flake.nix", LOCAL_FLAKE)
          write_text(host, "/tmp/evix-client.nix", CLIENT_EXPR)

          host.succeed(
              "cd /tmp/evix-fixture && evix eval --no-daemon --flake .#hydraJobs > /tmp/evix-local.ndjson"
          )
          assert_derivation("/tmp/evix-local.ndjson", f"{SYSTEM}.smoke", "evix-smoke")

          host.succeed(
              "evix eval --socket /tmp/missing-evix.sock --file /tmp/evix-client.nix --argstr label fallback --meta > /tmp/evix-fallback.ndjson"
          )
          assert_derivation("/tmp/evix-fallback.ndjson", "client", "evix-fallback")
          assert_meta("/tmp/evix-fallback.ndjson", "client", "client fixture")

      with subtest("daemon eval, query, and diff"):
          start_daemon()
          try:
              host.succeed(
                  "evix eval --socket /tmp/evix.sock --flake path:/tmp/evix-fixture#hydraJobs > /tmp/evix-daemon.ndjson"
              )
              assert_derivation("/tmp/evix-daemon.ndjson", f"{SYSTEM}.smoke", "evix-smoke")

              host.succeed(
                  "evix query --socket /tmp/evix.sock --flake path:/tmp/evix-fixture#hydraJobs --system "
                  + q(SYSTEM)
                  + " --attr-prefix "
                  + q(f"{SYSTEM}.smoke")
                  + " > /tmp/evix-query.ndjson"
              )
              assert_derivation("/tmp/evix-query.ndjson", f"{SYSTEM}.smoke", "evix-smoke")

              host.succeed(
                  "evix diff --socket /tmp/evix.sock --flake path:/tmp/evix-fixture#hydraJobs > /tmp/evix-diff.json"
              )
              host.succeed(
                  "jq -e '.added == [] and .removed == [] and .errors == []' /tmp/evix-diff.json >/dev/null"
              )

              host.succeed(
                  "evix eval --socket /tmp/evix.sock --file /tmp/evix-client.nix --argstr label daemon --meta >/tmp/evix-file-daemon.ndjson"
              )
              assert_derivation("/tmp/evix-file-daemon.ndjson", "client", "evix-daemon")
              assert_meta("/tmp/evix-file-daemon.ndjson", "client", "client fixture")

              host.succeed(
                  "evix query --socket /tmp/evix.sock --file /tmp/evix-client.nix --argstr label daemon --meta --attr-prefix client >/tmp/evix-file-query.ndjson"
              )
              assert_derivation("/tmp/evix-file-query.ndjson", "client", "evix-daemon")
              assert_meta("/tmp/evix-file-query.ndjson", "client", "client fixture")
          finally:
              stop_daemon()

      with subtest("remote worker evaluation"):
          host.succeed(
              "evix eval --no-daemon --workers 0 --remote worker:7357 "
              + q(SYSTEM)
              + " 1 --expr "
              + q(REMOTE_EXPR)
              + " > /tmp/evix-remote.ndjson"
          )
          assert_derivation("/tmp/evix-remote.ndjson", "remote.smoke", "evix-remote-smoke")

      stop_worker()
    '';
  }
