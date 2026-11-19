{pkgs}: let
  ports = builtins.fromJSON (builtins.readFile ../../terraform/modules/network/ports.json);
  content = pkgs.writeTextDir "serve" "healthy\n";
in
  pkgs.testers.runNixOSTest {
    name = "listener-firewall";
    globalTimeout = 300;

    nodeDefaults.virtualisation = {
      memorySize = 1024;
      cores = 1;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
    };

    nodes = {
      machine = {
        imports = [../controller.nix];
        _module.args.gang = pkgs.writeShellScriptBin "gang" "exit 1";
        networking.firewall.enable = true;

        systemd.services.health-test-server = {
          wantedBy = ["multi-user.target"];
          serviceConfig.ExecStart = "${pkgs.python3}/bin/python3 -m http.server ${toString ports.health} --directory ${content}";
        };
      };
      client = {environment.systemPackages = [pkgs.curl];};
    };

    testScript = ''
      from datetime import timedelta

      start_all()
      machine.wait_for_unit("health-test-server.service")
      machine.wait_for_open_port(${toString ports.health})
      client.wait_until_succeeds("${pkgs.curl}/bin/curl --fail --silent --max-time 2 http://machine:${toString ports.health}/serve", timeout=timedelta(seconds=10))
    '';
  }
