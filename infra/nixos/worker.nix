{
  config,
  pkgs,
  gang,
  ...
}: {
  imports = [./gang-scratch.nix];

  # bwrap bind paths reference /run/current-system/sw.
  environment.systemPackages = [pkgs.bubblewrap];

  users.users.gang.home = "/var/lib/gang-home";

  # The worker accepts no inbound connections.
  networking.firewall = {
    allowedTCPPorts = [];
    allowedUDPPorts = [];
  };

  systemd.services.gang-worker = {
    description = "Gang worker";
    wantedBy = ["multi-user.target"];
    requires = ["gang-scratch.service" "gang-config.service"];
    after = [
      "gang-scratch.service"
      "gang-config.service"
      "network-online.target"
    ];
    wants = ["network-online.target"];
    path = [pkgs.bubblewrap];
    serviceConfig = {
      User = "gang";
      Group = "gang";
      # Delegate=yes hands this unit's cgroup subtree to the gang user: the
      # per-action cgroup scopes are created under that delegated subtree.
      Delegate = "yes";
      ExecStart = "${gang}/bin/gang worker --config /etc/gang/gang.toml";
      Restart = "always";
      RestartSec = 5;
      LimitNOFILE = 1048576;
      TimeoutStopSec = 120;
      WorkingDirectory = config.users.users.gang.home;
      # systemd takes the state directory name relative to /var/lib, so this
      # is the last element of users.users.gang.home.
      StateDirectory = "gang-home";
      Environment = ["HOME=${config.users.users.gang.home}"];
    };
  };
}
