{
  config,
  pkgs,
  gang,
  ...
}: {
  imports = [./gang-scratch.nix];

  environment.etc."gang/action-rootfs".source = import ./action-rootfs {inherit pkgs;};
  environment.systemPackages = [pkgs.bubblewrap pkgs.slirp4netns pkgs.nftables pkgs.util-linux pkgs.iproute2];
  boot.kernelModules = ["nf_tables"];

  users.users.gang.home = "/var/lib/gang-home";

  systemd.services.gang-worker = {
    description = "Gang worker";
    wantedBy = ["gang-worker.target"];
    requires = ["gang-scratch.service" "gang-config.service"];
    after = [
      "gang-scratch.service"
      "gang-config.service"
      "network-online.target"
    ];
    wants = ["network-online.target"];
    path = [pkgs.bubblewrap pkgs.slirp4netns pkgs.nftables pkgs.util-linux pkgs.iproute2];
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
