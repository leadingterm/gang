{gang, ...}: {
  # State volume: EBS attached as /dev/sdf by the controller module; formatted
  # ext4 on first boot when blank.
  fileSystems."/var/lib/gang" = {
    device = "/dev/sdf";
    fsType = "ext4";
    autoFormat = true;
    options = ["noatime" "nofail"];
  };

  # The security group is the real network filter; these ports are the
  # controller listeners (public endpoint, worker).
  networking.firewall.allowedTCPPorts = [443 9090];

  systemd.services.gang-server = {
    description = "Gang controller server";
    wantedBy = ["multi-user.target"];
    # Requires gang-config.service: restarting gang-config.service also restarts this unit.
    requires = ["gang-config.service"];
    after = ["gang-config.service" "network-online.target"];
    wants = ["network-online.target"];
    unitConfig.RequiresMountsFor = "/var/lib/gang";

    serviceConfig = {
      User = "gang";
      Group = "gang";
      ExecStart = "${gang}/bin/gang server --config /etc/gang/gang.toml";
      Restart = "always";
      RestartSec = 5;
      LimitNOFILE = 1048576;
      TimeoutStopSec = 60;

      # Hardening. The controller needs no bubblewrap; it only needs the
      # state volume writable.
      NoNewPrivileges = true;
      StateDirectory = "gang";
      StateDirectoryMode = "0750";
      ProtectSystem = "strict";
      ProtectHome = true;
      PrivateTmp = true;
      ProtectKernelTunables = true;
      ProtectControlGroups = true;
      RestrictAddressFamilies = ["AF_INET" "AF_INET6" "AF_UNIX"];
      # Binds the public port 443 as the unprivileged gang user.
      AmbientCapabilities = ["CAP_NET_BIND_SERVICE"];
      CapabilityBoundingSet = ["CAP_NET_BIND_SERVICE"];
    };
  };
}
