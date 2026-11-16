{gang, ...}: {
  # The controller's listeners (public endpoint, worker). One system serves both roles: on a worker nothing
  # listens on them, and the worker security group has no ingress.
  networking.firewall.allowedTCPPorts = [443 9090];

  systemd.services.gang-server = {
    description = "Gang controller server";
    wantedBy = ["gang-controller.target"];
    # Requires gang-config.service: restarting gang-config.service also restarts this unit.
    requires = ["gang-config.service"];
    after = ["gang-config.service" "network-online.target"];
    wants = ["network-online.target"];

    serviceConfig = {
      User = "gang";
      Group = "gang";
      ExecStart = "${gang}/bin/gang server --config /etc/gang/gang.toml";
      Restart = "always";
      RestartSec = 5;
      LimitNOFILE = 1048576;
      TimeoutStopSec = 60;

      # Hardening. The controller needs no bubblewrap; it only needs the
      # state directory writable.
      NoNewPrivileges = true;
      # On the state disk when user data names it, on the root disk otherwise; shared database engines keep no state here.
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
