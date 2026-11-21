{
  config,
  pkgs,
  gang,
  ...
}: let
  pasta = import ./pasta.nix {inherit pkgs;};
  actions = "/sys/fs/cgroup/system.slice/gang-worker.service/actions";
  prepare = pkgs.writeShellScript "gang-prepare-action-egress" ''
    set -eu
    ${pkgs.coreutils}/bin/install -d -m 0755 -o root -g root /run/gang
    ${pkgs.coreutils}/bin/rm -f /run/gang/action-egress.json
    ${pkgs.coreutils}/bin/mkdir -p ${actions}
    ${pkgs.coreutils}/bin/chown gang:gang ${actions} ${actions}/cgroup.procs ${actions}/cgroup.subtree_control
    exec ${gang}/bin/gang __prepare-action-egress --config /etc/gang/gang.toml --cgroup ${actions} --nft ${pkgs.nftables}/bin/nft --ip ${pkgs.iproute2}/bin/ip
  '';
  cleanup = pkgs.writeShellScript "gang-cleanup-action-egress" ''
    set -eu
    ${pkgs.coreutils}/bin/rm -f /run/gang/action-egress.json
    ${pkgs.nftables}/bin/nft -f - <<'EOF'
    add table inet gang_action_egress
    delete table inet gang_action_egress
    EOF
  '';
in {
  imports = [./gang-scratch.nix];

  environment.etc."gang/action-rootfs".source = import ./action-rootfs {inherit pkgs;};
  environment.systemPackages = [pkgs.bubblewrap pasta pkgs.nftables];
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
    path = [pkgs.bubblewrap pasta];
    serviceConfig = {
      User = "gang";
      Group = "gang";
      # Delegate=yes hands this unit's cgroup subtree to the gang user: the
      # per-action cgroup scopes are created under that delegated subtree.
      Delegate = "yes";
      ExecStart = "${gang}/bin/gang worker --config /etc/gang/gang.toml";
      ExecStartPre = "+${prepare}";
      ExecStopPost = "+${cleanup}";
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
