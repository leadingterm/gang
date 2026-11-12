{
  config,
  lib,
  pkgs,
  ...
}: let
  scratch = pkgs.writeShellApplication {
    name = "gang-scratch";
    runtimeInputs = [
      pkgs.mdadm
      pkgs.xfsprogs
      pkgs.util-linux
      pkgs.coreutils
      pkgs.gnugrep
      pkgs.yq-go
      pkgs.systemd
    ];
    text = ''
      mp="$(yq -p toml '.provisioner.aws.scratch_mount_point' /etc/gang/gang.toml)"
      if [ -z "$mp" ] || [ "$mp" = "null" ]; then
        echo "gang-scratch: provisioner.aws.scratch_mount_point is not set in /etc/gang/gang.toml" >&2
        exit 1
      fi

      if mountpoint -q "$mp"; then
        echo "gang-scratch: $mp is already mounted"
        exit 0
      fi

      # Instance-store NVMe namespaces. Each namespace has two by-id links
      # (plain serial and a suffixed one), so resolve, de-duplicate, and keep
      # whole disks only.
      devs=()
      declare -A seen=()
      for link in ${config.gang.scratch.instanceStoreGlob}; do
        [ -e "$link" ] || continue
        disk="$(readlink -f "$link")"
        if [ -n "''${seen[$disk]:-}" ]; then
          continue
        fi
        seen[$disk]=1
        if [ "$(lsblk -dno TYPE "$disk")" = "disk" ]; then
          devs+=("$disk")
        fi
      done

      if [ ''${#devs[@]} -eq 0 ]; then
        if [ -e /dev/sdf ]; then
          devs=(/dev/sdf)
        else
          echo "gang-scratch: no instance-store NVMe disks and no EBS scratch volume at /dev/sdf" >&2
          exit 1
        fi
      fi

      if [ ''${#devs[@]} -eq 1 ]; then
        dev="''${devs[0]}"
      else
        # udev may have reassembled a RAID0 from a previous boot under another
        # name, holding the member disks busy; stop every array so --create
        # can claim them.
        mdadm --stop --scan || true
        mdadm --create /dev/md/gang-scratch --run --level=0 \
          --raid-devices=''${#devs[@]} --force "''${devs[@]}"
        dev=/dev/md/gang-scratch
        udevadm settle
        [ -b "$dev" ] || exit 1
      fi

      mkfs.xfs -f -m reflink=1 "$dev"
      mkdir -p "$mp"
      mount -o noatime "$dev" "$mp"
      chown gang:gang "$mp"
      chmod 0755 "$mp"
    '';
  };
in {
  options.gang.scratch.instanceStoreGlob = lib.mkOption {
    type = lib.types.str;
    default = "/dev/disk/by-id/nvme-Amazon_EC2_NVMe_Instance_Storage_*";
    description = "Glob of the instance-store NVMe devices that make up the scratch volume; the disks are resolved, de-duplicated and kept when they are whole disks.";
  };

  config = {
    boot.swraid.enable = true;
    # No alert mail or program on workers; silences mdadm's MAILADDR/PROGRAM warning.
    boot.swraid.mdadmConf = "PROGRAM ${pkgs.coreutils}/bin/true";
    boot.kernelModules = ["raid0" "xfs"];

    systemd.services.gang-scratch = {
      description = "Assemble and mount Gang worker scratch storage";
      wantedBy = ["multi-user.target"];
      requires = ["gang-config.service"];
      after = ["gang-config.service"];
      before = ["gang-worker.service"];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = lib.getExe scratch;
      };
    };
  };
}
