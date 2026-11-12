{pkgs}: let
  # gang-config needs EC2 to write gang.toml; scratch only reads that file, so stub it.
  base = {pkgs, ...}: {
    imports = [../gang-scratch.nix];

    environment.systemPackages = [pkgs.xfsprogs pkgs.mdadm];

    users.groups.gang = {};
    users.users.gang = {
      isSystemUser = true;
      group = "gang";
    };

    systemd.services.gang-config = {
      description = "Stub gang-config: writes gang.toml without EC2";
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      script = ''
        mkdir -p /etc/gang
        printf '[provisioner.aws]\nscratch_mount_point = "/mnt/scratch"\n' > /etc/gang/gang.toml
      '';
    };
  };
in
  pkgs.testers.runNixOSTest {
    name = "gang-scratch";

    nodes = {
      raid = {
        imports = [base];
        virtualisation.emptyDiskImages = [512 512];
        gang.scratch.instanceStoreGlob = "/dev/vd[bc]";
      };

      single = {
        imports = [base];
        virtualisation.emptyDiskImages = [512];
        gang.scratch.instanceStoreGlob = "/dev/vd[b]";
      };

      none = {
        imports = [base];
        gang.scratch.instanceStoreGlob = "/dev/vd[b]";
      };
    };

    testScript = ''
      start_all()

      with subtest("two disks assemble a reflink xfs RAID0 scratch volume"):
          raid.wait_for_unit("gang-scratch.service")
          raid.succeed("mountpoint -q /mnt/scratch")
          assert raid.succeed("findmnt -no FSTYPE /mnt/scratch").strip() == "xfs"
          raid.succeed("xfs_info /mnt/scratch | grep -q 'reflink=1'")
          raid.succeed("mdadm --detail /dev/md/gang-scratch | grep -q 'Raid Level : raid0'")
          raid.succeed("mdadm --detail /dev/md/gang-scratch | grep -q 'Raid Devices : 2'")
          assert raid.succeed("stat -c %U:%G:%a /mnt/scratch").strip() == "gang:gang:755"
          raid.succeed("dd if=/dev/urandom of=/mnt/scratch/src bs=1M count=1 status=none")
          raid.succeed("cp --reflink=always /mnt/scratch/src /mnt/scratch/dst")
          raid.succeed("cmp /mnt/scratch/src /mnt/scratch/dst")
          raid.succeed("echo marker > /mnt/scratch/marker")
          # Already mounted: the service must exit 0 and must not reformat.
          raid.succeed("systemctl restart gang-scratch.service")
          raid.succeed("test -f /mnt/scratch/marker")

      with subtest("one disk mounts directly without RAID"):
          single.wait_for_unit("gang-scratch.service")
          single.succeed("mountpoint -q /mnt/scratch")
          assert single.succeed("findmnt -no FSTYPE /mnt/scratch").strip() == "xfs"
          single.succeed("xfs_info /mnt/scratch | grep -q 'reflink=1'")
          single.succeed("test ! -e /dev/md/gang-scratch")

      with subtest("no instance-store disks and no /dev/sdf fails"):
          none.wait_for_unit("multi-user.target")
          none.succeed("test ! -e /dev/sdf")
          none.fail("systemctl start gang-scratch.service")
          none.succeed("journalctl -u gang-scratch | grep -q 'no instance-store NVMe disks'")
    '';
  }
