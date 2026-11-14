# The EC2 disk image of the system, built offline by systemd-repart so that its bytes are a function of its inputs:
# nothing boots or mounts it while it is built, partition and filesystem identifiers derive from `image.repart.seed`, timestamps are SOURCE_DATE_EPOCH,
# and the root filesystem is made from a copy of the closure with one inode per file, so the build host's store deduplication (hard links) never reaches the disk.
# The firmware boots the system's unified kernel image from the EFI system partition's removable-media path, on both architectures; no boot loader is installed or updated.
{
  config,
  lib,
  modulesPath,
  pkgs,
  ...
}: let
  image = config.system.build.image;
  efiArch = pkgs.stdenv.hostPlatform.efiArch;
  closure = pkgs.closureInfo {rootPaths = [config.system.build.toplevel];};
in {
  imports = [(modulesPath + "/image/repart.nix")];

  # Only the firmware reads the EFI system partition, to boot the unified kernel image; nothing in the running system does.
  # So the system never mounts or checks it, and nothing in its boot depends on it: `ec2.efi` is off on both architectures (nixpkgs turns it on by default on aarch64),
  # because in the EC2 module it only declares a `/boot` mount of the partition (and GRUB's EFI options), and that mount's file system check raced growpart's
  # rewrite of the disk's partition table and could hang the boot.
  ec2.efi = false;
  boot.loader.grub.enable = false;
  # Nor does systemd's GPT auto generator, which is on by default: because the unified kernel image's stub reports the partition it booted from,
  # it would declare a `/boot` mount of the EFI system partition with its file system check. The root, the image's only other partition, is mounted by label.
  systemd.generators.systemd-gpt-auto-generator = "/dev/null";
  # Nor writes it: systemd-boot-random-seed refreshes systemd-boot's random seed on the partition, and the image has no systemd-boot.
  systemd.services.systemd-boot-random-seed.enable = false;

  image.repart = {
    enable = true;
    # The raw disk; growpart extends the root partition to the EBS volume at boot.
    imageSize = "4G";
    # The root's label is the one the EC2 module mounts it by.
    partitions = {
      "00-esp" = {
        contents."/EFI/BOOT/BOOT${lib.toUpper efiArch}.EFI".source = "${config.system.build.uki}/${config.system.boot.loader.ukiFile}";
        repartConfig = {
          Type = "esp";
          Format = "vfat";
          Label = "ESP";
          # Exactly this size, so the root partition gets the rest of the image (repart would otherwise share the free space equally).
          # The unified kernel image is 42 MB on x86_64; on aarch64 its uncompressed 71 MB kernel alone is the larger part. 256 MiB is six times
          # x86_64's image and leaves aarch64's room to grow; repart fails the build if an image ever outgrows the partition.
          SizeMinBytes = "256M";
          SizeMaxBytes = "256M";
        };
      };
      # Last, so growpart can extend it.
      "10-root" = {
        repartConfig = {
          Type = "root";
          Format = "ext4";
          Label = "nixos";
          # The closure, from the build's link-free copy (system.build.image below); %T is the build's temporary directory.
          CopyFiles = "%T/nix-store:/nix/store";
        };
      };
    };
  };

  # systemd-repart copies hard links as hard links and mke2fs keeps them, so the build copies the closure with one inode per file first; the copy's top directory,
  # which becomes /nix/store, takes SOURCE_DATE_EPOCH as repart's own directories do. nixpkgs' repart module offers no build hook
  # (its image.repart.image is read-only and is system.build.image), so this overrides that derivation.
  system.build.image = lib.mkForce (config.image.repart.image.overrideAttrs {
    preBuild = ''
      mkdir "$TMPDIR/nix-store"
      xargs -r -d '\n' cp -a --no-preserve=links -t "$TMPDIR/nix-store" <${closure}/store-paths
      touch -d "@$SOURCE_DATE_EPOCH" "$TMPDIR/nix-store"
    '';
  });

  # The disk and its description in the layout of nixpkgs' EC2 image (`nix-support/image-info.json`), which AMI import tools read.
  system.build.ec2Image = pkgs.runCommand "${config.image.baseName}-ec2" {nativeBuildInputs = [pkgs.jq];} ''
    file=${image}/${config.image.fileName}
    mkdir -p $out/nix-support
    jq -n --arg label ${lib.escapeShellArg config.system.nixos.version} --arg system ${pkgs.stdenv.hostPlatform.system} \
      --arg logical_bytes "$(stat -c %s $file)" --arg file "$file" \
      '{label: $label, boot_mode: "uefi", system: $system, logical_bytes: $logical_bytes, file: $file, disks: {root: {logical_bytes: $logical_bytes, file: $file}}}' \
      >$out/nix-support/image-info.json
    ln -s $file $out/
  '';
}
