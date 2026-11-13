# The EC2 disk image of the system, built offline by systemd-repart so that its bytes are a function of its inputs:
# nothing boots or mounts it while it is built, partition and filesystem identifiers derive from `image.repart.seed`, and timestamps are SOURCE_DATE_EPOCH.
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
in {
  imports = [(modulesPath + "/image/repart.nix")];

  ec2.efi = true;
  boot.loader.grub.enable = false;

  image.repart = {
    enable = true;
    # The raw disk; growpart extends the root partition to the EBS volume at boot.
    imageSize = "4G";
    # The labels are those the EC2 module mounts by.
    partitions = {
      "00-esp" = {
        contents."/EFI/BOOT/BOOT${lib.toUpper efiArch}.EFI".source = "${config.system.build.uki}/${config.system.boot.loader.ukiFile}";
        repartConfig = {
          Type = "esp";
          Format = "vfat";
          Label = "ESP";
          # Room for the unified kernel image.
          SizeMinBytes = "256M";
        };
      };
      # Last, so growpart can extend it.
      "10-root" = {
        storePaths = [config.system.build.toplevel];
        repartConfig = {
          Type = "root";
          Format = "ext4";
          Label = "nixos";
        };
      };
    };
  };

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
