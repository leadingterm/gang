{
  lib,
  modulesPath,
  gang,
  ...
}: {
  # AMI disk image: `system.build.amazonImage` is defined in nixos/maintainers/scripts/ec2/amazon-image.nix, which the image build (not this module) must import.
  imports = [
    (modulesPath + "/virtualisation/amazon-image.nix")
    ./gang-config.nix
  ];

  virtualisation.amazon-init.enable = false;
  system.stateVersion = "26.11";

  nix.enable = false;
  users.mutableUsers = false;
  users.users.root.hashedPassword = "!";
  # Access is through the SSM agent; no credentials are baked into the image.
  users.allowNoPasswordLogin = true;

  # amazon-image.nix sets these at default priority; override them.
  services.openssh.enable = lib.mkForce false;
  networking.timeServers = lib.mkForce [];

  services.amazon-ssm-agent.enable = true;

  services.chrony = {
    enable = true;
    servers = ["169.254.169.123"];
  };

  networking.firewall.enable = true;

  environment.systemPackages = [gang];

  boot.kernel.sysctl."fs.file-max" = 2097152;
}
