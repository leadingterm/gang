{
  lib,
  modulesPath,
  gang,
  ...
}: {
  # The EC2 disk image, `system.build.ec2Image`, is defined in ./ec2-image.nix, which the image build (not this module) imports.
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
  # The EC2 module prints the SSH host keys on the console; without sshd there are none, so the unit could only fail.
  systemd.services.print-host-key.enable = false;

  services.amazon-ssm-agent.enable = true;

  services.chrony = {
    enable = true;
    servers = ["169.254.169.123"];
  };

  networking.firewall.enable = true;

  environment.systemPackages = [gang];

  boot.kernel.sysctl."fs.file-max" = 2097152;
}
