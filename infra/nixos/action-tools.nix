{
  config,
  lib,
  pkgs,
  ...
}: let
  # Bash as a Linux distribution builds it: with no PATH it searches /usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:., where nixpkgs' default bash searches nothing.
  fhsBash = pkgs.bash.override {forFHSEnv = true;};
in {
  options.gang.actionTools = lib.mkOption {
    type = lib.types.listOf lib.types.package;
    default = [fhsBash] ++ (with pkgs; [coreutils diffutils findutils gawk gnugrep gnused gnutar gzip]);
    defaultText = lib.literalExpression "[(pkgs.bash.override {forFHSEnv = true;})] ++ (with pkgs; [coreutils diffutils findutils gawk gnugrep gnused gnutar gzip])";
    description = ''
      The programs a sandboxed action finds at /bin and /usr/bin, where a Linux build host has
      them: /bin/sh and /bin/bash (Bazel's default shell executable and `#!/bin/sh` scripts),
      /usr/bin/env (`#!/usr/bin/env bash` launchers) and the tools of Bazel's strict action
      PATH=/bin:/usr/bin:/usr/local/bin. They are linked under /etc/gang-action-tools/bin, which
      `[sandbox.bwrap] readonly_bind_mounts` binds read-only at /bin and /usr/bin.
      The default bash is built as for an FHS environment: with no PATH it searches
      /usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:., as a Linux distribution's
      bash does, so actions Bazel starts without a PATH find the tools.
    '';
  };

  config = {
    environment.etc."gang-action-tools".source = pkgs.buildEnv {
      name = "gang-action-tools";
      paths = config.gang.actionTools;
      pathsToLink = ["/bin"];
    };

    # With no PATH, nixpkgs' glibc searches /run/current-system/sw/bin before /bin (its CS_PATH), and
    # the sandbox binds the system profile, so the profile's bash is the same FHS build.
    environment.systemPackages = [(lib.hiPrio fhsBash)];
  };
}
