{pkgs}: let
  # The bwrap section the checked-in AWS configuration gives sandboxed actions on the worker image, whose modules the node imports.
  shipped = (builtins.fromTOML (builtins.readFile ../../../configs/gang.aws.toml)).sandbox.bwrap;
  # A script as toolchains ship them (rules_js's launcher): its interpreter through /usr/bin/env,
  # its tools through the action's PATH, and the shells actions name by absolute path.
  script = pkgs.writeScript "fhs-tools" ''
    #!/usr/bin/env bash
    set -eu
    /bin/sh -c 'exit 0'
    /bin/bash -c 'exit 0'
    printf 'RAN\n' | tr A-Z a-z
  '';
  binds = pkgs.lib.concatMapStringsSep " " (path: "--ro-bind ${path} ${path}") shipped.readonly_bind_paths;
  mounts = mounted: pkgs.lib.concatStringsSep " " (pkgs.lib.mapAttrsToList (destination: source: "--ro-bind ${source} ${destination}") mounted);
  # gang-bwrap's fixed sandbox arguments (crates/gang-bwrap/src/bwrap.rs), started with an empty environment.
  sandbox = mounted: environment: "env -i ${pkgs.bubblewrap}/bin/bwrap --unshare-all --die-with-parent --new-session --proc /proc --dev /dev --tmpfs /tmp --clearenv ${binds} ${mounts mounted} ${environment} -- ${script}";
  # Bazel's --incompatible_strict_action_env PATH.
  strictPath = "--setenv PATH /bin:/usr/bin:/usr/local/bin";
in
  pkgs.testers.runNixOSTest {
    name = "action-tools";

    nodes.machine = {imports = [../glibc-loader.nix ../action-tools.nix];};

    testScript = ''
      machine.wait_for_unit("multi-user.target")

      with subtest("a /usr/bin/env bash script runs with Bazel's strict action PATH under the shipped binds"):
          assert machine.succeed("${sandbox (shipped.readonly_bind_mounts or {}) strictPath}").strip() == "ran"

      with subtest("it runs with no PATH at all, through the default search path"):
          assert machine.succeed("${sandbox (shipped.readonly_bind_mounts or {}) ""}").strip() == "ran"

      with subtest("without the mounts it cannot start"):
          machine.fail("${sandbox {} strictPath}")
    '';
  }
