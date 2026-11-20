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
    # Exercise the shipped gawk action tool in both PATH modes.
    printf '40 2\n' | gawk '{ print $1 + $2 }'
  '';
  # A real FHS env observes the loader's exact execve environment; its binary is pinned by nixpkgs.
  fhsEnv = pkgs.runCommand "fhs-env" {nativeBuildInputs = [pkgs.patchelf];} ''
    install -D -m 0755 ${pkgs.coreutils}/bin/env $out/bin/env
    patchelf --set-interpreter /${pkgs.stdenv.hostPlatform.libDir}/${baseNameOf pkgs.stdenv.cc.bintools.dynamicLinker} $out/bin/env
  '';
  binds = pkgs.lib.concatMapStringsSep " " (path: "--ro-bind ${path} ${path}") shipped.readonly_bind_paths;
  mounts = mounted: pkgs.lib.concatStringsSep " " (pkgs.lib.mapAttrsToList (destination: source: "--ro-bind ${source} ${destination}") mounted);
  # gang-bwrap's fixed sandbox arguments (crates/gang-bwrap/src/bwrap.rs), started with an empty environment.
  sandbox = mounted: environment: command: "env -i ${pkgs.bubblewrap}/bin/bwrap --unshare-all --die-with-parent --new-session --proc /proc --dev /dev --tmpfs /tmp --clearenv ${binds} ${mounts mounted} ${environment} -- ${command}";
  # Bazel's --incompatible_strict_action_env PATH.
  strictPath = "--setenv PATH /bin:/usr/bin:/usr/local/bin";
in
  pkgs.testers.runNixOSTest {
    name = "action-tools";
    globalTimeout = 300;

    nodeDefaults.virtualisation = {
      memorySize = 1024;
      cores = 1;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
    };

    nodes.machine = {imports = [../glibc-loader.nix ../action-tools.nix ../action-environment.nix];};

    testScript = ''
      machine.wait_for_unit("multi-user.target")

      with subtest("a /usr/bin/env bash script runs with Bazel's strict action PATH under the shipped binds"):
          assert machine.succeed("${sandbox (shipped.readonly_bind_mounts or {}) strictPath "${script}"}").strip() == "ran\n42"

      with subtest("it runs with no PATH at all, through the default search path"):
          assert machine.succeed("${sandbox (shipped.readonly_bind_mounts or {}) "" "${script}"}").strip() == "ran\n42"

      with subtest("without the mounts it cannot start"):
          machine.fail("${sandbox {} strictPath "${script}"}")

      # The pinned env clears bwrap's PWD before the FHS child starts.
      with subtest("the loader preserves an absent LD_LIBRARY_PATH"):
          assert machine.succeed("${sandbox (shipped.readonly_bind_mounts or {}) "" "${pkgs.coreutils}/bin/env -i ORIGINAL_MARKER=declared ${fhsEnv}/bin/env"}").strip() == "ORIGINAL_MARKER=declared"

      for value in ["", "::/declared path::tail:"]:
          with subtest(f"the loader preserves declared LD_LIBRARY_PATH {value!r}"):
              output = machine.succeed(f"${sandbox (shipped.readonly_bind_mounts or {}) "" "${pkgs.coreutils}/bin/env -i ORIGINAL_MARKER=declared LD_LIBRARY_PATH='{value}' ${fhsEnv}/bin/env"}")
              assert output.splitlines() == ["ORIGINAL_MARKER=declared", f"LD_LIBRARY_PATH={value}"]
    '';
  }
