{pkgs}: let
  # The binds the checked-in AWS configuration gives sandboxed actions on the worker image.
  shipped = (builtins.fromTOML (builtins.readFile ../../../configs/gang.aws.toml)).sandbox.bwrap.readonly_bind_paths;
  # The same binds without the ELF interpreter directories, as workers had before the image served the loader.
  withoutLoader = builtins.filter (path: path != "/lib" && path != "/lib64") shipped;
  # A dynamically linked glibc program not linked for Nix, as a downloaded toolchain's binary is:
  # hello with the standard ELF interpreter and no RUNPATH.
  interpreter = "/${pkgs.stdenv.hostPlatform.libDir}/${baseNameOf pkgs.stdenv.cc.bintools.dynamicLinker}";
  fhsHello = pkgs.runCommand "fhs-hello" {nativeBuildInputs = [pkgs.patchelf];} ''
    install -D -m 0755 ${pkgs.hello}/bin/hello $out/bin/hello
    patchelf --set-interpreter ${interpreter} --remove-rpath $out/bin/hello
  '';
  # gang-bwrap's fixed sandbox arguments (crates/gang-bwrap/src/bwrap.rs), started with an empty environment.
  sandbox = binds: "env -i ${pkgs.bubblewrap}/bin/bwrap --unshare-all --die-with-parent --new-session --proc /proc --dev /dev --tmpfs /tmp --clearenv ${pkgs.lib.concatMapStringsSep " " (path: "--ro-bind ${path} ${path}") binds} -- ${fhsHello}/bin/hello";
in
  pkgs.testers.runNixOSTest {
    name = "glibc-loader";

    nodes.machine = {imports = [../glibc-loader.nix];};

    testScript = ''
      machine.wait_for_unit("multi-user.target")

      with subtest("both ELF interpreter directories exist, owned by root and writable by no one else"):
          assert machine.succeed("stat -c %U:%G:%a /lib /lib64").split() == ["root:root:755", "root:root:755"]

      with subtest("a glibc program runs in the sandbox under the shipped binds"):
          assert "Hello, world!" in machine.succeed("${sandbox shipped}")

      with subtest("without the interpreter directories it cannot start"):
          machine.fail("${sandbox withoutLoader}")
    '';
  }
