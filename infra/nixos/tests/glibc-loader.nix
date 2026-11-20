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
  # FHS-interpreter gawk retains its own RUNPATH with an empty environment.
  hermeticGawk = pkgs.runCommand "hermetic-gawk" {nativeBuildInputs = [pkgs.patchelf];} ''
    install -D -m 0755 ${pkgs.gawk}/bin/gawk $out/bin/gawk
    patchelf --set-interpreter ${interpreter} $out/bin/gawk
  '';
  envProbe = pkgs.runCommandCC "environment-writable-probe" {nativeBuildInputs = [pkgs.patchelf];} ''
    cat > env-probe.c <<'EOF'
    #include <stdio.h>
    #include <sys/auxv.h>
    #include <unistd.h>
    #include <string.h>

    extern char **environ;

    int main(void) {
      if (getauxval(AT_PAGESZ) != (unsigned long) sysconf(_SC_PAGESIZE)
          || getauxval(AT_EXECFN) == 0) {
        fprintf(stderr, "loader auxiliary vector was not preserved\n");
        return 1;
      }
      for (char **entry = environ; *entry; ++entry) {
        puts(*entry);
        volatile char *separator = strchr(*entry, '=');
        if (separator) {
          char saved = *separator;
          *separator = '\0';
          *separator = saved;
        }
      }
      return 0;
    }
    EOF
    mkdir -p $out/bin
    $CC -O2 -Wall -Wextra -Werror env-probe.c -o $out/bin/env-probe
    patchelf --set-interpreter ${interpreter} $out/bin/env-probe
  '';
  fhsZstd = pkgs.runCommand "fhs-zstd" {nativeBuildInputs = [pkgs.patchelf];} ''
    install -D -m 0755 ${pkgs.zstd}/bin/zstd $out/bin/zstd
    patchelf --set-interpreter ${interpreter} --remove-rpath $out/bin/zstd
  '';
  # Clear bwrap's inserted PWD too, without injecting worker or action environment variables.
  sandbox = binds: command: "env -i ${pkgs.bubblewrap}/bin/bwrap --unshare-all --die-with-parent --new-session --proc /proc --dev /dev --tmpfs /tmp --clearenv ${pkgs.lib.concatMapStringsSep " " (path: "--ro-bind ${path} ${path}") binds} -- ${pkgs.coreutils}/bin/env -i ${command}";
in
  pkgs.testers.runNixOSTest {
    name = "glibc-loader";
    globalTimeout = 300;

    nodeDefaults.virtualisation = {
      memorySize = 1024;
      cores = 1;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
    };

    nodes.machine = {imports = [../glibc-loader.nix];};

    testScript = ''
      machine.wait_for_unit("multi-user.target")

      with subtest("both ELF interpreter directories exist, owned by root and writable by no one else"):
          assert machine.succeed("stat -c %U:%G:%a /lib /lib64").split() == ["root:root:755", "root:root:755"]

      with subtest("a hermetic action tool runs under the shipped binds"):
          assert machine.succeed("${sandbox shipped "${hermeticGawk}/bin/gawk 'BEGIN { print 6 * 7 }'"}").strip() == "42"

      probe_command = "${sandbox shipped ""}"
      for name, declared, expected in [
          ("absent library path", "ORIGINAL_MARKER=declared", ["ORIGINAL_MARKER=declared"]),
          ("empty environment", "", []),
          ("empty library path", "ORIGINAL_MARKER=declared LD_LIBRARY_PATH=", ["ORIGINAL_MARKER=declared", "LD_LIBRARY_PATH="]),
          ("declared library path", "ORIGINAL_MARKER=declared LD_LIBRARY_PATH='::/declared path::tail:'", ["ORIGINAL_MARKER=declared", "LD_LIBRARY_PATH=::/declared path::tail:"]),
          ("declared nix library path", "ORIGINAL_MARKER=declared NIX_LD_LIBRARY_PATH=", ["ORIGINAL_MARKER=declared", "NIX_LD_LIBRARY_PATH="]),
      ]:
          with subtest(f"the exact declared environment stays writable: {name}"):
              actual = machine.succeed(probe_command + declared + " ${envProbe}/bin/env-probe").splitlines()
              assert actual == expected, actual

      with subtest("a program resolves non-glibc libraries under the shipped binds"):
          assert "Zstandard" in machine.succeed("${sandbox shipped "${fhsZstd}/bin/zstd --version"}")

      with subtest("an empty library profile cannot resolve zstd without RUNPATH"):
          status, output = machine.execute("${sandbox shipped "NIX_LD_LIBRARY_PATH= ${fhsZstd}/bin/zstd --version"} 2>&1")
          assert status != 0 and "libzstd.so.1" in output, output

      with subtest("a glibc program runs in the sandbox under the shipped binds"):
          assert "Hello, world!" in machine.succeed("${sandbox shipped "${fhsHello}/bin/hello"}")

      with subtest("without the interpreter directories it cannot start"):
          machine.fail("${sandbox withoutLoader "${fhsHello}/bin/hello"}")
    '';
  }
