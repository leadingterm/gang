{pkgs}: let
  rootfs = import ../action-rootfs {inherit pkgs;};
  interpreter =
    if pkgs.stdenv.hostPlatform.isx86_64
    then "/lib64/ld-linux-x86-64.so.2"
    else "/lib/ld-linux-aarch64.so.1";
  # This fixture uses the rootfs loader and libc, rather than a Nix-store runtime.
  probe = pkgs.runCommandCC "gang-rootfs-probe" {nativeBuildInputs = [pkgs.patchelf];} ''
    cat > probe.c <<'EOF'
    #include <grp.h>
    #include <netdb.h>
    #include <pwd.h>
    #include <stdio.h>
    #include <string.h>
    #include <sys/auxv.h>
    #include <unistd.h>
    extern char **environ;
    int main(int argc, char **argv) {
      if (argc > 1 && strcmp(argv[1], "identity") == 0) {
        struct passwd *pw = getpwuid(getuid());
        struct group *gr = getgrgid(getgid());
        struct addrinfo *addresses = NULL;
        int status = getaddrinfo("localhost", NULL, NULL, &addresses);
        if (status || !addresses || getuid() != 1000 || getgid() != 1000 ||
            !pw || strcmp(pw->pw_name, "builder") || !gr || strcmp(gr->gr_name, "builder")) return 1;
        freeaddrinfo(addresses);
        return 0;
      }
      if (getauxval(AT_PAGESZ) != (unsigned long)sysconf(_SC_PAGESIZE) || !getauxval(AT_EXECFN)) return 1;
      for (char **p = environ; *p; ++p) {
        puts(*p);
        volatile char *separator = strchr(*p, '=');
        if (separator) { *separator = '\0'; *separator = '='; }
      }
      return 0;
    }
    EOF
    $CC -O2 -Wall -Wextra -Werror probe.c -o "$out"
    patchelf --set-interpreter ${interpreter} --remove-rpath "$out"
  '';
  # Go's static Linux trust loader discovers roots only through conventional paths.
  roots = pkgs.runCommand "gang-rootfs-roots" {nativeBuildInputs = [pkgs.go];} ''
    export HOME="$TMPDIR" GOCACHE="$TMPDIR/cache" GO111MODULE=off CGO_ENABLED=0
    cat > main.go <<'EOF'
    package main
    import ("crypto/x509"; "os")
    func main() {
      roots, err := x509.SystemCertPool()
      if err != nil || len(roots.Subjects()) == 0 { os.Exit(1) }
    }
    EOF
    go build -o "$out" main.go
  '';
  script = pkgs.writeScript "gang-rootfs-script" ''
    #!/usr/bin/env bash
    set -eu
    /bin/sh -c 'exit 0'
    /bin/bash -c 'exit 0'
    printf '40 2\n' | gawk '{print $1 + $2}'
    printf 'before\n' > /tmp/awk-input
    gawk -i inplace '{print "changed"}' /tmp/awk-input
    cat /tmp/awk-input
  '';
  sandbox = extra: command: "${pkgs.bubblewrap}/bin/bwrap --unshare-all --unshare-user --uid 1000 --gid 1000 --die-with-parent --tmpfs / --ro-bind ${rootfs}/root/usr /usr --ro-bind ${rootfs}/root/etc /etc --symlink usr/bin /bin --symlink usr/sbin /sbin --symlink usr/lib /lib ${pkgs.lib.optionalString pkgs.stdenv.hostPlatform.isx86_64 "--symlink usr/lib64 /lib64"} --proc /proc --dev /dev --tmpfs /tmp --ro-bind ${probe} /probe --ro-bind ${roots} /roots --ro-bind ${script} /script --clearenv ${extra} --remount-ro / -- ${command}";
in
  pkgs.testers.runNixOSTest {
    name = "action-rootfs";
    globalTimeout = 300;
    nodeDefaults.virtualisation = {
      memorySize = 1024;
      cores = 1;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
    };
    nodes.machine = {};
    testScript = ''
      machine.wait_for_unit("multi-user.target")
      with subtest("FHS script and gawk work with strict and absent PATH"):
          assert machine.succeed("${sandbox "--setenv PATH /bin:/usr/bin" "/script"}").strip() == "42\nchanged"
          assert machine.succeed("${sandbox "" "/script"}").strip() == "42\nchanged"
      with subtest("the loader preserves the exact writable environment"):
          assert machine.succeed("${sandbox "" "/usr/bin/env -i ORIGINAL_MARKER=declared /probe"}").splitlines() == ["ORIGINAL_MARKER=declared"]
          for value in ["", "::/declared path::tail:"]:
              output = machine.succeed(f"${sandbox "" "/usr/bin/env -i ORIGINAL_MARKER=declared LD_LIBRARY_PATH='{value}' /probe"}")
              assert output.splitlines() == ["ORIGINAL_MARKER=declared", f"LD_LIBRARY_PATH={value}"]
      with subtest("fixed uid, passwd, group and localhost work without host NSS"):
          machine.succeed("${sandbox "" "/probe identity"}")
      with subtest("native trust roots are present and read-only"):
          machine.succeed("${sandbox "" "/roots"}")
          machine.fail("${sandbox "--tmpfs /etc/ssl --tmpfs /etc/pki" "/roots"}")
          machine.fail("${sandbox "" "/bin/sh -c 'echo changed > /etc/ssl/certs/ca-certificates.crt'"}")
      with subtest("dependency-only tools and host state are absent"):
          machine.succeed("${sandbox "" "/bin/sh -c 'test ! -e /usr/bin/which && test ! -e /usr/bin/run-parts && test ! -e /nix/store && test ! -e /etc/gang'"}")
    '';
  }
