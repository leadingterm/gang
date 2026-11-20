{pkgs}: let
  shipped = (builtins.fromTOML (builtins.readFile ../../../configs/gang.aws.toml)).sandbox.bwrap;
  bundle = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  # OpenSSL verifies an actual Mozilla root without network or clock dependence.
  root = pkgs.runCommand "gang-action-root.pem" {nativeBuildInputs = [pkgs.openssl];} ''
    openssl x509 -in ${bundle} -out "$out"
  '';
  # Go's native Linux root loader searches conventional /etc paths, not a Nix store default.
  nativeRoots = pkgs.runCommand "gang-native-roots" {nativeBuildInputs = [pkgs.go];} ''
    export HOME="$TMPDIR" GOCACHE="$TMPDIR/go-cache" GO111MODULE=off CGO_ENABLED=0
    cat > main.go <<'EOF'
    package main

    import (
        "crypto/x509"
        "os"
    )

    func main() {
        roots, err := x509.SystemCertPool()
        if err != nil || len(roots.Subjects()) == 0 {
            os.Exit(1)
        }
    }
    EOF
    go build -o "$out" main.go
  '';
  binds = pkgs.lib.concatMapStringsSep " " (path: "--ro-bind ${path} ${path}") shipped.readonly_bind_paths;
  mounts = mounted: pkgs.lib.concatStringsSep " " (pkgs.lib.mapAttrsToList (destination: source: "--ro-bind ${source} ${destination}") mounted);
  withoutCa = pkgs.lib.filterAttrs (destination: _: !(pkgs.lib.hasPrefix "/etc/" destination)) shipped.readonly_bind_mounts;
  sandbox = mounted: command: "${pkgs.util-linux}/bin/runuser -u action -- ${pkgs.coreutils}/bin/env -i ${pkgs.bubblewrap}/bin/bwrap --unshare-all --die-with-parent --new-session --proc /proc --dev /dev --tmpfs /tmp --clearenv ${binds} ${mounts mounted} -- ${command}";
  check = pkgs.writeShellScript "check-action-roots" ''
    set -eu
    for file in /etc/ssl/certs/ca-certificates.crt /etc/ssl/cert.pem /etc/pki/tls/certs/ca-bundle.crt; do
      ${pkgs.diffutils}/bin/cmp "$file" ${bundle}
      if (echo changed > "$file") 2>/dev/null; then exit 1; fi
      ${pkgs.gawk}/bin/awk -v path="$file" '$5 == path && $6 ~ /^ro(,|$)/ { found = 1 } END { exit !found }' /proc/self/mountinfo
    done
    ${pkgs.openssl}/bin/openssl verify -no-CApath -no-CAstore -no_check_time -CAfile /etc/ssl/certs/ca-certificates.crt ${root}
  '';
in
  pkgs.testers.runNixOSTest {
    name = "action-environment";
    globalTimeout = 300;

    nodeDefaults.virtualisation = {
      memorySize = 1024;
      cores = 1;
      useNixStoreImage = true;
      mountHostNixStore = false;
      sharedDirectories = pkgs.lib.mkForce {};
    };

    nodes.machine = {
      imports = [../glibc-loader.nix ../action-tools.nix ../action-environment.nix];
      users.users.action = {
        isNormalUser = true;
        extraGroups = [];
      };
    };

    testScript = ''
      machine.wait_for_unit("multi-user.target")
      machine.succeed("mkdir -p /etc/gang /etc/credentials; touch /etc/gang/gang.toml /etc/credentials/secret")

      with subtest("shipped mounts expose the pinned public roots read-only to a nonroot action"):
          machine.succeed("${sandbox shipped.readonly_bind_mounts "${check}"}")
          machine.succeed("${sandbox shipped.readonly_bind_mounts "${nativeRoots}"}")

      with subtest("the empty-root action sees only the intended certificate paths under /etc"):
          entries = machine.succeed("${sandbox shipped.readonly_bind_mounts "${pkgs.findutils}/bin/find /etc -mindepth 1 -print"}").splitlines()
          assert sorted(entries) == sorted([
              "/etc/pki", "/etc/pki/tls", "/etc/pki/tls/certs", "/etc/pki/tls/certs/ca-bundle.crt",
              "/etc/ssl", "/etc/ssl/cert.pem", "/etc/ssl/certs", "/etc/ssl/certs/ca-certificates.crt",
          ]), entries

      with subtest("without CA mounts the conventional trust file cannot verify the same root"):
          machine.fail("${sandbox withoutCa "${pkgs.openssl}/bin/openssl verify -no-CApath -no-CAstore -no_check_time -CAfile /etc/ssl/certs/ca-certificates.crt ${root}"}")
          machine.fail("${sandbox withoutCa "${nativeRoots}"}")
    '';
  }
