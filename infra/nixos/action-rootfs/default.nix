{pkgs}: let
  inherit (pkgs) lib;
  rootPackages = ["bash" "coreutils" "findutils" "grep" "sed" "gawk" "diffutils" "tar" "gzip" "zip" "unzip" "patch" "bzip2" "xz-utils"];
  tree = arch: let
    debs = lib.flatten (import (./. + "/debs-${arch}.nix") {inherit (pkgs) fetchurl;});
  in
    pkgs.runCommand "gang-action-rootfs-tree-${arch}" {
      nativeBuildInputs = [pkgs.dpkg pkgs.findutils];
      allowedReferences = [];
    } ''
      mkdir -p "$out/usr/bin" "$out/usr/sbin" "$out/usr/lib" "$out/etc/ssl/certs"
      for deb in ${lib.escapeShellArgs debs}; do
        rm -rf unpack
        mkdir unpack
        dpkg-deb -x "$deb" unpack
        package=$(dpkg-deb -f "$deb" Package)
        case " ${lib.concatStringsSep " " rootPackages} " in
          *" $package "*)
            if [ -d unpack/bin ] && [ ! -L unpack/bin ] && [ -n "$(find unpack/bin -type f -perm /111 -print -quit)" ]; then
              echo "unmerged /bin executable in $package" >&2
              exit 1
            fi
            if [ -d unpack/usr/bin ]; then cp -a unpack/usr/bin/. "$out/usr/bin/"; fi
            if [ -d unpack/usr/sbin ]; then cp -a unpack/usr/sbin/. "$out/usr/sbin/"; fi
            if [ -d unpack/usr/share ]; then
              mkdir -p "$out/usr/share"
              cp -a unpack/usr/share/. "$out/usr/share/"
            fi
            ;;
        esac
        # Library packages also carry runtime data such as gconv configuration.
        for dir in usr/lib usr/lib64 usr/libexec; do
          if [ -d "unpack/$dir" ]; then
            mkdir -p "$out/$dir"
            cp -a "unpack/$dir/." "$out/$dir/"
          fi
        done
      done
      rm -rf "$out/usr/share/doc" "$out/usr/share/man" "$out/usr/share/info" "$out/usr/share/locale"
      ln -s bash "$out/usr/bin/sh"
      ln -s gawk "$out/usr/bin/awk"
      ln -s usr/bin "$out/bin"
      ln -s usr/sbin "$out/sbin"
      ln -s usr/lib "$out/lib"
      ${lib.optionalString (arch == "x86_64") ''ln -s usr/lib64 "$out/lib64"''}
      printf 'builder:x:1000:1000:Sandbox user:/:/bin/sh\nnobody:x:65534:65534::/:/usr/sbin/nologin\n' > "$out/etc/passwd"
      printf 'builder:x:1000:\nnogroup:x:65534:\n' > "$out/etc/group"
      printf 'passwd: files\ngroup: files\nhosts: files\n' > "$out/etc/nsswitch.conf"
      printf '127.0.0.1 localhost\n::1 localhost\n' > "$out/etc/hosts"
      cp -L ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt "$out/etc/ssl/certs/ca-certificates.crt"
      ln -s certs/ca-certificates.crt "$out/etc/ssl/cert.pem"
      mkdir -p "$out/etc/pki/tls/certs"
      ln -s ../../../ssl/certs/ca-certificates.crt "$out/etc/pki/tls/certs/ca-bundle.crt"
    '';
  trees = {
    x86_64 = tree "x86_64";
    aarch64 = tree "aarch64";
  };
  identity =
    pkgs.runCommand "gang-action-rootfs-identity" {
      nativeBuildInputs = [pkgs.nix];
      allowedReferences = [];
    } ''
      export NIX_CONFIG='experimental-features = nix-command'
      {
        printf 'x86_64:%s\n' "$(nix hash path --type sha256 --base32 ${trees.x86_64})"
        printf 'aarch64:%s\n' "$(nix hash path --type sha256 --base32 ${trees.aarch64})"
      } | sha256sum | cut -d ' ' -f 1 > "$out"
    '';
  arch = pkgs.stdenv.hostPlatform.parsed.cpu.name;
in
  pkgs.runCommand "gang-action-rootfs-${arch}" {allowedReferences = [trees.${arch}];} ''
    mkdir -p "$out"
    ln -s ${trees.${arch}} "$out/root"
    cp ${identity} "$out/identity"
  ''
