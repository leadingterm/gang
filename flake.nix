{
  description = "Gang development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    gang-binaries = {
      url = "path:./infra/nixos/gang-binaries";
      flake = false;
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    gang-binaries,
    rust-overlay,
    ...
  }: let
    inherit (nixpkgs) lib;
    systems = [
      "x86_64-linux"
      "aarch64-linux"
      "aarch64-darwin"
    ];
    linuxSystems = lib.filter (lib.hasSuffix "-linux") systems;
    forAllSystems = lib.genAttrs systems;
    forLinuxSystems = lib.genAttrs linuxSystems;
    pkgsFor = system: import nixpkgs {inherit system;};
    # Bazel's Rust version (MODULE.bazel's RUST_VERSION): the darwin check compiles with the toolchain Bazel uses.
    rustVersion =
      lib.head (lib.findFirst (match: match != null) (throw "MODULE.bazel sets no RUST_VERSION")
        (map (builtins.match "RUST_VERSION = \"(.+)\"") (lib.splitString "\n" (builtins.readFile ./MODULE.bazel))));
    opentofuVersion =
      lib.head (lib.findFirst (match: match != null) (throw "MODULE.bazel sets no OPENTOFU_VERSION")
        (map (builtins.match "OPENTOFU_VERSION = \"(.+)\"") (lib.splitString "\n" (builtins.readFile ./MODULE.bazel))));

    # Lints every Cargo target of the workspace for macOS (tools/darwin-check.sh), which no Linux build compiles.
    darwinCheck = pkgs: let
      rust = (rust-overlay.lib.mkRustBin {} pkgs).stable.${rustVersion}.minimal.override {
        extensions = ["clippy"];
        targets = ["aarch64-apple-darwin"];
      };
      # Cross-checking on Linux links nothing; native macOS uses the real C toolchain.
      stubCc = pkgs.writeShellScript "darwin-stub-cc" ''
        out=
        while [ "$#" -gt 0 ]; do
          if [ "$1" = -o ]; then out=$2; fi
          shift
        done
        if [ -n "$out" ]; then : >"$out"; fi
      '';
      stubAr = pkgs.writeShellScript "darwin-stub-ar" ''
        if [ -n "''${2:-}" ]; then : >>"$2"; fi
      '';
    in
      pkgs.writeShellApplication {
        name = "darwin-check";
        runtimeInputs = [rust pkgs.coreutils pkgs.rsync pkgs.stdenv.cc];
        runtimeEnv = lib.optionalAttrs (!pkgs.stdenv.hostPlatform.isDarwin) {
          CC_aarch64_apple_darwin = "${stubCc}";
          CXX_aarch64_apple_darwin = "${stubCc}";
          AR_aarch64_apple_darwin = "${stubAr}";
        };
        text = builtins.readFile ./tools/darwin-check.sh;
      };

    # NixOS system architectures, keyed by the suffix used in nixosConfigurations names.
    nixosTargets = {
      x86_64 = "x86_64-linux";
      aarch64 = "aarch64-linux";
    };
    gangPackage = pkgs: binary:
      pkgs.stdenvNoCC.mkDerivation {
        name = "gang";
        dontUnpack = true;
        # The bytes Bazel produced; no strip or patchelf.
        dontFixup = true;
        installPhase = ''
          runHook preInstall
          if [ ! -e ${binary} ]; then
            echo "gang-binaries has no ${pkgs.stdenv.hostPlatform.system}/gang: stage the Bazel-built binary and pass --override-input gang-binaries path:<absolute dir> (see infra/nixos/gang-binaries/README.md)" >&2
            exit 1
          fi
          install -D -m 0755 ${binary} $out/bin/gang
          # FSL redistribution requires the terms to travel with the AMIs.
          install -D -m 0644 ${./NOTICE} $out/share/doc/gang/NOTICE
          runHook postInstall
        '';
        doInstallCheck = true;
        installCheckPhase = ''
          runHook preInstallCheck
          if ! ${pkgs.binutils}/bin/readelf -lW "$out/bin/gang" > program-headers \
            || ! ${pkgs.binutils}/bin/readelf -dW "$out/bin/gang" > dynamic-section; then
            echo 'gang is not an ELF binary' >&2
            exit 1
          fi
          if grep -q INTERP program-headers || grep -q NEEDED dynamic-section; then
            echo "gang must be static: it has an ELF interpreter or a needed shared library" >&2
            exit 1
          fi
          runHook postInstallCheck
        '';
        meta.mainProgram = "gang";
      };

    # The one NixOS system of an architecture: both roles, the EC2 user data starts one at boot; `extraModules` adds to it.
    mkNixos = system: gang: extraModules:
      nixpkgs.lib.nixosSystem {
        inherit system;
        specialArgs = {inherit gang;};
        modules = [./infra/nixos/system.nix] ++ extraModules;
      };

    # The EC2 disk image is the same system with the image builder added.
    ec2ImageModule = ./infra/nixos/ec2-image.nix;
  in {
    # The verified raw EC2 disk image of Gang's NixOS system for a static gang binary: one image per architecture, both roles (raw, so the disk compresses well and EBS direct APIs or VM Import take it unconverted); `modules` are NixOS modules added to Gang's system, and the result passes the same checks.
    lib.image = {
      system,
      binary,
      modules ? [],
    }: let
      pkgs = pkgsFor system;
    in
      import ./infra/nixos/verified-image.nix {
        inherit pkgs;
        nixos = mkNixos system (gangPackage pkgs binary) ([ec2ImageModule {image.repart.name = "gang";}] ++ modules);
      };

    formatter = forAllSystems (system: (pkgsFor system).alejandra);

    checks = lib.genAttrs ["x86_64-linux"] (system: let
      pkgs = pkgsFor system;
      testsDir = ./infra/nixos/tests;
      tests = lib.filterAttrs (file: type: type == "regular" && lib.hasSuffix ".nix" file) (builtins.readDir testsDir);
    in
      lib.mapAttrs' (file: _: lib.nameValuePair (lib.removeSuffix ".nix" file) (import (testsDir + "/${file}") {inherit pkgs;})) tests);

    packages =
      forLinuxSystems (system: let
        pkgs = pkgsFor system;
      in {
        # Bazel is the only builder of gang (statically linked, dashboard embedded): the images carry exactly the binary it built, taken unchanged from the gang-binaries input.
        gang = gangPackage pkgs "${gang-binaries}/${system}/gang";
        action-rootfs = import ./infra/nixos/action-rootfs {inherit pkgs;};
        action-rootfs-e2e = self.packages.${system}.action-rootfs;
        image = self.lib.image {
          inherit system;
          binary = "${gang-binaries}/${system}/gang";
        };
        darwin-check = darwinCheck pkgs;
      })
      // {
        aarch64-darwin.darwin-check = darwinCheck (pkgsFor "aarch64-darwin");
      };

    nixosConfigurations = lib.mapAttrs' (arch: system: lib.nameValuePair "gang-${arch}" (mkNixos system self.packages.${system}.gang [])) nixosTargets;

    devShells = forAllSystems (system: let
      pkgs = pkgsFor system;
      opentofu = pkgs.opentofu.overrideAttrs (previous: {
        version = opentofuVersion;
        src = pkgs.fetchFromGitHub {
          owner = "opentofu";
          repo = "opentofu";
          tag = "v${opentofuVersion}";
          hash = "sha256-FC2CQO7WnHRy02CHypa2a5grZ32ulobNUyg5VG3rWQI=";
        };
        meta =
          previous.meta
          // {
            changelog = "https://github.com/opentofu/opentofu/blob/v${opentofuVersion}/CHANGELOG.md";
          };
      });
      actionTools = [pkgs.bash pkgs.coreutils pkgs.diffutils pkgs.gawk pkgs.gnugrep pkgs.gnused pkgs.findutils] ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [pkgs.util-linux];
      # macOS builds use Apple's toolchain through /usr/bin; Nix tools come first to keep bash and coreutils pinned.
      actionPath = lib.concatStringsSep ":" ([(lib.makeBinPath actionTools)] ++ lib.optional pkgs.stdenv.hostPlatform.isDarwin "/usr/bin");
      bazelrc = pkgs.writeText "gang.bazelrc" ''
        # Actions run with bash and coreutils pinned by flake.lock, so a build is the same on NixOS, Linux and macOS.
        build --shell_executable=${pkgs.bash}/bin/bash
        build --action_env=PATH=${actionPath}
        build --host_action_env=PATH=${actionPath}
        test --test_env=PATH=${actionPath}
      '';
      bazelWrapper = name:
        pkgs.writeShellScriptBin name ''
          exec ${pkgs.bazelisk}/bin/bazelisk --bazelrc=${bazelrc} "$@"
        '';
      bazel = pkgs.symlinkJoin {
        name = "bazel-bazelisk";
        paths = [(bazelWrapper "bazel") (bazelWrapper "bazelisk")];
      };
      # The identity scenario's browser. Chromium aborts when fontconfig finds no fonts, so it gets a pinned configuration.
      e2eChromium = pkgs.runCommand "gang-e2e-chromium" {nativeBuildInputs = [pkgs.makeWrapper];} ''
        makeWrapper ${pkgs.chromium}/bin/chromium $out/bin/chromium \
          --set FONTCONFIG_FILE ${pkgs.writeText "gang-fonts.conf" ''
          <fontconfig><dir>${pkgs.dejavu_fonts}/share/fonts/truetype</dir><cachedir prefix='xdg'>fontconfig</cachedir></fontconfig>
        ''}
        ln -s ${pkgs.chromium}/bin/chromium $out/bin/chromium-bazel
        mkdir -p $out/share/fonts
        ln -s ${pkgs.dejavu_fonts}/share/fonts/truetype/DejaVuSans.ttf $out/share/fonts/DejaVuSans.ttf
        ln -s ${pkgs.dejavu_fonts}/share/fonts/truetype/DejaVuSansMono.ttf $out/share/fonts/DejaVuSansMono.ttf
      '';
      deployTools = [pkgs.awscli2 pkgs.coreutils pkgs.curl pkgs.gh pkgs.git pkgs.jq pkgs.openssl opentofu];
    in {
      default = pkgs.mkShell ({
          # The e2e harness uses pinned action tools without the host toolchain.
          GANG_E2E_ACTION_SHELL = "${pkgs.bash}/bin/bash";
          GANG_E2E_ACTION_PATH = lib.makeBinPath actionTools;
          GANG_IMPORT_TEST_TOOLS = "${pkgs.buildEnv {
            name = "gang-import-test-tools";
            paths = [pkgs.jq pkgs.zstd];
            pathsToLink = ["/bin"];
          }}/bin";
          packages =
            [
              bazel
              pkgs.actionlint
              pkgs.alejandra
              pkgs.findutils
              pkgs.gnutar
              pkgs.shellcheck
              pkgs.yq-go
              pkgs.zstd
            ]
            ++ deployTools
            ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [pkgs.bubblewrap pkgs.slirp4netns pkgs.nftables pkgs.util-linux pkgs.iproute2];
        }
        // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          GANG_E2E_ACTION_ROOTFS = "${self.packages.${system}.action-rootfs-e2e}";
          GANG_E2E_BWRAP = "${pkgs.bubblewrap}/bin/bwrap";
          GANG_E2E_SLIRP4NETNS = "${pkgs.slirp4netns}/bin/slirp4netns";
          GANG_E2E_NFT = "${pkgs.nftables}/bin/nft";
          GANG_E2E_NSENTER = "${pkgs.util-linux}/bin/nsenter";
          GANG_E2E_IP = "${pkgs.iproute2}/bin/ip";
          GANG_E2E_UNSHARE = "${pkgs.util-linux}/bin/unshare";
          # The browser of the identity scenario's device approval, which observes what only a browser enforces: the dashboard's Content-Security-Policy.
          # Pinned by flake.lock's nixpkgs, like bubblewrap; never taken from the host PATH.
          GANG_E2E_CHROMIUM = "${e2eChromium}/bin/chromium";
        });
      deploy = pkgs.mkShell {packages = deployTools;};
      # The release workflow's public AMI publication: EBS direct upload, EC2 and zstd.
      release = pkgs.mkShell {packages = [pkgs.awscli2 pkgs.coldsnap pkgs.coreutils pkgs.jq pkgs.zstd];};
    });
  };
}
