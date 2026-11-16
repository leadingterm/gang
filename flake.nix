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
          if ${pkgs.binutils}/bin/readelf -lW $out/bin/gang | grep -q INTERP \
            || ${pkgs.binutils}/bin/readelf -dW $out/bin/gang | grep -q NEEDED; then
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
    in {
      gang-scratch = import ./infra/nixos/tests/gang-scratch.nix {inherit pkgs;};
      gang-config = import ./infra/nixos/tests/gang-config.nix {inherit pkgs;};
      glibc-loader = import ./infra/nixos/tests/glibc-loader.nix {inherit pkgs;};
      action-tools = import ./infra/nixos/tests/action-tools.nix {inherit pkgs;};
    });

    packages = forLinuxSystems (system: let
      pkgs = pkgsFor system;
    in {
      # Bazel is the only builder of gang (statically linked, dashboard embedded): the images carry exactly the binary it built, taken unchanged from the gang-binaries input.
      gang = gangPackage pkgs "${gang-binaries}/${system}/gang";
      image = self.lib.image {
        inherit system;
        binary = "${gang-binaries}/${system}/gang";
      };
      # Lints every Cargo target of the workspace for macOS (tools/darwin-check.sh), which no Linux build compiles.
      # Nothing is linked, so the target's C compiler and archiver only write empty objects and archives.
      darwin-check = let
        rust = (rust-overlay.lib.mkRustBin {} pkgs).stable.${rustVersion}.minimal.override {
          extensions = ["clippy"];
          targets = ["aarch64-apple-darwin"];
        };
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
          runtimeEnv = {
            CC_aarch64_apple_darwin = "${stubCc}";
            CXX_aarch64_apple_darwin = "${stubCc}";
            AR_aarch64_apple_darwin = "${stubAr}";
          };
          text = builtins.readFile ./tools/darwin-check.sh;
        };
    });

    nixosConfigurations = lib.mapAttrs' (arch: system: lib.nameValuePair "gang-${arch}" (mkNixos system self.packages.${system}.gang [])) nixosTargets;

    devShells = forAllSystems (system: let
      pkgs = pkgsFor system;
      actionTools = [pkgs.bash pkgs.coreutils pkgs.diffutils pkgs.gawk pkgs.gnugrep pkgs.gnused pkgs.findutils];
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
          --set FONTCONFIG_FILE ${pkgs.makeFontsConf {fontDirectories = [pkgs.dejavu_fonts];}}
      '';
      deployTools = [pkgs.awscli2 pkgs.coreutils pkgs.gh pkgs.git pkgs.jq pkgs.opentofu];
    in {
      default = pkgs.mkShell ({
          # The e2e harness uses pinned action tools without the host toolchain.
          GANG_E2E_ACTION_SHELL = "${pkgs.bash}/bin/bash";
          GANG_E2E_ACTION_PATH = lib.makeBinPath actionTools;
          # The same tools in one directory, which the harness's bwrap sandbox binds at /bin and /usr/bin as the worker image binds its action tools.
          GANG_E2E_ACTION_BIN = "${pkgs.buildEnv {
            name = "gang-e2e-action-tools";
            paths = actionTools;
            pathsToLink = ["/bin"];
          }}/bin";
          packages =
            [
              bazel
              pkgs.actionlint
              pkgs.alejandra
              pkgs.shellcheck
            ]
            ++ deployTools
            ++ lib.optional pkgs.stdenv.hostPlatform.isLinux pkgs.bubblewrap;
        }
        // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          GANG_E2E_BWRAP = "${pkgs.bubblewrap}/bin/bwrap";
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
