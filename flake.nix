{
  description = "Gang development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    gang-binaries = {
      url = "path:./infra/nixos/gang-binaries";
      flake = false;
    };
  };

  outputs = {
    self,
    nixpkgs,
    gang-binaries,
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

    # NixOS system architectures, keyed by the suffix used in nixosConfigurations names.
    nixosTargets = {
      x86_64 = "x86_64-linux";
      aarch64 = "aarch64-linux";
    };
    roles = ["controller" "worker"];

    # Builds a NixOS system for a role; `extraModules` adds to the role's config.
    mkNixos = extraModules: role: system:
      nixpkgs.lib.nixosSystem {
        inherit system;
        specialArgs = {gang = self.packages.${system}.gang;};
        modules =
          [
            ./infra/nixos/common.nix
            (./infra/nixos + "/${role}.nix")
          ]
          ++ extraModules;
      };

    # The EC2 disk image is the same system with the image builder added.
    ec2ImageModule = ./infra/nixos/ec2-image.nix;
  in {
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
      binary = "${gang-binaries}/${system}/gang";
      # Raw, so the disk compresses well and EBS direct APIs or VM Import take it unconverted.
      image = role:
        import ./infra/nixos/verified-image.nix {
          inherit pkgs;
          nixos = mkNixos [ec2ImageModule {image.repart.name = "gang-${role}";}] role system;
        };
    in {
      # Bazel is the only builder of gang (statically linked, dashboard embedded): the images carry exactly the binary it built, taken unchanged from the gang-binaries input.
      gang = pkgs.stdenvNoCC.mkDerivation {
        name = "gang";
        dontUnpack = true;
        # The bytes Bazel produced; no strip or patchelf.
        dontFixup = true;
        installPhase = ''
          runHook preInstall
          if [ ! -e ${binary} ]; then
            echo "gang-binaries has no ${system}/gang: stage the Bazel-built binary and pass --override-input gang-binaries path:<absolute dir> (see infra/nixos/gang-binaries/README.md)" >&2
            exit 1
          fi
          install -D -m 0755 ${binary} $out/bin/gang
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
      controller-image = image "controller";
      worker-image = image "worker";
    });

    nixosConfigurations = lib.listToAttrs (lib.concatMap (
        role:
          lib.mapAttrsToList (arch: system: {
            name = "${role}-${arch}";
            value = mkNixos [] role system;
          })
          nixosTargets
      )
      roles);

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
        });
      deploy = pkgs.mkShell {packages = deployTools;};
      # The release workflow's image publication: S3 uploads and zstd.
      release = pkgs.mkShell {packages = [pkgs.awscli2 pkgs.coreutils pkgs.zstd];};
    });
  };
}
