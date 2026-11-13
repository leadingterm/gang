{
  lib,
  pkgs,
  ...
}: {
  # Dynamically linked glibc programs that Bazel downloads (rules_rust's rustc and cargo, the
  # toolchains of other rules, prebuilt tools) name the standard ELF interpreter:
  # /lib64/ld-linux-x86-64.so.2 on x86-64, /lib/ld-linux-aarch64.so.1 on aarch64. nix-ld serves
  # that path with a loader that runs the program with NixOS's glibc and the libraries of
  # programs.nix-ld.libraries (default: the NixOS module's list; a deployment that needs others sets
  # the option in a module of its own), linked under /run/current-system/sw/share/nix-ld/lib. The
  # loader reads that directory without NIX_LD variables, so sandboxed actions, which see none of
  # the worker's environment, need none: the sandbox binds /lib, /lib64 and /run/current-system/sw
  # read-only.
  programs.nix-ld.enable = true;

  # One gang.toml serves the workers of every architecture, so both interpreter directories exist
  # on every worker. NixOS creates this architecture's (environment.ldso); the other one stays
  # empty.
  systemd.tmpfiles.rules =
    map (directory: "d /${directory} 0755 root root -")
    (lib.remove pkgs.stdenv.hostPlatform.libDir ["lib" "lib64"]);
}
