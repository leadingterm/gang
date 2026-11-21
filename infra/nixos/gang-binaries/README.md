# gang-binaries

Flake input `gang-binaries` (`flake = false`), consumed by `packages.<system>.gang`.

This directory is a placeholder. It holds no binary.

Layout: `<system>/gang`, where `<system>` is `x86_64-linux` or `aarch64-linux`. Each `gang` is the static musl binary built by Bazel: the release asset `gang-x86_64-unknown-linux-musl` or `gang-aarch64-unknown-linux-musl` of a Gang release (checked against its `SHA256SUMS`), or a local build:

    nix develop -c bazel build --config=local -c opt //:gang
    # binary: bazel-bin/crates/gang-main/gang

The VM check also requires Bazel's `action_network` at `<system>/action_network` alongside `gang`. `tools/lint.sh --nix-vm` builds and stages both in a temporary `gang-binaries` override automatically.

Package admission requires successful inspection of readable ELF program headers and the dynamic section, and rejects binaries with an ELF interpreter or needed shared libraries.

Copy the binary to `<system>/gang` (for example `x86_64-linux/gang`), then build the raw disk image against this directory. The output contains `<out>/gang.raw` and `nix-support/image-info.json`; the build fails unless the image holds the system closure byte for byte and boots three times under UEFI firmware (controller, controller with the state disk, and worker), which needs the `kvm` system feature (KVM, or qemu's TCG fallback on a builder that declares `extra-system-features = kvm`):

    nix build --override-input gang-binaries path:<absolute dir> .#packages.<system>.image

`gang.lib.image { system, binary, modules ? [] }` builds the image for a given binary and optional NixOS modules.

Staged binaries are git-ignored (see `.gitignore`).
