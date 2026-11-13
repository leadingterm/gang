# gang-binaries

Flake input `gang-binaries` (`flake = false`), consumed by `packages.<system>.gang`.

This directory is a placeholder. It holds no binary.

Layout: `<system>/gang`, where `<system>` is `x86_64-linux` or `aarch64-linux`. Each `gang` is the static musl binary built by Bazel: the release asset `gang-x86_64-unknown-linux-musl` or `gang-aarch64-unknown-linux-musl` of a Gang release (checked against its `SHA256SUMS`), or a local build:

    bazel build -c opt //:gang
    # binary: bazel-bin/crates/gang-main/gang

Copy the binary to `<system>/gang` (for example `x86_64-linux/gang`), then build the raw disk images against this directory. Each output contains `<out>/gang-<role>.raw` and `nix-support/image-info.json`; the build fails unless the image holds the system closure byte for byte and boots under UEFI firmware, which needs the `kvm` system feature (KVM, or qemu's TCG fallback on a builder that declares `extra-system-features = kvm`):

    nix build --override-input gang-binaries path:<absolute dir> .#packages.<system>.controller-image
    nix build --override-input gang-binaries path:<absolute dir> .#packages.<system>.worker-image

Staged binaries are git-ignored (see `.gitignore`).
