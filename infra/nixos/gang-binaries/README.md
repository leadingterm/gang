# gang-binaries

Flake input `gang-binaries` (`flake = false`), consumed by `packages.<system>.gang`.

This directory is a placeholder. It holds no binary.

Layout: `<system>/gang`, where `<system>` is `x86_64-linux` or `aarch64-linux`. Each `gang` is the static musl binary built by Bazel on a host of that architecture:

    bazel build -c opt //:gang
    # binary: bazel-bin/crates/gang-main/gang

Copy the binary to `<system>/gang` (for example `x86_64-linux/gang`), then build the images against this directory:

    nix build --override-input gang-binaries path:<absolute dir> .#packages.<system>.controller-image
    nix build --override-input gang-binaries path:<absolute dir> .#packages.<system>.worker-image

Staged binaries are git-ignored (see `.gitignore`).
