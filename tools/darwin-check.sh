#!/usr/bin/env bash
# Lints the workspace for aarch64-apple-darwin, so a Linux host compiles the macOS-only code (cfg(target_os = "macos"))
# that no Linux build compiles: Clippy of every Cargo target (libraries, binaries and tests) with -D warnings and the
# workspace's lints, which Bazel's Clippy applies to the same targets (Cargo caps warnings in dependencies).
# Run it from the repository root as `nix run .#darwin-check`, which provides Bazel's Rust version with the darwin
# standard library, and stubs for the target's C compiler and archiver since nothing is linked (flake.nix).
# The sources are checked in a scratch copy, where gang-ui's `assets` module, which Bazel generates from the dashboard
# bundle, is gang-embed's table of an empty bundle.
set -euo pipefail
scratch=$(mktemp -d)
trap 'chmod -R u+w "$scratch" && rm -rf "$scratch"' EXIT
export CARGO_HOME="${CARGO_HOME:-$scratch/cargo}" CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
rsync -a --exclude=/.git --filter=':- .gitignore' ./ "$scratch/src/"
mkdir "$scratch/dist"
cd "$scratch/src"
cargo run --locked --quiet --package gang-embed -- "$scratch/dist" crates/gang-ui/src/assets.rs
cargo clippy --locked --workspace --all-targets --target aarch64-apple-darwin -- -D warnings
