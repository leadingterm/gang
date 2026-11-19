#!/usr/bin/env bash
# Lints the workspace for aarch64-apple-darwin, so a Linux host compiles the macOS-only code (cfg(target_os = "macos"))
# that no Linux build compiles: Clippy of every Cargo target (libraries, binaries and tests) with -D warnings and the
# workspace's lints, which Bazel's Clippy applies to the same targets (Cargo caps warnings in dependencies).
# Run it from the repository root as `nix run .#darwin-check`, which provides Bazel's Rust version with the darwin
# standard library, and stubs for the target's C compiler and archiver since nothing is linked (flake.nix).
# The sources are checked in an isolated copy, where gang-ui's `assets` module, which Bazel generates from the dashboard
# bundle, is gang-embed's table of an empty bundle. CI supplies DARWIN_SOURCE_DIR to keep Cargo's source paths stable;
# local invocations default to scratch/src. The source copy must stay outside the archived Cargo directories.
set -euo pipefail
scratch=$(mktemp -d)
trap 'chmod -R u+w "$scratch" && rm -rf "$scratch"' EXIT
export CARGO_HOME="${CARGO_HOME:-$scratch/cargo}" CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
source_dir=${DARWIN_SOURCE_DIR:-$scratch/src}
mkdir -p "$source_dir" "$CARGO_HOME" "$CARGO_TARGET_DIR"
CARGO_HOME=$(cd "$CARGO_HOME" && pwd -P)
CARGO_TARGET_DIR=$(cd "$CARGO_TARGET_DIR" && pwd -P)
source_dir=$(cd "$source_dir" && pwd -P)
# Neither rsync's deletion nor Cargo's output may overlap the checkout or source copy.
for dir in "$PWD" "$CARGO_HOME" "$CARGO_TARGET_DIR"; do
  dir=$(cd "$dir" && pwd -P)
  if [[ $source_dir == / || $dir == / || $source_dir/ == "$dir/"* || $dir/ == "$source_dir/"* ]]; then
    printf 'Darwin source directory must not overlap the checkout or Cargo cache: %s\n' "$dir" >&2
    exit 1
  fi
done
# Remove deleted and newly ignored files from a reused CI copy before generating the stub again.
rsync -a --delete --delete-excluded --exclude=/.git --filter=':- .gitignore' ./ "$source_dir/"
mkdir "$scratch/dist"
cd "$source_dir"
cargo run --locked --quiet --package gang-embed -- "$scratch/dist" crates/gang-ui/src/assets.rs
cargo clippy --locked --workspace --all-targets --target aarch64-apple-darwin -- -D warnings
