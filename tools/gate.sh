#!/usr/bin/env bash
# The local x86_64 Linux diagnostic mirrors the PR gate, stopping at the first failure:
# portable lint (including BUILD sync), nix-vm checks, the full workspace and bundle, then
# Gate 1 and both Gate 2 addition shards through the same tools/e2e-gate.sh launcher as CI.
# Run these harnesses sequentially: local parity means identical scenario coverage, not
# launching CI's sixteen independent harness jobs together on one bounded host.
# ARM, cross and macOS validation run separately on default-branch pushes and v* tags.
# Usage: nix develop -c tools/gate.sh. BAZEL names the Bazel command (default: the devShell's `bazel`).
set -euo pipefail
cd "$(dirname "$0")/.."
tools/lint.sh
bazel=${BAZEL:-bazel}
# Disable inherited remote services without disabling the local disk cache.
flags=(--compilation_mode=opt --remote_cache= --remote_executor=)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
BAZEL="$bazel" tools/ci-targets.sh --query | tools/ci-targets.sh 1 1 >"$scratch/targets"
"$bazel" build "${flags[@]}" --skip_incompatible_explicit_targets --aspects=//bazel/rust:unit_compilation.bzl%unit_compilation --output_groups=unit_compilation --target_pattern_file="$scratch/targets"
"$bazel" test "${flags[@]}" --skip_incompatible_explicit_targets --target_pattern_file="$scratch/targets"
"$bazel" build "${flags[@]}" //crates/gang-e2e:bundle
bundle=$(dirname "$("$bazel" cquery "${flags[@]}" --output=files //crates/gang-e2e:bundle/local)")
gang="$bundle/gang"
tools/e2e-gate.sh "$bundle" "$gang" --gate 1
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions --shard 1/2
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions --shard 2/2
