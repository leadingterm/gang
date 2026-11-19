#!/usr/bin/env bash
# The local x86_64 Linux full workspace diagnostic mirrors CI checks, stopping at the first failure:
# portable lint and nix-vm checks, BUILD files in sync, the full workspace, then Gate 1 and both
# Gate 2 addition shards launched through the same tools/e2e-gate.sh launcher as CI.
# Architecture-specific release and cross builds are verified separately in CI.
# Usage: nix develop -c tools/gate.sh. BAZEL names the Bazel command (default: the devShell's `bazel`).
set -euo pipefail
cd "$(dirname "$0")/.."
tools/lint.sh
bazel=${BAZEL:-bazel}
# Use the shipping optimized configuration without uploading results to a remote cache.
flags=(--compilation_mode=opt --remote_upload_local_results=false)
"$bazel" run "${flags[@]}" //:gazelle.check
"$bazel" test "${flags[@]}" //...
"$bazel" build "${flags[@]}" //:gang //crates/gang-e2e:bundle
gang=$("$bazel" cquery "${flags[@]}" --output=files //:gang)
bundle=$(dirname "$("$bazel" cquery "${flags[@]}" --output=files //crates/gang-e2e:bundle/local)")
tools/e2e-gate.sh "$bundle" "$gang" --gate 1
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions --shard 1/2
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions --shard 2/2
