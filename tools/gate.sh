#!/usr/bin/env bash
# The pre-merge gate on one Linux host, in CI's configuration, stopping at the first failure (docs/SPECIFICATION.md §7):
# CI's lint job (tools/lint.sh, macOS Clippy of every Cargo target included), BUILD files in sync, the full workspace, then
# Gate 1 and the Gate 2 additions launched exactly as CI's e2e jobs launch them (tools/e2e-gate.sh).
# Usage: nix develop -c tools/gate.sh. BAZEL names the Bazel command (default: the devShell's `bazel`).
set -euo pipefail
cd "$(dirname "$0")/.."
tools/lint.sh
bazel=${BAZEL:-bazel}
# CI builds and tests in the configuration Gang ships (.bazelrc `build:ci`); a gate uploads nothing to a remote cache.
flags=(--compilation_mode=opt --remote_upload_local_results=false)
"$bazel" run "${flags[@]}" //:gazelle.check
"$bazel" test "${flags[@]}" //...
"$bazel" build "${flags[@]}" //:gang //crates/gang-e2e:bundle
gang=$("$bazel" cquery "${flags[@]}" --output=files //:gang)
bundle=$(dirname "$("$bazel" cquery "${flags[@]}" --output=files //crates/gang-e2e:bundle/local)")
tools/e2e-gate.sh "$bundle" "$gang" --gate 1
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions
