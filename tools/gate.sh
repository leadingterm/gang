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
# Disable inherited remote services without disabling the local disk cache.
flags=(--compilation_mode=opt --remote_cache= --remote_executor=)
"$bazel" run "${flags[@]}" //:gazelle.check
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
manual_tests=$("$bazel" query 'attr("tags", "(^|, |\\[)manual(, |\\]$)", kind(".*_test rule", //...) union kind("test_suite rule", //...))')
if [[ -n $manual_tests ]]; then
  printf 'Manual tests or test suites would be excluded:\n%s\n' "$manual_tests" >&2
  exit 1
fi
"$bazel" query 'tests(//...) union (kind(".* rule", //...) except kind(".*_test rule", //...) except kind("test_suite rule", //...) except attr("tags", "(^|, |\\[)manual(, |\\]$)", //...))' |
  tools/ci-targets.sh 1 1 >"$scratch/targets"
"$bazel" test "${flags[@]}" --skip_incompatible_explicit_targets --target_pattern_file="$scratch/targets"
"$bazel" build "${flags[@]}" //:gang //crates/gang-e2e:bundle
gang=$("$bazel" cquery "${flags[@]}" --output=files //:gang)
bundle=$(dirname "$("$bazel" cquery "${flags[@]}" --output=files //crates/gang-e2e:bundle/local)")
tools/e2e-gate.sh "$bundle" "$gang" --gate 1
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions --shard 1/2
tools/e2e-gate.sh "$bundle" "$gang" --gate 2 --additions --shard 2/2
