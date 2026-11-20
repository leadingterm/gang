#!/usr/bin/env bash
# The local x86_64 Linux diagnostic mirrors the PR gate, stopping at the first failure:
# portable lint (including BUILD sync), nix-vm checks, the full workspace and bundle, then
# all CI Gate 1 and Gate 2 addition shards through the same tools/e2e-gate.sh launcher as CI.
# Run these harnesses sequentially: local parity means identical scenario coverage, not
# launching CI's sixteen independent harness jobs together on one bounded host.
# ARM, cross and macOS validation run separately on default-branch pushes and v* tags.
# Usage: nix develop -c tools/gate.sh [--list-e2e]. BAZEL names the Bazel command (default: the devShell's `bazel`).
set -euo pipefail
cd "$(dirname "$0")/.."
if (($# > 1)) || { (($# == 1)) && [[ $1 != --list-e2e ]]; }; then
  echo "usage: tools/gate.sh [--list-e2e]" >&2
  exit 2
fi

selections=$(yq -o=json '.jobs' .github/workflows/ci.yml | jq -er '
  def shards($job):
    .[$job].strategy.matrix.shard as $shards
    | if ($shards | type) != "array" then error("invalid shard matrix: " + $job)
      else ($shards | length) as $n
      | if $n == 0 or ($shards | sort) != [range(1; $n + 1)]
        then error("invalid shard matrix: " + $job)
        else $shards[] | "\(.)/\($n)" end
      end;
  . as $jobs
  | [($jobs | shards("e2e-gate-1-shards") | "--gate 1 --shard \(.)"),
     ($jobs | shards("e2e-gate-2-shards") | "--gate 2 --additions --shard \(.)")]
  | .[]
')
if (($# == 1)); then
  printf '%s\n' "$selections"
  exit 0
fi
tools/lint.sh
bazel=${BAZEL:-bazel}
# Disable inherited remote services without disabling the local disk cache.
flags=(--compilation_mode=opt --remote_cache= --remote_executor=)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
BAZEL="$bazel" tools/ci-targets.sh --query | tools/ci-targets.sh 1 1 >"$scratch/targets"
"$bazel" test "${flags[@]}" --skip_incompatible_explicit_targets --target_pattern_file="$scratch/targets"
"$bazel" build "${flags[@]}" //crates/gang-e2e:bundle
bundle=$(dirname "$("$bazel" cquery "${flags[@]}" --output=files //crates/gang-e2e:bundle/local)")
gang="$bundle/gang"
while IFS= read -r selection; do
  read -r -a arguments <<<"$selection"
  tools/e2e-gate.sh "$bundle" "$gang" "${arguments[@]}"
done <<<"$selections"
