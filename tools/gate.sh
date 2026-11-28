#!/usr/bin/env bash
# The local x86_64 Linux diagnostic covers the queue's x86 checks, stopping at the first failure:
# portable lint (including BUILD sync), nix-vm checks, the full workspace and bundle, then
# all CI Gate 1 and Gate 2 addition shards through the same tools/e2e-gate.sh launcher as CI.
# Run these harnesses sequentially: coverage matches CI, without launching sixteen
# independent harness jobs together on one bounded host.
# ARM, cross and advisory macOS validation run separately in the merge queue.
# Usage: GANG_CONTROLLER_BINARY=<qualified-same-commit-ARM64-static-gang> nix develop -c tools/gate.sh
# The binary must be a readable file; --list-e2e does not require it.
# BAZEL names the Bazel command (default: the devShell's `bazel`).
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
if [[ -z ${GANG_CONTROLLER_BINARY:-} || ! -f $GANG_CONTROLLER_BINARY || ! -r $GANG_CONTROLLER_BINARY ]]; then
  echo 'Set GANG_CONTROLLER_BINARY to a readable, qualified same-commit ARM64 static gang binary.' >&2
  exit 1
fi
tools/lint.sh
bazel=${BAZEL:-bazel}
# Disable inherited remote services without disabling the local disk cache.
flags=(--config=local --compilation_mode=opt --remote_cache= --remote_executor=)
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
