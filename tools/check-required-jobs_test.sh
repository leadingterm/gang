#!/usr/bin/env bash
# Exercise the coverage guard and the actual CI aggregator scripts.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
workflow="$script_dir/../.github/workflows/ci.yml"
guard="$script_dir/check-required-jobs.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
fixture="$scratch/ci.yml"
failures=0
cases=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# expect <success|failure> <description> <command> [args...]
expect() {
  local expected=$1 description=$2
  shift 2
  cases=$((cases + 1))
  if "$@" >"$scratch/output" 2>&1; then
    if [[ $expected != success ]]; then
      fail "$description: unexpectedly succeeded"
    fi
  elif [[ $expected == success ]]; then
    fail "$description: unexpectedly failed: $(cat "$scratch/output")"
  fi
}

expect success 'current workflow coverage' "$guard" "$workflow"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.required.needs[] | select(. == "lint-arm64"))' "$fixture"
expect failure 'removed dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.guard_fixture."runs-on" = "ubuntu-latest" | .jobs.guard_fixture.steps = [{"run": "true"}]' "$fixture"
expect failure 'new job without dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.required)' "$fixture"
expect failure 'missing required job' "$guard" "$fixture"

jobs=$(yq -o=json '.jobs' "$workflow")
statuses=(success failure skipped cancelled)

# Execute the actual workflow body with Nix flake tools to test runner behavior.

for job in lint e2e-gate-2 required; do
  body="$scratch/$job.sh"
  jq -er --arg job "$job" '
    [.[$job].steps[] | select(.env.NEEDS_RESULTS != null) | .run]
    | if length == 1 and (.[0] | type) == "string" then .[0]
      else error("expected exactly one NEEDS_RESULTS run step") end
  ' <<<"$jobs" >"$body"
  needs_count=$(jq -er --arg job "$job" '.[$job].needs | arrays | length' <<<"$jobs")
  if (( needs_count < 2 )); then
    echo "$job must have at least two dependencies for the status matrix" >&2
    exit 1
  fi

  for first in "${statuses[@]}"; do
    for second in "${statuses[@]}"; do
      results="$first $second"
      for ((position = 2; position < needs_count; position++)); do
        results+=' success'
      done
      expected=failure
      if [[ $first == success && $second == success ]]; then
        expected=success
      fi
      expect "$expected" "$job: $results" env NEEDS_RESULTS="$results" bash --noprofile --norc -e -o pipefail <"$body"
    done
  done

  for ((position = 2; position < needs_count; position++)); do
    for status in failure skipped cancelled; do
      results=
      for ((i = 0; i < needs_count; i++)); do
        result=success
        if (( i == position )); then
          result=$status
        fi
        results+="${results:+ }$result"
      done
      expect failure "$job: position $position is $status" env NEEDS_RESULTS="$results" bash --noprofile --norc -e -o pipefail <"$body"
    done
  done
  expect failure "$job: empty results" env NEEDS_RESULTS='' bash --noprofile --norc -e -o pipefail <"$body"
done

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
