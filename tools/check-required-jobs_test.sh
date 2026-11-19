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

jobs=$(yq -o=json '.jobs' "$workflow")
expect success 'macos is informational' jq -e '(.macos | type) == "object" and (.required.needs | index("macos") == null)' <<<"$jobs"
expect success 'x86 cache is informational' jq -e '(.["cache-x86_64"] | type) == "object" and (.required.needs | index("cache-x86_64") == null)' <<<"$jobs"

expect success 'current workflow coverage' "$guard" "$workflow"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.required.needs[] | select(. == "lint"))' "$fixture"
expect failure 'removed dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.required.needs += ["macos"] | (.jobs.required.steps[] | select(.env.NEEDS_RESULTS != null) | .env.NEEDS_RESULTS) |= sub("needs[.]lint[.]result", "needs.lint.result, needs.macos.result")' "$fixture"
expect failure 'macos cannot be a required dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.required.needs += ["cache-x86_64"] | (.jobs.required.steps[] | select(.env.NEEDS_RESULTS != null) | .env.NEEDS_RESULTS) |= sub("needs[.]lint[.]result", "needs.lint.result, needs.cache-x86_64.result")' "$fixture"
expect failure 'x86 cache cannot be a required dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.linux-x86_64.steps += [{"uses": "actions/checkout@fixture"}]' "$fixture"
expect failure 'x86 aggregator cannot run extra steps' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.guard_fixture."runs-on" = "ubuntu-latest" | .jobs.guard_fixture.steps = [{"run": "true"}]' "$fixture"
expect failure 'new job without dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.guard_fixture."runs-on" = "ubuntu-latest" | .jobs.guard_fixture.steps = [{"run": "true"}] | .jobs.required.needs += ["guard_fixture"] | (.jobs.required.steps[] | select(.env.NEEDS_RESULTS != null) | .env.NEEDS_RESULTS) |= sub("needs[.]lint[.]result", "needs.lint.result, needs.guard_fixture.result")' "$fixture"
expect failure 'new required heavy job missing changes dependency' "$guard" "$fixture"
for dependency in changes lint-arm64 nix-vm; do
  cp "$workflow" "$fixture"
  dependency="$dependency" yq -i 'del(.jobs.lint.needs[] | select(. == strenv(dependency)))' "$fixture"
  expect failure "lint missing $dependency prerequisite" "$guard" "$fixture"
done
for dependency in changes linux-x86_64-shards; do
  cp "$workflow" "$fixture"
  dependency="$dependency" yq -i 'del(.jobs.linux-x86_64.needs[] | select(. == strenv(dependency)))' "$fixture"
  expect failure "linux-x86_64 missing $dependency prerequisite" "$guard" "$fixture"
done
for dependency in changes linux-x86_64-shards; do
  cp "$workflow" "$fixture"
  dependency="$dependency" yq -i 'del(.jobs.cache-x86_64.needs[] | select(. == strenv(dependency)))' "$fixture"
  expect failure "cache-x86_64 missing $dependency prerequisite" "$guard" "$fixture"
done
for dependency in changes linux-arm64-bundle linux-arm64-checks; do
  cp "$workflow" "$fixture"
  dependency="$dependency" yq -i 'del(.jobs.linux-arm64.needs[] | select(. == strenv(dependency)))' "$fixture"
  expect failure "linux-arm64 missing $dependency prerequisite" "$guard" "$fixture"
done
for dependency in changes linux-arm64-bundle; do
  cp "$workflow" "$fixture"
  dependency="$dependency" yq -i 'del(.jobs.linux-arm64-checks.needs[] | select(. == strenv(dependency)))' "$fixture"
  expect failure "linux-arm64-checks missing $dependency prerequisite" "$guard" "$fixture"
done
for job in e2e-gate-1-shards e2e-gate-2-shards; do
  for dependency in changes linux-arm64-bundle; do
    cp "$workflow" "$fixture"
    job="$job" dependency="$dependency" yq -i 'del(.jobs[strenv(job)].needs[] | select(. == strenv(dependency)))' "$fixture"
    expect failure "$job missing $dependency prerequisite" "$guard" "$fixture"
  done
done
for job in linux-arm64 linux-arm64-bundle linux-arm64-checks; do
  cp "$workflow" "$fixture"
  job="$job" yq -i 'del(.jobs.required.needs[] | select(. == strenv(job)))' "$fixture"
  expect failure "required missing $job" "$guard" "$fixture"
done
for job in e2e-gate-1 e2e-gate-2; do
  for dependency in changes "$job-shards"; do
    cp "$workflow" "$fixture"
    job="$job" dependency="$dependency" yq -i 'del(.jobs[strenv(job)].needs[] | select(. == strenv(dependency)))' "$fixture"
    expect failure "$job missing $dependency prerequisite" "$guard" "$fixture"
  done
done
cp "$workflow" "$fixture"
yq -i '(.jobs.required.steps[] | select(.env.NEEDS_RESULTS != null) | .env.NEEDS_RESULTS) |= sub("needs[.]lint-arm64[.]result", "success")' "$fixture"
expect failure 'required result omits a dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.required)' "$fixture"
expect failure 'missing required job' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.changes)' "$fixture"
expect failure 'missing changes job' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.linux-arm64-bundle.needs)' "$fixture"
expect failure 'heavy leaf missing changes dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.e2e-gate-1-shards.needs[] | select(. == "changes"))' "$fixture"
expect failure 'e2e leaf missing changes dependency' "$guard" "$fixture"

for job in lint linux-x86_64 linux-arm64 e2e-gate-1 e2e-gate-2 required; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if = "success()"' "$fixture"
  expect failure "conditional $job aggregator" "$guard" "$fixture"
done

# Execute the actual workflow body with Nix flake tools to test runner behavior.

for job in lint linux-x86_64 linux-arm64 e2e-gate-1 e2e-gate-2 required; do
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

  all_success=
  docs_results=
  for ((i = 0; i < needs_count; i++)); do
    all_success+="${all_success:+ }success"
    if (( i % 2 == 0 )); then
      docs_results+="${docs_results:+ }success"
    else
      docs_results+="${docs_results:+ }skipped"
    fi
  done
  expect success "$job: heavy all success" env CHANGES_RESULT=success HEAVY=true NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -o pipefail <"$body"
  expect success "$job: docs success and skipped" env CHANGES_RESULT=success HEAVY=false NEEDS_RESULTS="$docs_results" bash --noprofile --norc -e -o pipefail <"$body"

  for ((position = 0; position < needs_count; position++)); do
    for status in failure skipped cancelled; do
      results=
      for ((i = 0; i < needs_count; i++)); do
        result=success
        if (( i == position )); then
          result=$status
        fi
        results+="${results:+ }$result"
      done
      expect failure "$job: heavy position $position is $status" env CHANGES_RESULT=success HEAVY=true NEEDS_RESULTS="$results" bash --noprofile --norc -e -o pipefail <"$body"
      if [[ $status != skipped ]]; then
        expect failure "$job: docs position $position is $status" env CHANGES_RESULT=success HEAVY=false NEEDS_RESULTS="$results" bash --noprofile --norc -e -o pipefail <"$body"
      fi
    done
  done
  for heavy in true false; do
    expect failure "$job: $heavy empty results" env CHANGES_RESULT=success HEAVY="$heavy" NEEDS_RESULTS='' bash --noprofile --norc -e -o pipefail <"$body"
    expect failure "$job: $heavy whitespace-only results" env CHANGES_RESULT=success HEAVY="$heavy" NEEDS_RESULTS='   ' bash --noprofile --norc -e -o pipefail <"$body"
    for status in failure cancelled skipped; do
      expect failure "$job: detector $status, heavy $heavy" env CHANGES_RESULT="$status" HEAVY="$heavy" NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -o pipefail <"$body"
    done
  done
  expect failure "$job: missing heavy output" env -u HEAVY CHANGES_RESULT=success NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -u -o pipefail <"$body"
  expect failure "$job: empty heavy output" env CHANGES_RESULT=success HEAVY='' NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -o pipefail <"$body"
  expect failure "$job: malformed heavy output" env CHANGES_RESULT=success HEAVY=invalid NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -o pipefail <"$body"
done

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
