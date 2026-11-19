#!/usr/bin/env bash
# Exercise the graph guard and execute the actual CI aggregator bodies.
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

expect() {
  local expected=$1 description=$2
  shift 2
  cases=$((cases + 1))
  if "$@" >"$scratch/output" 2>&1; then
    if [[ $expected != success ]]; then fail "$description: unexpectedly succeeded"; fi
  elif [[ $expected == success ]]; then
    fail "$description: unexpectedly failed: $(cat "$scratch/output")"
  fi
}

jobs=$(yq -o=json '.jobs' "$workflow")
expect success 'current workflow coverage' "$guard" "$workflow"
# Follow the real dependency graph, not just required.needs: optional architectures
# must not sneak into the PR critical path through an intermediate dependency.
expect success 'PR graph excludes optional architectures and cache fan-in' jq -e '
  . as $jobs
  | def ancestors($id): [$id] + [($jobs[$id].needs // [] | if type == "array" then .[] else . end) | ancestors(.)[]];
  (ancestors("required") | unique) as $pr
  | all(["linux-arm64", "linux-arm64-bundle", "linux-arm64-checks", "cross", "macos", "cache-x86_64", "release-validation"][];
        . as $id | ($pr | index($id)) == null)
' <<<"$jobs"

for job in required nix-vm lint linux-x86_64-shards linux-x86_64 linux-x86_64-bundle cache-x86_64 linux-arm64 linux-arm64-bundle linux-arm64-checks cross macos release-validation e2e-gate-1-shards e2e-gate-1 e2e-gate-2-shards e2e-gate-2; do
  while IFS= read -r dependency; do
    cp "$workflow" "$fixture"
    job="$job" dependency="$dependency" yq -i '.jobs[strenv(job)].needs |= ([.] | flatten) | del(.jobs[strenv(job)].needs[] | select(. == strenv(dependency)))' "$fixture"
    expect failure "$job missing $dependency prerequisite" "$guard" "$fixture"
  done < <(jq -r --arg job "$job" '.[$job].needs | if type == "array" then .[] else . end' <<<"$jobs")
done
for job in linux-arm64 linux-arm64-bundle linux-arm64-checks cross macos cache-x86_64 release-validation; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs.required.needs += [strenv(job)]' "$fixture"
  expect failure "$job cannot be a PR required dependency" "$guard" "$fixture"
done
for job in linux-x86_64 e2e-gate-1 e2e-gate-2 required; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if = "success()"' "$fixture"
  expect failure "conditional $job aggregator" "$guard" "$fixture"
done
for job in linux-arm64-bundle linux-arm64-checks linux-arm64 cross macos release-validation; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if = "always()"' "$fixture"
  expect failure "$job cannot run on PRs" "$guard" "$fixture"
done
for job in required changes; do
  cp "$workflow" "$fixture"
  job="$job" yq -i 'del(.jobs[strenv(job)])' "$fixture"
  expect failure "missing $job job" "$guard" "$fixture"
done
cp "$workflow" "$fixture"
yq -i '.jobs.linux-x86_64.steps += [{"uses": "actions/checkout@fixture"}]' "$fixture"
expect failure 'x86 aggregator cannot run extra steps' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.guard_fixture."runs-on" = "ubuntu-latest" | .jobs.guard_fixture.steps = [{"run": "true"}]' "$fixture"
expect failure 'new heavy job missing changes dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.required.steps[] | select(.env.NEEDS_RESULTS != null) | .env.NEEDS_RESULTS) |= sub("needs[.]lint[.]result", "success")' "$fixture"
expect failure 'required result omits a dependency' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.lint.steps[] | select(.run != null) | .run) |= sub("tools/lint.sh --portable", "true")' "$fixture"
expect failure 'lint must perform portable lint rather than aggregate statuses' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.lint.steps[] | select(.name == "Lint") | ."timeout-minutes") = 4' "$fixture"
expect failure 'portable lint cannot exceed three minutes' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.release-validation.steps[0].env.ARM_RESULT = "success"' "$fixture"
expect failure 'release results cannot omit ARM validation' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.linux-x86_64-shards.needs = ["changes", "linux-arm64"]' "$fixture"
expect failure 'x86 shards cannot transitively require optional ARM jobs' "$guard" "$fixture"
for job in lint linux-x86_64-bundle e2e-gate-1-shards e2e-gate-2-shards; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)]."runs-on" = "ubuntu-24.04-arm"' "$fixture"
  expect failure "$job must run on x86" "$guard" "$fixture"
done
for job in e2e-gate-1-shards e2e-gate-2-shards; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].needs = ["changes", "linux-arm64-bundle"]' "$fixture"
  expect failure "$job cannot consume an ARM-only producer" "$guard" "$fixture"
done

# Execute actual GitHub job conditions with synthetic event values. Only the
# repository-owned expression is shell code; fixture values remain quoted data.
condition_matches() {
  local expr=$1 dependency
  expr=${expr#'${{'}
  expr=${expr%'}}'}
  expr=${expr//"always()"/true}
  expr=${expr//"startsWith(github.ref, 'refs/tags/v')"/'"$REF" == refs/tags/v*'}
  expr=${expr//github.event_name/'"$EVENT"'}
  expr=${expr//github.ref/'"$REF"'}
  expr=${expr//'!github.event.repository.private'/'"$PRIVATE" != true'}
  expr=${expr//github.event.repository.private/'"$PRIVATE" == true'}
  expr=${expr//needs.changes.outputs.heavy/'"$HEAVY"'}
  while IFS= read -r dependency; do
    expr=${expr//needs.$dependency.result/'"success"'}
  done < <(jq -r 'keys[]' <<<"$jobs")
  if [[ $expr == *github.* || $expr == *needs.* || $expr == *startsWith* ]]; then
    fail "Unsupported job condition: $expr"
    return 2
  fi
  expr=${expr//\(/ ( }
  expr=${expr//\)/ ) }
  bash --noprofile --norc -c "[[ $expr ]]"
}
export HEAVY=true PRIVATE=false
for job in linux-arm64-bundle linux-arm64-checks linux-arm64 cross macos release-validation; do
  condition=$(jq -er --arg job "$job" '.[$job].if' <<<"$jobs")
  export EVENT=pull_request REF=refs/pull/1/merge
  expect failure "$job intentionally skips a PR" condition_matches "$condition"
  # A PR targeting main must not bypass the event guard either.
  REF=refs/heads/main
  expect failure "$job rejects a PR even with a main ref" condition_matches "$condition"
  EVENT=push
  for REF in refs/heads/main refs/heads/master refs/tags/v0.3.2; do
    expect success "$job runs on push $REF" condition_matches "$condition"
  done
  for REF in refs/heads/feature refs/tags/not-a-release; do
    expect failure "$job skips push $REF" condition_matches "$condition"
  done
  EVENT=workflow_dispatch REF=refs/heads/main
  expect failure "$job does not run on dispatch" condition_matches "$condition"
  EVENT=push REF=refs/heads/main HEAVY=false
  if [[ $job == linux-arm64 || $job == release-validation ]]; then
    expected=success
  else
    expected=failure
  fi
  expect "$expected" "$job docs-only push preserves intentional skips" condition_matches "$condition"
  HEAVY=true
  if [[ $job == macos ]]; then
    PRIVATE=true
    expect failure 'private repositories intentionally skip macOS' condition_matches "$condition"
    PRIVATE=false
  fi
done

# Execute the actual status aggregators using the devShell-pinned Bash.
for job in linux-x86_64 linux-arm64 e2e-gate-1 e2e-gate-2 required; do
  body="$scratch/$job.sh"
  jq -er --arg job "$job" '
    [.[$job].steps[] | select(.env.NEEDS_RESULTS != null) | .run]
    | if length == 1 and (.[0] | type) == "string" then .[0]
      else error("expected exactly one NEEDS_RESULTS run step") end
  ' <<<"$jobs" >"$body"
  needs_count=$(jq -er --arg job "$job" '.[$job].needs | arrays | length' <<<"$jobs")
  all_success=
  docs_results=
  for ((i = 0; i < needs_count; i++)); do
    all_success+="${all_success:+ }success"
    if (( i % 2 == 0 )); then docs_results+="${docs_results:+ }success"; else docs_results+="${docs_results:+ }skipped"; fi
  done
  expect success "$job: heavy all success" env CHANGES_RESULT=success HEAVY=true NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -o pipefail <"$body"
  expect success "$job: docs success and skipped" env CHANGES_RESULT=success HEAVY=false NEEDS_RESULTS="$docs_results" bash --noprofile --norc -e -o pipefail <"$body"
  expect failure "$job: missing one result" env CHANGES_RESULT=success HEAVY=true NEEDS_RESULTS="${all_success#success }" bash --noprofile --norc -e -o pipefail <"$body"
  expect failure "$job: extra result" env CHANGES_RESULT=success HEAVY=true NEEDS_RESULTS="$all_success success" bash --noprofile --norc -e -o pipefail <"$body"
  for ((position = 0; position < needs_count; position++)); do
    for status in failure skipped cancelled; do
      results=
      for ((i = 0; i < needs_count; i++)); do
        result=success
        if (( i == position )); then result=$status; fi
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
  for heavy in '' invalid; do
    expect failure "$job: malformed heavy output '$heavy'" env CHANGES_RESULT=success HEAVY="$heavy" NEEDS_RESULTS="$all_success" bash --noprofile --norc -e -o pipefail <"$body"
  done
done

release_body="$scratch/release-validation.sh"
jq -er '.["release-validation"].steps[] | select(.env.REQUIRED_RESULT != null) | .run' <<<"$jobs" >"$release_body"
unset HEAVY
export CHANGES_RESULT=success REQUIRED_RESULT=success ARM_RESULT=success CROSS_RESULT=success MACOS_RESULT=success PRIVATE=false
expect success 'public release validation requires all platforms' bash --noprofile --norc -e -u -o pipefail <"$release_body"
for PRIVATE in false true; do
  for variable in CHANGES_RESULT REQUIRED_RESULT ARM_RESULT CROSS_RESULT MACOS_RESULT; do
    for status in failure cancelled skipped ''; do
      if [[ $variable == MACOS_RESULT && $PRIVATE == true && $status == skipped ]]; then
        expected=success
      else
        expected=failure
      fi
      expect "$expected" "release: heavy=false private=$PRIVATE $variable=$status" env HEAVY=false "$variable=$status" bash --noprofile --norc -e -u -o pipefail <"$release_body"
    done
  done
  expect failure 'release: heavy=false cannot bypass architecture validation' env HEAVY=false ARM_RESULT=skipped CROSS_RESULT=skipped MACOS_RESULT=skipped bash --noprofile --norc -e -u -o pipefail <"$release_body"
done

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
