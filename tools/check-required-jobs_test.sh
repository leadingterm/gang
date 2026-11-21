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
cp "$workflow" "$fixture"
yq -i 'del(.on.merge_group)' "$fixture"
expect failure 'merge group trigger is required' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.on.merge_group.types = ["destroyed"]' "$fixture"
expect failure 'merge group trigger must request checks' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.changes.steps[] | select(.id == "filter") | .env.BASE) = "${{ github.event_name == '\''pull_request'\'' && github.event.pull_request.base.sha || '\''0000000000000000000000000000000000000000'\'' }}"' "$fixture"
expect failure 'change classification must use the merge group base' "$guard" "$fixture"
# Follow the real dependency graph, not just required.needs: optional architectures
# must not sneak into the PR critical path through an intermediate dependency.
expect success 'PR graph excludes optional architectures and cache fan-in' jq -e "
  . as \$jobs
  | def ancestors(\$id): [\$id] + [(\$jobs[\$id].needs // [] | if type == \"array\" then .[] else . end) | ancestors(.)[]];
  (ancestors(\"required\") | unique) as \$pr
  | all([\"linux-arm64\", \"linux-arm64-bundle\", \"linux-arm64-checks\", \"cross\", \"macos\", \"cache-x86_64\", \"release-validation\"][];
        . as \$id | (\$pr | index(\$id)) == null)
" <<<"$jobs"
expect success 'release validation graph excludes macOS' jq -e "
  . as \$jobs
  | def ancestors(\$id): [\$id] + [(\$jobs[\$id].needs // [] | if type == \"array\" then .[] else . end) | ancestors(.)[]];
  ancestors(\"release-validation\") | index(\"macos\") == null
" <<<"$jobs"

for job in required nativelink-memory-tests nix-vm lint linux-x86_64-shards linux-x86_64 linux-x86_64-bundle cache-x86_64 linux-arm64 linux-arm64-bundle linux-arm64-checks cross macos release-validation e2e-gate-1-shards e2e-gate-1 e2e-gate-2-shards e2e-gate-2; do
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
cp "$workflow" "$fixture"
yq -i '.jobs.release-validation.needs += ["macos"]' "$fixture"
expect failure 'macOS cannot be a release validation dependency' "$guard" "$fixture"
for job in linux-x86_64 e2e-gate-1 e2e-gate-2 required; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if = "success()"' "$fixture"
  expect failure "conditional $job aggregator" "$guard" "$fixture"
done
for job in linux-arm64-bundle linux-arm64-checks linux-arm64 cross macos release-validation; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if = "always()"' "$fixture"
  expect failure "$job cannot run on untrusted events" "$guard" "$fixture"
done
for job in linux-arm64-bundle linux-arm64-checks linux-arm64 cross release-validation; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if |= sub("github.event.pull_request.head.repo.full_name == github.repository", "true")' "$fixture"
  expect failure "$job cannot accept fork release PRs" "$guard" "$fixture"
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if |= sub("startsWith[(]github.event.pull_request.head.ref, '\''release/'\''[)]", "true")' "$fixture"
  expect failure "$job cannot accept ordinary same-repository PRs" "$guard" "$fixture"
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].if |= sub("github.event_name == '\''merge_group'\'' [|][|] ", "")' "$fixture"
  expect failure "$job must include merge groups" "$guard" "$fixture"
done
cp "$workflow" "$fixture"
yq -i 'del(.jobs.linux-x86_64-bundle.steps[0].with.ref)' "$fixture"
expect failure 'release binary checkout must use the PR head' "$guard" "$fixture"
cp "$workflow" "$fixture"
# Keep the GitHub expression literal so the mutation tests checkout-source validation.
# shellcheck disable=SC2016
yq -i '(.jobs.changes.steps[] | select(.id == "filter") | .env.HEAD) = "${{ github.sha }}"' "$fixture"
expect failure 'release change classification must use the PR head' "$guard" "$fixture"
for job in required changes nativelink-memory-tests; do
  cp "$workflow" "$fixture"
  job="$job" yq -i 'del(.jobs[strenv(job)])' "$fixture"
  expect failure "missing $job job" "$guard" "$fixture"
done
cp "$workflow" "$fixture"
yq -i '.jobs.nativelink-memory-tests.if = "needs.changes.outputs.heavy == '\''true'\'' && false"' "$fixture"
expect failure 'NativeLink memory tests cannot skip heavy changes' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.required.steps[] | select(.env.NEEDS_RESULTS != null) | .env.NEEDS_RESULTS) |= sub("needs[.]nativelink-memory-tests[.]result", "success")' "$fixture"
expect failure 'required result cannot omit NativeLink memory tests' "$guard" "$fixture"
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
yq -i '(.jobs.release-validation.steps[] | select(.env.REQUIRED_RESULT != null) | .env.HEAVY) = "true"' "$fixture"
expect failure 'release validation must use changes heavy output' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.linux-x86_64-shards.needs = ["changes", "linux-arm64"]' "$fixture"
expect failure 'x86 shards cannot transitively require optional ARM jobs' "$guard" "$fixture"
for job in nativelink-memory-tests lint linux-x86_64-bundle e2e-gate-1-shards e2e-gate-2-shards; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)]."runs-on" = "ubuntu-24.04-arm"' "$fixture"
  expect failure "$job must run on x86" "$guard" "$fixture"
done
for job in e2e-gate-1-shards e2e-gate-2-shards; do
  cp "$workflow" "$fixture"
  job="$job" yq -i '.jobs[strenv(job)].needs = ["changes", "linux-arm64-bundle"]' "$fixture"
  expect failure "$job cannot consume an ARM-only producer" "$guard" "$fixture"
done
for job in e2e-gate-1-shards e2e-gate-2-shards; do
  for matrix in '{"shard":[1,2,3,4]}' '{"shard":[1,2,3,4,5,6,7,7]}' '{"shard":[1,2,3,4,5,6,7,8],"exclude":[{"shard":8}]}' '{"shard":[1,2,3,4,5,6,7,8],"engine":["sqlite","postgres"]}'; do
    cp "$workflow" "$fixture"
    job="$job" matrix="$matrix" yq -i '.jobs[strenv(job)].strategy.matrix = (strenv(matrix) | from_json)' "$fixture"
    expect failure "$job rejects incomplete, duplicate or expanded matrices: $matrix" "$guard" "$fixture"
  done
  for denominator in 4 8; do
    cp "$workflow" "$fixture"
    job="$job" denominator="$denominator" yq -i '(.jobs[strenv(job)].steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) |= sub("/.*$", "/" + strenv(denominator))' "$fixture"
    expect failure "$job denominator must derive from matrix job total, not $denominator" "$guard" "$fixture"
  done
  cp "$workflow" "$fixture"
  job="$job" yq -i '(.jobs[strenv(job)].steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) += " --scenario api"' "$fixture"
  expect failure "$job cannot filter away scenarios" "$guard" "$fixture"
done

# Run the real local gate against a copied workflow; lint must never run in list mode.
local_gate="$scratch/local-gate"
mkdir -p "$local_gate/tools" "$local_gate/.github/workflows"
cp "$script_dir/gate.sh" "$local_gate/tools/gate.sh"
cp "$workflow" "$local_gate/.github/workflows/ci.yml"
cat >"$local_gate/tools/lint.sh" <<'EOF'
#!/usr/bin/env bash
touch tools/lint-called
exit 99
EOF
chmod +x "$local_gate/tools/lint.sh"
local_workflow="$local_gate/.github/workflows/ci.yml"

expect_local_selections() {
  local description=$1
  expected=$(yq -o=json '.jobs' "$local_workflow" | jq -r '
    . as $jobs
    | ["e2e-gate-1-shards", "e2e-gate-2-shards"][]
    | $jobs[.] as $job
    | ($job.strategy.matrix.shard | length | tostring) as $total
    | ($job.steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) as $args
    | $job.strategy.matrix.shard[] | tostring as $shard
    | $args | split("${{ matrix.shard }}") | join($shard)
    | split("${{ strategy.job-total }}") | join($total)
  ')
  cases=$((cases + 1))
  if actual=$(bash "$local_gate/tools/gate.sh" --list-e2e 2>"$scratch/local-error"); then
    if [[ $actual != "$expected" ]]; then fail "$description: local selections differ from CI"; fi
  else
    fail "$description: list failed: $(cat "$scratch/local-error")"
  fi
}

expect_local_selections 'local gate follows actual CI selections'
ci_selections=$expected
listed_selections=$actual
cases=$((cases + 1))
if ! yq -o=json '.jobs' "$local_workflow" | jq -e '
  [.["e2e-gate-1-shards"], .["e2e-gate-2-shards"] |
   .strategy.matrix.shard[]] | length == 16
' >/dev/null; then
  fail 'actual CI must have sixteen selections'
fi
cases=$((cases + 1))
if [[ $(printf '%s\n' "$actual" | sort -u | wc -l | tr -d ' ') != 16 ]]; then
  fail 'local gate must list sixteen unique selections'
fi
for job in e2e-gate-1-shards e2e-gate-2-shards; do
  cp "$workflow" "$local_workflow"
  job="$job" yq -i '.jobs[strenv(job)].strategy.matrix.shard = [1, 2, 3]' "$local_workflow"
  expect_local_selections "$job follows a different valid matrix size"
  for matrix in '[]' '[1,3]' '[1,1]' '[1,"2"]' '[1,2.5]' '[0,1]'; do
    cp "$workflow" "$local_workflow"
    job="$job" matrix="$matrix" yq -i '.jobs[strenv(job)].strategy.matrix.shard = (strenv(matrix) | from_json)' "$local_workflow"
    expect failure "$job rejects invalid local matrix $matrix" bash "$local_gate/tools/gate.sh" --list-e2e
  done
done
cp "$workflow" "$local_workflow"
for args in '--unknown' '--list-e2e --unknown' '--list-e2e --list-e2e'; do
  read -r -a arguments <<<"$args"
  cases=$((cases + 1))
  status=0
  bash "$local_gate/tools/gate.sh" "${arguments[@]}" >"$scratch/output" 2>&1 || status=$?
  if [[ $status -ne 2 ]]; then fail "local gate rejects $args with status 2 (got $status)"; fi
done
cases=$((cases + 1))
if [[ -e $local_gate/tools/lint-called ]]; then fail 'list and invalid arguments must not invoke lint'; fi

# Substitute only the heavyweight tools so the real normal-mode dispatch can run.
cat >"$local_gate/tools/lint.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat >"$local_gate/tools/ci-targets.sh" <<'EOF'
#!/usr/bin/env bash
if [[ ${1:-} == --query ]]; then
  printf '%s\n' '//fixture:target'
else
  cat
fi
EOF
cat >"$local_gate/tools/bazel" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  test|build) exit 0 ;;
  cquery) printf '%s\n' "$(dirname "$0")/../bundle/local" ;;
  *) exit 1 ;;
esac
EOF
cat >"$local_gate/tools/e2e-gate.sh" <<'EOF'
#!/usr/bin/env bash
shift 2
{
  printf '%s' "$1"
  shift
  printf ' %s' "$@"
  printf '\n'
} >>tools/e2e-calls
EOF
chmod +x "$local_gate/tools/lint.sh" "$local_gate/tools/ci-targets.sh" "$local_gate/tools/bazel" "$local_gate/tools/e2e-gate.sh"
cases=$((cases + 1))
if BAZEL="$local_gate/tools/bazel" bash "$local_gate/tools/gate.sh" >"$scratch/output" 2>&1; then
  calls=$(cat "$local_gate/tools/e2e-calls")
  if [[ $calls != "$ci_selections" ]]; then
    fail 'normal local gate calls must match CI'
  fi
  if [[ $calls != "$listed_selections" ]]; then
    fail 'normal local gate calls must match list mode'
  fi
else
  fail "normal local gate failed: $(cat "$scratch/output")"
fi
cp "$workflow" "$fixture"
yq -i '(.jobs.e2e-gate-2-shards.steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) |= sub(" --additions", "")' "$fixture"
expect failure 'Gate 2 must run additions without repeating Gate 1' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.name == "Build gang and e2e harness") | .run) |= sub("//crates/gang-e2e:bundle", "//:gang")' "$fixture"
expect failure 'bundle producer must build the harness bundle' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.name == "Build gang and e2e harness") | .run) |= sub("//:gang ", "")' "$fixture"
expect failure 'bundle producer must explicitly materialize gang for the release upload' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.linux-x86_64-bundle.steps |= (map(select(.name != "Build gang and e2e harness")) + map(select(.name == "Build gang and e2e harness")))' "$fixture"
expect failure 'bundle producer must materialize gang before the release upload' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.linux-x86_64-bundle.steps[] | select(.uses == "./.github/actions/upload-gang"))' "$fixture"
expect failure 'bundle producer must upload the gang release artifact' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i 'del(.jobs.linux-x86_64-bundle.steps[] | select(.with.name == "e2e-harness-x86_64"))' "$fixture"
expect failure 'bundle producer must upload the harness artifact' "$guard" "$fixture"
cp "$workflow" "$fixture"
yq -i '.jobs.linux-x86_64-bundle.steps += [{"uses": "./.github/actions/save-bazel-cache", "with": {"mode": "export", "shard": "x86-bundle"}}]' "$fixture"
expect failure 'bundle working-set export must stay removed' "$guard" "$fixture"
for metric in bazel-build.bep.json bazel-build.profile.gz; do
  cp "$workflow" "$fixture"
  metric="$metric" yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.with.name == "bazel-metrics-x86-bundle") | .with.path) |= sub(strenv(metric), "missing")' "$fixture"
  expect failure "bundle metrics must retain $metric" "$guard" "$fixture"
done
for mutation in condition pin retention; do
  cp "$workflow" "$fixture"
  case "$mutation" in
    condition) yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.with.name == "bazel-metrics-x86-bundle") | .if) = "success()"' "$fixture" ;;
    pin) yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.with.name == "bazel-metrics-x86-bundle") | .uses) = "actions/upload-artifact@main"' "$fixture" ;;
    retention) yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.with.name == "bazel-metrics-x86-bundle") | .with."retention-days") = 90' "$fixture" ;;
  esac
  expect failure "bundle metrics enforce $mutation" "$guard" "$fixture"
done

# Execute actual GitHub job conditions with synthetic event values. Only the
# repository-owned expression is shell code; fixture values remain quoted data.
expect_condition() {
  local expected=$1 description=$2 expr=$3 dependency actual=failure
  expr=${expr#"\${{"}
  expr=${expr%'}}'}
  expr=${expr//"always()"/true}
  expr=${expr//"startsWith(github.ref, 'refs/tags/v')"/"\"\$REF\" == refs/tags/v*"}
  expr=${expr//"startsWith(github.event.pull_request.head.ref, 'release/')"/"\"\$HEAD_REF\" == release/*"}
  expr=${expr//github.event.pull_request.head.repo.full_name/"\"\$HEAD_REPO\""}
  expr=${expr//github.repository/"\"\$REPO\""}
  expr=${expr//github.event_name/"\"\$EVENT\""}
  expr=${expr//github.ref/"\"\$REF\""}
  expr=${expr//'!github.event.repository.private'/"\"\$PRIVATE\" != true"}
  expr=${expr//github.event.repository.private/"\"\$PRIVATE\" == true"}
  expr=${expr//needs.changes.outputs.heavy/"\"\$HEAVY\""}
  while IFS= read -r dependency; do
    expr=${expr//needs.$dependency.result/'"success"'}
  done < <(jq -r 'keys[]' <<<"$jobs")
  if [[ $expr == *github.* || $expr == *needs.* || $expr == *startsWith* ]]; then
    fail "Unsupported job condition: $expr"
    return 2
  fi
  expr=${expr//\(/ ( }
  expr=${expr//\)/ ) }
  if bash --noprofile --norc -c "[[ $expr ]]"; then actual=success; fi
  expect success "$description" test "$actual" = "$expected"
}
export HEAVY=true PRIVATE=false REPO=example/gang HEAD_REPO=example/gang HEAD_REF=feature
for job in linux-arm64-bundle linux-arm64-checks linux-arm64 cross macos release-validation; do
  condition=$(jq -er --arg job "$job" '.[$job].if' <<<"$jobs")
  export EVENT=pull_request REF=refs/pull/1/merge
  expect_condition failure "$job skips ordinary PRs" "$condition"
  REF=refs/heads/main
  expect_condition failure "$job rejects an ordinary PR even with a main ref" "$condition"
  REF=refs/pull/1/merge HEAD_REF=release/v0.3.5 HEAD_REPO=fork/gang
  expect_condition failure "$job skips fork release PRs" "$condition"
  HEAD_REPO=example/gang
  if [[ $job == macos ]]; then expected=failure; else expected=success; fi
  expect_condition "$expected" "$job scopes trusted release PRs" "$condition"
  HEAD_REF=release-candidate/v0.3.5
  expect_condition failure "$job rejects a non-release-prefix PR" "$condition"
  HEAD_REF=release/v0.3.5 HEAVY=false
  if [[ $job == linux-arm64 || $job == release-validation ]]; then expected=success; else expected=failure; fi
  expect_condition "$expected" "$job docs-only release PR preserves intentional skips" "$condition"
  HEAD_REF=feature HEAVY=true
  EVENT=merge_group REF=refs/heads/gh-readonly-queue/main/fixture
  if [[ $job == macos ]]; then expected=failure; else expected=success; fi
  expect_condition "$expected" "$job scopes merge groups" "$condition"
  HEAVY=false
  if [[ $job == linux-arm64 || $job == release-validation ]]; then expected=success; else expected=failure; fi
  expect_condition "$expected" "$job docs-only merge group preserves intentional skips" "$condition"
  HEAVY=true
  EVENT=push
  for REF in refs/heads/main refs/heads/master refs/tags/v0.3.2; do
    expect_condition success "$job runs on push $REF" "$condition"
  done
  for REF in refs/heads/feature refs/tags/not-a-release; do
    expect_condition failure "$job skips push $REF" "$condition"
  done
  EVENT=workflow_dispatch REF=refs/heads/main
  expect_condition failure "$job does not run on dispatch" "$condition"
  EVENT=push REF=refs/heads/main HEAVY=false
  if [[ $job == linux-arm64 || $job == release-validation ]]; then
    expected=success
  else
    expected=failure
  fi
  expect_condition "$expected" "$job docs-only push preserves intentional skips" "$condition"
  HEAVY=true
  if [[ $job == macos ]]; then
    PRIVATE=true
    expect_condition failure 'private repositories intentionally skip macOS' "$condition"
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
export HEAVY=true CHANGES_RESULT=success REQUIRED_RESULT=success ARM_RESULT=success CROSS_RESULT=success
expect success 'release validation requires mandatory platforms' bash --noprofile --norc -e -u -o pipefail <"$release_body"
for PRIVATE in false true; do
  for variable in CHANGES_RESULT REQUIRED_RESULT ARM_RESULT CROSS_RESULT; do
    for status in failure cancelled skipped ''; do
      expect failure "release: heavy=true private=$PRIVATE $variable=$status" env "$variable=$status" bash --noprofile --norc -e -u -o pipefail <"$release_body"
    done
    expect failure "release: heavy=true private=$PRIVATE missing $variable" env -u "$variable" bash --noprofile --norc -e -u -o pipefail <"$release_body"
  done
  for variable in CHANGES_RESULT REQUIRED_RESULT ARM_RESULT; do
    for status in failure cancelled skipped ''; do
      expect failure "release: heavy=false private=$PRIVATE $variable=$status" env HEAVY=false "$variable=$status" bash --noprofile --norc -e -u -o pipefail <"$release_body"
    done
    expect failure "release: heavy=false private=$PRIVATE missing $variable" env -u "$variable" HEAVY=false bash --noprofile --norc -e -u -o pipefail <"$release_body"
  done
  expect success "release: heavy=false private=$PRIVATE cross skipped" env HEAVY=false CROSS_RESULT=skipped bash --noprofile --norc -e -u -o pipefail <"$release_body"
  expect success "release: heavy=false private=$PRIVATE cross success" env HEAVY=false bash --noprofile --norc -e -u -o pipefail <"$release_body"
  for status in failure cancelled ''; do
    expect failure "release: heavy=false private=$PRIVATE cross=$status" env HEAVY=false CROSS_RESULT="$status" bash --noprofile --norc -e -u -o pipefail <"$release_body"
  done
  expect failure "release: heavy=false private=$PRIVATE missing cross" env -u CROSS_RESULT HEAVY=false bash --noprofile --norc -e -u -o pipefail <"$release_body"
  for status in success failure cancelled skipped in_progress ''; do
    expect success "release: private=$PRIVATE macos=$status" env MACOS_RESULT="$status" bash --noprofile --norc -e -u -o pipefail <"$release_body"
  done
  expect success "release: private=$PRIVATE macos unset" env -u MACOS_RESULT bash --noprofile --norc -e -u -o pipefail <"$release_body"
  expect failure 'release: heavy=false cannot bypass architecture validation' env HEAVY=false ARM_RESULT=skipped CROSS_RESULT=skipped bash --noprofile --norc -e -u -o pipefail <"$release_body"
done
expect failure 'release: missing heavy output' env -u HEAVY bash --noprofile --norc -e -u -o pipefail <"$release_body"
for heavy in '' invalid; do
  expect failure "release: malformed heavy output '$heavy'" env HEAVY="$heavy" bash --noprofile --norc -e -u -o pipefail <"$release_body"
done

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
