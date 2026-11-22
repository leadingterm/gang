#!/usr/bin/env bash
# Exercise the graph guard and execute the actual CI aggregator bodies.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
workflow="$script_dir/../.github/workflows/ci.yml"
pr_workflow="$script_dir/../.github/workflows/pr.yml"
guard="$script_dir/check-required-jobs.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
fixture="$scratch/ci.yml"
pr_fixture="$scratch/pr.yml"
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

reset_fixtures() {
  cp "$workflow" "$fixture"
  cp "$pr_workflow" "$pr_fixture"
}

expect success 'current workflow graph' "$guard" "$workflow" "$pr_workflow"
critical=(nativelink-memory-tests nix-vm lint-checks linux-x86_64-shards linux-x86_64-bundle linux-arm64-bundle linux-arm64-checks cross-build e2e-gate-1-shards e2e-gate-2-shards e2e-v035-upgrade)
for job in "${critical[@]}"; do
  reset_fixtures
  job="$job" yq -i 'del(.jobs.release-validation.needs[] | select(. == strenv(job)))' "$fixture"
  expect failure "release gate directly needs $job" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].if = "true"' "$fixture"
  expect failure "$job is queue-only" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)]."continue-on-error" = true' "$fixture"
  expect failure "$job cannot ignore failures" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].steps[0]."continue-on-error" = true' "$fixture"
  expect failure "$job steps cannot ignore failures" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].steps[0].with.ref = "main"' "$fixture"
  expect failure "$job checks out queue SHA" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].steps[1].if = "false"' "$fixture"
  expect failure "$job critical step cannot skip" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].permissions.contents = "write"' "$fixture"
  expect failure "$job cannot escalate permissions" "$guard" "$fixture" "$pr_fixture"
done

for file in ci pr; do
  if [[ $file == ci ]]; then target=$fixture; source=$workflow; else target=$pr_fixture; source=$pr_workflow; fi
  reset_fixtures
  yq -i '.jobs.readiness.needs = []' "$target"
  expect failure "$file readiness requires producer" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.jobs.readiness.if = "always()"' "$target"
  expect failure "$file readiness event scope" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.jobs.readiness.strategy.matrix.check = ["lint"]' "$target"
  expect failure "$file readiness exposes all six names" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.jobs.readiness.strategy.matrix.extra = [1]' "$target"
  expect failure "$file readiness cannot add matrix dimensions" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.jobs.readiness.strategy.matrix.exclude = [{"check": "lint"}]' "$target"
  expect failure "$file readiness cannot exclude checks" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.jobs.readiness.steps[0].env.RESULT = "success"' "$target"
  expect failure "$file readiness uses real result" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.jobs.readiness.steps[0].run = "true"' "$target"
  expect failure "$file readiness cannot be a green stub" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  yq -i '.permissions.contents = "write"' "$target"
  expect failure "$file permissions are read-only" "$guard" "$fixture" "$pr_fixture"
  yq -o=json '.jobs.readiness.steps[0].run' "$source" | jq -r . >"$scratch/readiness.sh"
  expect success "$file readiness accepts success" env RESULT=success bash --noprofile --norc -e -u -o pipefail "$scratch/readiness.sh"
  for value in failure cancelled skipped ''; do
    expect failure "$file readiness rejects '$value'" env RESULT="$value" bash --noprofile --norc -e -u -o pipefail "$scratch/readiness.sh"
  done
  expect failure "$file readiness rejects missing RESULT" env -u RESULT bash --noprofile --norc -e -u -o pipefail "$scratch/readiness.sh"
done

reset_fixtures
yq -i '.on.push = {}' "$fixture"
expect failure 'queue workflow excludes push' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.on.merge_group.types = ["destroyed"]' "$fixture"
expect failure 'queue trigger only checks_requested' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.on.push = {}' "$pr_fixture"
expect failure 'PR workflow excludes push' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.release-validation.if = "always()"' "$fixture"
expect failure 'release gate is queue-only' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.release-validation.steps[0].env.NEEDS_RESULTS = "success"' "$fixture"
expect failure 'release gate checks real results' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.release-validation.steps[0].run = "true"' "$fixture"
expect failure 'release gate cannot be a green stub' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.release-validation.needs += ["macos"]' "$fixture"
expect failure 'advisory macOS cannot join release gate' "$guard" "$fixture" "$pr_fixture"
for job in nix-vm linux-arm64-checks e2e-gate-1-shards e2e-gate-2-shards e2e-v035-upgrade; do
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].needs = []' "$fixture"
  expect failure "$job requires its bundle" "$guard" "$fixture" "$pr_fixture"
done
reset_fixtures
yq -i '.jobs.nix-vm.needs = ["linux-x86_64-bundle"]' "$fixture"
expect failure 'nix-vm requires the ARM bundle, not the x86 bundle' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.nix-vm.needs += ["linux-x86_64-bundle"]' "$fixture"
expect failure 'nix-vm cannot add another dependency' "$guard" "$fixture" "$pr_fixture"
for job in nativelink-memory-tests nix-vm cross-build linux-arm64-checks linux-x86_64-shards; do
  reset_fixtures
  job="$job" yq -i '(.jobs[strenv(job)].steps[] | select(.name == "Run NativeLink memory tests" or .name == "Run NixOS VM checks" or .name == "Cross-build gang" or .name == "Test") | .run) = "true"' "$fixture"
  expect failure "$job must run its actual validation" "$guard" "$fixture" "$pr_fixture"
done
for job in linux-x86_64-bundle linux-arm64-bundle; do
  reset_fixtures
  job="$job" yq -i 'del(.jobs[strenv(job)].steps[] | select(.uses == "./.github/actions/upload-gang"))' "$fixture"
  expect failure "$job uploads the release binary" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i 'del(.jobs[strenv(job)].steps[] | select(.with."if-no-files-found" == "error"))' "$fixture"
  expect failure "$job uploads the harness" "$guard" "$fixture" "$pr_fixture"
done
reset_fixtures
yq -i '(.jobs.linux-x86_64-bundle.steps[] | select(.with.name == "bazel-metrics-x86-bundle") | .with.path) = "missing"' "$fixture"
expect failure 'bundle metrics preserve both artifacts' "$guard" "$fixture" "$pr_fixture"
for job in linux-x86_64-shards e2e-gate-1-shards e2e-gate-2-shards; do
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].strategy.matrix.shard = [1]' "$fixture"
  expect failure "$job needs every shard" "$guard" "$fixture" "$pr_fixture"
  reset_fixtures
  job="$job" yq -i '.jobs[strenv(job)].strategy.matrix.exclude = [{"shard": 1}]' "$fixture"
  expect failure "$job cannot exclude shards" "$guard" "$fixture" "$pr_fixture"
done
for job in e2e-gate-1-shards e2e-gate-2-shards; do
  reset_fixtures
  job="$job" yq -i '(.jobs[strenv(job)].steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) = "--gate 1"' "$fixture"
  expect failure "$job uses the real shard selection" "$guard" "$fixture" "$pr_fixture"
done
reset_fixtures
yq -i 'del(.jobs.e2e-v035-upgrade)' "$fixture"
expect failure 'v035 upgrade job is required' "$guard" "$fixture" "$pr_fixture"
for mutation in \
  '--gate 2 --scenario rolling-upgrade' \
  "--gate 1 --scenario rolling-upgrade --previous-gang \${{ runner.temp }}/e2e/gang-v035 --previous-fixture \${{ runner.temp }}/e2e/deployment-v035.json" \
  "--gate 2 --scenario config-refetch-restart --previous-gang \${{ runner.temp }}/e2e/gang-v035 --previous-fixture \${{ runner.temp }}/e2e/deployment-v035.json" \
  "--gate 2 --scenario rolling-upgrade --previous-gang \${{ runner.temp }}/e2e/gang-previous --previous-fixture \${{ runner.temp }}/e2e/deployment-v035.json" \
  "--gate 2 --scenario rolling-upgrade --previous-gang \${{ runner.temp }}/e2e/gang-v035 --previous-fixture \${{ runner.temp }}/e2e/deployment-previous.json"; do
  reset_fixtures
  mutation="$mutation" yq -i '(.jobs.e2e-v035-upgrade.steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) = strenv(mutation)' "$fixture"
  expect failure 'v035 upgrade requires the exact scenario and published pair' "$guard" "$fixture" "$pr_fixture"
done
reset_fixtures
yq -i '.jobs.e2e-v035-upgrade.runs-on = "ubuntu-24.04-arm"' "$fixture"
expect failure 'v035 upgrade requires x86 runners' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '(.jobs.lint-checks.steps[] | select(.run == "tools/lint.sh --portable") | .run) = "true"' "$fixture"
expect failure 'queue lint executes portable lint' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.lint-checks.steps[0].with."fetch-depth" = 1' "$fixture"
expect failure 'portable lint requires full history' "$guard" "$fixture" "$pr_fixture"
for job in lint-checks linux-x86_64-shards; do
  reset_fixtures
  job="$job" yq -i 'del(.jobs[strenv(job)].steps[] | select(.uses == "./.github/actions/save-bazel-cache"))' "$fixture"
  expect failure "$job exports working set" "$guard" "$fixture" "$pr_fixture"
done
reset_fixtures
yq -i '(.jobs.preflight.steps[] | select(.name == "Run PR preflight") | .run) = "true"' "$pr_fixture"
expect failure 'PR must execute preflight command' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.preflight.if = "true"' "$pr_fixture"
expect failure 'preflight must be PR-only' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i 'del(.jobs.preflight.if)' "$pr_fixture"
expect failure 'preflight must explicitly scope the event' "$guard" "$fixture" "$pr_fixture"
for variable in BASE HEAD; do
  reset_fixtures
  variable="$variable" yq -i '(.jobs.preflight.steps[] | select(.name == "Run PR preflight") | .env[strenv(variable)]) = "main"' "$pr_fixture"
  expect failure "preflight receives actual $variable" "$guard" "$fixture" "$pr_fixture"
done
reset_fixtures
mutation="\${{ github.event.pull_request.head.sha }}" yq -i '.jobs.preflight.steps[0].with.ref = strenv(mutation)' "$pr_fixture"
expect failure 'PR checks out synthetic merge SHA' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.preflight.steps[0].with."persist-credentials" = true' "$pr_fixture"
expect failure 'PR checkout cannot persist credentials' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '(.jobs.preflight.steps[] | select(.uses == "./.github/actions/save-bazel-cache") | .if) = "success()"' "$pr_fixture"
expect failure 'fork PRs cannot write caches' "$guard" "$fixture" "$pr_fixture"
reset_fixtures
yq -i '.jobs.preflight.permissions.contents = "write"' "$pr_fixture"
expect failure 'preflight cannot override permissions' "$guard" "$fixture" "$pr_fixture"

# Execute the actual release result body for every failing or missing dependency.
yq -o=json '.jobs.release-validation.steps[0].run' "$workflow" | jq -r . >"$scratch/release.sh"
results=(success success success success success success success success success success success)
expect success 'release gate accepts all success' env NEEDS_RESULTS="${results[*]}" bash --noprofile --norc -e -u -o pipefail "$scratch/release.sh"
for ((i = 0; i < 11; i++)); do
  for value in failure cancelled skipped missing; do
    results=(success success success success success success success success success success success)
    if [[ $value == missing ]]; then unset 'results[i]'; else results[i]=$value; fi
    expect failure "release gate rejects $value at $i" env NEEDS_RESULTS="${results[*]}" bash --noprofile --norc -e -u -o pipefail "$scratch/release.sh"
  done
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
  expected=$(yq -o=json '.jobs' "$local_workflow" | jq -r --arg shard_expr "\${{ matrix.shard }}" --arg total_expr "\${{ strategy.job-total }}" '
    . as $jobs
    | ["e2e-gate-1-shards", "e2e-gate-2-shards"][]
    | $jobs[.] as $job
    | ($job.strategy.matrix.shard | length | tostring) as $total
    | ($job.steps[] | select(.uses == "./.github/actions/run-e2e") | .with.args) as $args
    | $job.strategy.matrix.shard[] | tostring as $shard
    | $args | split($shard_expr) | join($shard)
    | split($total_expr) | join($total)
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
printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
