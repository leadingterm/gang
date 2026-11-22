#!/usr/bin/env bash
# Exercise workflow glue with fake tools; Bash/coreutils are pinned by the test shell,
# and jq is a declared Bazel data dependency. No AWS or GitHub calls leave this fixture.
set -euo pipefail

script="$PWD/$REVIEWED_PLAN"
fixtures="$TEST_TMPDIR/fixtures"
mkdir -p "$fixtures"
ln -s "$PWD/$REVIEWED_JQ" "$fixtures/jq"
cat >"$fixtures/tofu" <<'TOFU'
#!/usr/bin/env bash
set -euo pipefail
shift # -chdir
case "$1" in
  plan) printf 'saved plan\n' >"$RUNNER_TEMP/infra.tfplan"; exit 2 ;;
  show) printf '{}\n' ;;
  *) exit 90 ;;
esac
TOFU
cat >"$fixtures/git" <<'GIT'
#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == 'rev-parse HEAD^{tree}' ]] || exit 90
printf '1111111111111111111111111111111111111111\n'
GIT
cat >"$fixtures/gh" <<'GH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == 'api --paginate repos/example-org/gang/issues/7/comments' ]]; then
  printf '[]\n'
elif [[ "$*" == "api --method POST repos/example-org/gang/issues/7/comments --input $RUNNER_TEMP/infra-comment.json" ]]; then
  cp "$RUNNER_TEMP/infra-comment.json" "$RUNNER_TEMP/published.json"
else
  exit 90
fi
GH
cat >"$fixtures/gang" <<'GANG'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1 $2" == 'config check' ]]; then
  [[ "$(cat "$3")" == 'planned configuration' ]] || exit 90
  exit 0
fi
[[ "$1" == deploy ]] || exit 90
command="$2"
shift 2
case "$command" in
  comment-id) cat >/dev/null; exit 0 ;;
  configuration) cat >/dev/null; printf 'planned configuration'; exit 0 ;;
  policy | comment) ;;
  *) exit 90 ;;
esac
if [[ "$command" == comment ]]; then
  case "$1" in
    --tree) [[ "$2" == 1111111111111111111111111111111111111111 ]] || exit 90 ;;
    --manifest) [[ "$2" == "$RUNNER_TEMP/manifest.json" ]] || exit 90 ;;
    *) exit 90 ;;
  esac
  shift 2
fi
# Compare NUL-delimited argv, not joined text: spaces, quotes, backslashes and glob
# characters must survive as one exact address argument.
printf '%s\0' "$@" >"$RUNNER_TEMP/actual-args"
cmp "$EXPECTED_ARGS" "$RUNNER_TEMP/actual-args"
cat >/dev/null
printf '%s\n' "$command" >>"$RUNNER_TEMP/checked-calls"
if [[ "$command" == policy ]]; then
  blocked=false
  [[ "$POLICY_RC" == 0 ]] || blocked=true
  printf 'has_changes=true\nblocked=%s\n' "$blocked"
  exit "$POLICY_RC"
fi
printf '{"body":"reviewed plan"}\n'
GANG
chmod +x "$fixtures/tofu" "$fixtures/git" "$fixtures/gh" "$fixtures/gang"
export PATH="$fixtures:$PATH" GANG="$fixtures/gang"
export GITHUB_REPOSITORY=example-org/gang PR_NUMBER=7 GH_TOKEN=fixture
failures=0
cases=0

run_case() {
  local name="$1" input="$2" command="$3" policy_rc="$4" expected_rc="$5"
  shift 5
  export RUNNER_TEMP="$TEST_TMPDIR/$name" ALLOW_ASG_DESTROY="$input" POLICY_RC="$policy_rc"
  mkdir -p "$RUNNER_TEMP"
  export GITHUB_OUTPUT="$RUNNER_TEMP/outputs" EXPECTED_ARGS="$RUNNER_TEMP/expected-args"
  : >"$GITHUB_OUTPUT"
  # printf with no arguments still writes one NUL, as does the fake for empty argv.
  printf '%s\0' "$@" >"$EXPECTED_ARGS"
  printf '{}\n' >"$RUNNER_TEMP/infra-plan.json"
  if [[ "$name" == *manifest* ]]; then
    printf '{}\n' >"$RUNNER_TEMP/manifest.json"
  fi
  local rc=0
  bash "$script" "$command" >"$RUNNER_TEMP/out" 2>&1 || rc=$?
  cases=$((cases + 1))
  local checked_command=policy
  if [[ "$command" == publish ]]; then checked_command=comment; fi
  if [[ "$rc" != "$expected_rc" || ! -f "$RUNNER_TEMP/checked-calls" ]] ||
    [[ "$(cat "$RUNNER_TEMP/checked-calls")" != "$checked_command" ]]; then
    printf 'FAIL: %s: exit %s, expected %s\n' "$name" "$rc" "$expected_rc" >&2
    cat "$RUNNER_TEMP/out" >&2
    failures=$((failures + 1))
    return
  fi
  if [[ "$command" == publish ]]; then
    [[ "$(cat "$RUNNER_TEMP/published.json")" == '{"body":"reviewed plan"}' ]] || failures=$((failures + 1))
  elif [[ "$command" == plan || "$expected_rc" == 0 ]]; then
    local blocked=false
    [[ "$policy_rc" == 0 ]] || blocked=true
    [[ "$(cat "$GITHUB_OUTPUT")" == $'has_changes=true\nblocked='"$blocked" ]] || failures=$((failures + 1))
  else
    [[ ! -s "$GITHUB_OUTPUT" ]] || failures=$((failures + 1))
  fi
}

first='module.gang.aws_autoscaling_group.worker["arm spot"]'
second='module.nested.aws_autoscaling_group.other["quote\" and *"]'
for command in plan publish verify; do
  run_case "$command-none" '' "$command" 0 0
  run_case "$command-blank" $'\n\n' "$command" 0 0
  run_case "$command-one" "$first" "$command" 0 0 --allow-asg-destroy "$first"
  run_case "$command-multiple" "$first"$'\n\n'"$second"$'\n' "$command" 0 0 \
    --allow-asg-destroy "$first" --allow-asg-destroy "$second"
done
run_case publish-manifest "$first" publish 0 0 --allow-asg-destroy "$first"
run_case plan-blocked '' plan 3 3
run_case verify-blocked "$first" verify 3 1 --allow-asg-destroy "$first"
run_case verify-policy-error '' verify 2 1
printf '%s cases, %s failures\n' "$cases" "$failures"
exit "$((failures > 0))"
