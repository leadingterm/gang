#!/usr/bin/env bash
# Exercise the actual deployment step with a controlled GitHub boundary.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
ln -s "$script_dir/.." "$scratch/.gang"
yq -er '.jobs.apply.steps[] | select(.name == "Verify a real remote build") | .run' \
  "$script_dir/../.github/workflows/deploy.yml" >"$scratch/step.sh"
export GITHUB_REPOSITORY=example-org/example GITHUB_REF_NAME=main
export GITHUB_SHA=0123456789012345678901234567890123456789 GITHUB_RUN_ID=42 GITHUB_RUN_ATTEMPT=2
export GH_TOKEN=fixture REMOTE_BUILD_WORKFLOW=remote-build.yml REMOTE_BUILD_APPEAR_ATTEMPTS=3 REMOTE_BUILD_APPEAR_DELAY=2 CALLS="$scratch/calls"
gh() {
  local sha
  case "$1 $2" in
    'api --method')
      [[ $# == 14 && $3 == POST && $4 == "repos/$GITHUB_REPOSITORY/actions/workflows/$REMOTE_BUILD_WORKFLOW/dispatches" &&
        $5 == -H && $6 == 'X-GitHub-Api-Version: 2026-03-10' && $7 == -f && $8 == ref=main &&
        $9 == -f && ${10} == 'inputs[salt]=deploy-42-2' && ${11} == -F && ${12} == return_run_details=true &&
        ${13} == --jq && ${14} == .workflow_run_id ]] || return 1
      echo dispatch >>"$CALLS"
      [[ $SCENARIO != dispatch_failure ]] || return 1
      if [[ $SCENARIO == missing_id ]]; then echo null; else echo 123; fi
      ;;
    'run watch')
      [[ $# == 6 && $3 == 123 && $4 == --repo && $5 == "$GITHUB_REPOSITORY" && $6 == --exit-status ]] || return 1
      echo watch >>"$CALLS"
      [[ $SCENARIO != watch_failure ]]
      ;;
    'run view')
      [[ $# == 7 && $3 == 123 && $4 == --repo && $5 == "$GITHUB_REPOSITORY" && $6 == --json ]] || return 1
      if [[ $7 == databaseId ]]; then
        echo appear >>"$CALLS"
        [[ $SCENARIO != never_appears ]] || return 1
        if [[ $SCENARIO == transient && $(grep -c ^appear "$CALLS") -lt 3 ]]; then return 1; fi
        return 0
      fi
      [[ $7 == headSha,event,status,conclusion ]] || return 1
      echo view >>"$CALLS"
      if [[ $SCENARIO == wrong_sha ]]; then sha=wrong; else sha=$GITHUB_SHA; fi
      printf '{"headSha":"%s","event":"workflow_dispatch","status":"completed","conclusion":"success"}\n' "$sha"
      ;;
    *) return 1 ;;
  esac
}
sleep() { [[ $# == 1 && $1 == 2 ]] && echo sleep >>"$CALLS"; }
export -f gh sleep
for SCENARIO in success transient never_appears dispatch_failure watch_failure wrong_sha missing_id invalid_filename; do
  export SCENARIO
  if [[ $SCENARIO == invalid_filename ]]; then REMOTE_BUILD_WORKFLOW='../bad.yml'; else REMOTE_BUILD_WORKFLOW=remote-build.yml; fi
  : >"$CALLS"
  if (cd "$scratch" && bash "$scratch/step.sh") >"$scratch/output" 2>&1; then
    [[ $SCENARIO == success || $SCENARIO == transient ]]
  else
    [[ $SCENARIO != success && $SCENARIO != transient ]]
  fi
  case "$SCENARIO" in
    success) expected=$'dispatch\nappear\nwatch\nview' ;;
    transient) expected=$'dispatch\nappear\nsleep\nappear\nsleep\nappear\nwatch\nview' ;;
    never_appears) expected=$'dispatch\nappear\nsleep\nappear\nsleep\nappear'; grep -Fq '::error::remote build did not appear:' "$scratch/output" ;;
    dispatch_failure) expected=dispatch; grep -Fq '::error::cannot dispatch remote build:' "$scratch/output" ;;
    watch_failure) expected=$'dispatch\nappear\nwatch'; grep -Fq "::error::remote build failed: https://github.com/$GITHUB_REPOSITORY/actions/runs/123" "$scratch/output" ;;
    wrong_sha) expected=$'dispatch\nappear\nwatch\nview'; grep -Fq '::error::remote build does not match deployed commit or did not succeed:' "$scratch/output" ;;
    missing_id) expected=dispatch; grep -Fq '::error::cannot dispatch remote build:' "$scratch/output" ;;
    invalid_filename) expected=; grep -Fq '::error::invalid remote-build workflow filename' "$scratch/output" ;;
  esac
  [[ $(<"$CALLS") == "$expected" ]]
done
printf '8 cases, 0 failures\n'
