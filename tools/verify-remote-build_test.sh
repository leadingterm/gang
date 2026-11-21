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
export GH_TOKEN=fixture REMOTE_BUILD_WORKFLOW=remote-build.yml CALLS="$scratch/calls"
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
      [[ $# == 7 && $3 == 123 && $4 == --repo && $5 == "$GITHUB_REPOSITORY" && $6 == --json && $7 == headSha,event,status,conclusion ]] || return 1
      echo view >>"$CALLS"
      if [[ $SCENARIO == wrong_sha ]]; then sha=wrong; else sha=$GITHUB_SHA; fi
      printf '{"headSha":"%s","event":"workflow_dispatch","status":"completed","conclusion":"success"}\n' "$sha"
      ;;
    *) return 1 ;;
  esac
}
export -f gh
for SCENARIO in success dispatch_failure watch_failure wrong_sha missing_id; do
  export SCENARIO
  : >"$CALLS"
  if (cd "$scratch" && bash "$scratch/step.sh") >"$scratch/output" 2>&1; then
    [[ $SCENARIO == success ]]
  else
    [[ $SCENARIO != success ]]
  fi
  case "$SCENARIO" in
    success) expected=$'dispatch\nwatch\nview' ;;
    dispatch_failure) expected=dispatch; grep -Fq '::error::cannot dispatch remote build:' "$scratch/output" ;;
    watch_failure) expected=$'dispatch\nwatch'; grep -Fq "::error::remote build failed: https://github.com/$GITHUB_REPOSITORY/actions/runs/123" "$scratch/output" ;;
    wrong_sha) expected=$'dispatch\nwatch\nview'; grep -Fq '::error::remote build does not match deployed commit or did not succeed:' "$scratch/output" ;;
    missing_id) expected=dispatch; grep -Fq '::error::cannot dispatch remote build:' "$scratch/output" ;;
  esac
  [[ $(<"$CALLS") == "$expected" ]]
done
printf '5 cases, 0 failures\n'
