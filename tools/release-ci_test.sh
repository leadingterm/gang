#!/usr/bin/env bash
# Exercise actual release selectors with a controlled GitHub boundary, without network calls.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
body="$scratch/locate.sh"
yq -er '.jobs.locate.steps[] | select(.name == "Find the CI run to release") | .run' "$script_dir/../.github/workflows/release.yml" >"$body"
selector="$scratch/binaries.sh"
yq -er '.jobs.publish.steps[] | select(.name == "Select available CI binaries") | .run' "$script_dir/../.github/workflows/release.yml" >"$selector"
[[ $(yq -er '.jobs.publish.steps[] | select(.id == "binaries") | .env.RUN_ID' "$script_dir/../.github/workflows/release.yml") == "\${{ needs.locate.outputs.run-id }}" ]]
[[ $(yq -er '.jobs.publish.steps[] | select(.with["artifact-ids"]) | .with["artifact-ids"]' "$script_dir/../.github/workflows/release.yml") == "\${{ steps.binaries.outputs.artifact-ids }}" ]]
[[ $(yq -er '.jobs.publish.steps[] | select(.with["artifact-ids"]) | .with["run-id"]' "$script_dir/../.github/workflows/release.yml") == "\${{ needs.locate.outputs.run-id }}" ]]
export GH_REPO=example-org/gang SHA=0123456789012345678901234567890123456789
export HEAD_SHA=1111111111111111111111111111111111111111
export BASE_SHA=2222222222222222222222222222222222222222
export TREE_SHA=3333333333333333333333333333333333333333
export OTHER_SHA=4444444444444444444444444444444444444444
export GITHUB_OUTPUT="$scratch/output"
gh() {
  local endpoint
  [[ $1 == api ]] || return 1
  shift
  if [[ ${1:-} == --paginate ]]; then shift; fi
  [[ $# == 1 ]] || return 1
  endpoint=$1
  case "$endpoint" in
    "repos/$GH_REPO/actions/workflows/ci.yml/runs?head_sha=$SHA&per_page=100")
      if [[ $CASE == api_failure ]]; then return 23; fi
      if [[ $CASE == direct* ]]; then
        local event=push direct_sha=$SHA branch=main head_repo=",\"head_repository\":{\"full_name\":\"$GH_REPO\"}"
        if [[ $CASE == direct_dispatch ]]; then event=workflow_dispatch; fi
        if [[ $CASE == direct_wrong_sha ]]; then direct_sha=$OTHER_SHA; fi
        if [[ $CASE == direct_wrong_branch ]]; then branch=other; fi
        if [[ $CASE == direct_fork_repo ]]; then head_repo=',"head_repository":{"full_name":"fork/gang"}'; fi
        if [[ $CASE == direct_missing_repo ]]; then head_repo=''; fi
        if [[ $CASE == direct_null_repo ]]; then head_repo=',"head_repository":null'; fi
        printf '{"workflow_runs":[{"id":101,"event":"%s","head_branch":"%s","head_sha":"%s","status":"in_progress","conclusion":null,"created_at":"2026-01-01"%s}]}\n' "$event" "$branch" "$direct_sha" "$head_repo"
      else
        printf '{"workflow_runs":[]}\n'
      fi ;;
    "repos/$GH_REPO/commits/$SHA")
      local parents="{\"sha\":\"$BASE_SHA\"}"
      if [[ $CASE == merge_parent ]]; then parents="$parents,$parents"; fi
      if [[ $CASE == root_commit ]]; then parents=''; fi
      printf '{"commit":{"tree":{"sha":"%s"}},"parents":[%s]}\n' "$TREE_SHA" "$parents" ;;
    "repos/$GH_REPO/commits/$SHA/pulls?per_page=100")
      if [[ $CASE == no_pr || $CASE == direct* ]]; then printf '[]\n'; return; fi
      local base=$BASE_SHA head_repo=",\"repo\":{\"full_name\":\"$GH_REPO\"}"
      if [[ $CASE == wrong_base ]]; then base=$OTHER_SHA; fi
      if [[ $CASE == fork_associated_pr ]]; then head_repo=',"repo":{"full_name":"fork/gang"}'; fi
      if [[ $CASE == missing_associated_repo ]]; then head_repo=''; fi
      if [[ $CASE == null_associated_repo ]]; then head_repo=',"repo":null'; fi
      printf '[{"base":{"sha":"%s"},"head":{"sha":"%s"%s}}]\n' "$base" "$HEAD_SHA" "$head_repo" ;;
    "repos/$GH_REPO/commits/$HEAD_SHA")
      local tree=$TREE_SHA
      if [[ $CASE == wrong_tree ]]; then tree=$OTHER_SHA; fi
      printf '{"commit":{"tree":{"sha":"%s"}}}\n' "$tree" ;;
    "repos/$GH_REPO/actions/workflows/ci.yml/runs?head_sha=$HEAD_SHA&event=pull_request&per_page=100")
      local pr_sha=$HEAD_SHA pr_event=pull_request head_repo=",\"head_repository\":{\"full_name\":\"$GH_REPO\"}"
      if [[ $CASE == wrong_pr_sha ]]; then pr_sha=$OTHER_SHA; fi
      if [[ $CASE == wrong_pr_event ]]; then pr_event=push; fi
      if [[ $CASE == fork_pr_repo ]]; then head_repo=',"head_repository":{"full_name":"fork/gang"}'; fi
      if [[ $CASE == missing_pr_repo ]]; then head_repo=''; fi
      if [[ $CASE == null_pr_repo ]]; then head_repo=',"head_repository":null'; fi
      printf '{"workflow_runs":[{"id":202,"event":"%s","head_sha":"%s","conclusion":"failure","created_at":"2026-01-01","pull_requests":[]%s}]}\n' "$pr_event" "$pr_sha" "$head_repo" ;;
    "repos/$GH_REPO/actions/runs/101/jobs?filter=latest&per_page=100" | "repos/$GH_REPO/actions/runs/202/jobs?filter=latest&per_page=100")
      if [[ $CASE == jobs_api_failure ]]; then return 23; fi
      if [[ $CASE == invalid_jobs ]]; then printf '{"jobs":null}\n'; return; fi
      if [[ $CASE == missing_required ]]; then printf '{"jobs":[]}\n'; return; fi
      local conclusion='"success"'
      if [[ $CASE == failed_pr ]]; then conclusion='"failure"'; fi
      if [[ $CASE == pending_required ]]; then conclusion=null; fi
      if [[ $CASE == duplicate_required ]]; then
        printf '{"jobs":[{"name":"required","conclusion":"success"},{"name":"required","conclusion":"success"}]}\n'
        return
      fi
      printf '{"jobs":[{"name":"required","conclusion":%s}]}\n' "$conclusion" ;;
    "repos/$GH_REPO/actions/runs/101/artifacts?per_page=100" | "repos/$GH_REPO/actions/runs/202/artifacts?per_page=100")
      if [[ $CASE == artifact_api_failure ]]; then return 23; fi
      if [[ $CASE == invalid_artifacts ]]; then printf '{"artifacts":null}\n'; return; fi
      local expired=false linux=',{"id":2,"name":"gang-aarch64-unknown-linux-musl","expired":false}' darwin=',{"id":3,"name":"gang-aarch64-apple-darwin","expired":false}'
      if [[ $CASE == missing_artifact ]]; then linux=''; fi
      if [[ $CASE == no_darwin ]]; then darwin=''; fi
      if [[ $CASE == expired_darwin ]]; then darwin=',{"id":3,"name":"gang-aarch64-apple-darwin","expired":true}'; fi
      if [[ $CASE == expired_artifact ]]; then expired=true; fi
      printf '{"artifacts":[{"id":1,"name":"gang-x86_64-unknown-linux-musl","expired":%s}%s%s,{"id":4,"name":"gang-other","expired":false}]}\n' "$expired" "$linux" "$darwin" ;;
    *) printf 'unexpected gh endpoint: %s\n' "$endpoint" >&2; return 1 ;;
  esac
}
export -f gh
cases=0
for scenario in direct:101 direct_dispatch:101 squash:202 dispatch:202 no_darwin:202 expired_darwin:202 direct_wrong_sha:fail direct_wrong_branch:fail direct_fork_repo:fail direct_missing_repo:fail direct_null_repo:fail fork_associated_pr:fail missing_associated_repo:fail null_associated_repo:fail fork_pr_repo:fail missing_pr_repo:fail null_pr_repo:fail wrong_tree:fail wrong_base:fail wrong_pr_sha:fail wrong_pr_event:fail failed_pr:fail pending_required:fail missing_required:fail duplicate_required:fail jobs_api_failure:fail invalid_jobs:fail missing_artifact:fail expired_artifact:fail no_pr:fail merge_parent:fail root_commit:fail api_failure:fail; do
  export CASE=${scenario%:*} EVENT=push
  expected=${scenario#*:}
  if [[ $CASE == dispatch ]]; then EVENT=workflow_dispatch; fi
  : >"$GITHUB_OUTPUT"
  status=0
  bash --noprofile --norc -euo pipefail "$body" >"$scratch/case.log" 2>&1 || status=$?
  if [[ $expected == fail ]]; then
    if (( status == 0 )) || [[ -s $GITHUB_OUTPUT ]]; then
      printf '%s unexpectedly selected a run\n' "$CASE" >&2
      cat "$scratch/case.log" >&2
      exit 1
    fi
  elif (( status != 0 )) || [[ $(<"$GITHUB_OUTPUT") != "run-id=$expected" ]]; then
    printf '%s did not select run %s\n' "$CASE" "$expected" >&2
    cat "$scratch/case.log" >&2
    exit 1
  fi
  ((cases += 1))
done
export RUN_ID=202
for scenario in squash:1,2,3 no_darwin:1,2 expired_darwin:1,2 missing_artifact:fail expired_artifact:fail artifact_api_failure:fail invalid_artifacts:fail; do
  export CASE=${scenario%:*}
  expected=${scenario#*:}
  : >"$GITHUB_OUTPUT"
  status=0
  bash --noprofile --norc -euo pipefail "$selector" >"$scratch/case.log" 2>&1 || status=$?
  if [[ $expected == fail ]]; then
    if (( status == 0 )) || [[ -s $GITHUB_OUTPUT ]]; then
      printf '%s unexpectedly selected artifacts\n' "$CASE" >&2
      cat "$scratch/case.log" >&2
      exit 1
    fi
  elif (( status != 0 )) || [[ $(<"$GITHUB_OUTPUT") != "artifact-ids=$expected" ]]; then
    printf '%s did not select artifacts %s\n' "$CASE" "$expected" >&2
    cat "$scratch/case.log" >&2
    exit 1
  fi
  ((cases += 1))
done
printf '%d cases, 0 failures\n' "$cases"
