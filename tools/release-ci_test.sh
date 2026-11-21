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
workflow="$script_dir/../.github/workflows/release.yml"
yq -o=json '.jobs' "$workflow" | jq -e '
  . as $jobs
  | ($jobs.publish.needs == ["locate", "images", "amis"])
    and ($jobs.amiscopies.needs == ["locate", "amis"])
    and ($jobs.amiscopies["if"] == "${{ github.event_name == '\''push'\'' }}")
    and ($jobs.amiscopies["runs-on"] == "ubuntu-24.04-arm")
    and ($jobs.amiscopies["timeout-minutes"] == 180)
    and ($jobs.amiscopies.permissions == {"contents":"read", "actions":"read", "id-token":"write"})
    and ([$jobs.amiscopies.steps[] | select((.uses // "") | startswith("actions/checkout@")) | .with["persist-credentials"]] == [false])
    and ([$jobs.amiscopies.steps[] | select(.uses == "./.github/actions/setup-nix") | .with["cache-save"] // "false"] == ["false"])
    and ([$jobs.amiscopies.steps[] | select((.uses // "") | startswith("actions/download-artifact@")) | .with | [.name, .["run-id"]]] == [["gang-aarch64-unknown-linux-musl", "${{ needs.locate.outputs.run-id }}"]])
    and ([$jobs.amiscopies.steps[] | select(.run == "chmod +x staged/gang-aarch64-unknown-linux-musl")] | length == 1)
    and ([$jobs.amiscopies.steps[] | select((.uses // "") | startswith("aws-actions/configure-aws-credentials@")) | .with] == [$jobs.amis.steps[] | select((.uses // "") | startswith("aws-actions/configure-aws-credentials@")) | .with])
    and ([$jobs.amiscopies.steps[] | select((.uses // "") | startswith("actions/upload-artifact@"))] == [])
    and ([$jobs.amiscopies.steps[] | select(.name == "Converge the public AMI catalog") | [.run, .env.PUBLICATION_SCOPE, .env.RELEASE, .env.IMAGE_HOME_REGION, .env.IMAGE_REGIONS]] == [["infra/scripts/publish-amis.sh", "catalog", "", "${{ needs.amis.outputs.home-region }}", "${{ needs.amis.outputs.image-regions }}"]])
    and ([$jobs.amis.steps[] | select(.name == "Converge public AMIs") | [.env.PUBLICATION_SCOPE, .env.AMI_DEPLOY_REGIONS, .env.RELEASE, .env.IMAGES_DIR]] == [["${{ github.event_name == '\''push'\'' && '\''release'\'' || '\''catalog'\'' }}", "${{ vars.AMI_DEPLOY_REGIONS }}", "${{ github.event_name == '\''push'\'' && github.ref_name || '\'''\'' }}", "${{ github.event_name == '\''push'\'' && '\''images'\'' || '\'''\'' }}"]])
    and ($jobs.amis.outputs == {"home-region":"${{ steps.config.outputs.home-region }}", "image-regions":"${{ steps.config.outputs.image-regions }}"})
    and ($jobs.images.needs == "locate")
    and ($jobs.images["if"] == "${{ !(github.event_name == '\''workflow_dispatch'\'' && startsWith(github.ref, '\''refs/tags/v'\'')) }}")
    and ($jobs.images.strategy.matrix.include | length == 2)
    and ($jobs.amis["if"] == "${{ !cancelled() && ((github.event_name == '\''push'\'' && needs.locate.result == '\''success'\'' && needs.images.result == '\''success'\'') || (github.event_name == '\''workflow_dispatch'\'' && startsWith(github.ref, '\''refs/tags/v'\''))) }}")
    and ([$jobs.images.steps[] | select((.uses // "") | startswith("aws-actions/configure-aws-credentials@"))] == [])
    and ($jobs.locate.outputs == {"run-id":"${{ steps.ci.outputs.run-id }}"})
    and ([$jobs.locate.steps[] | select(.id == "dry-run")] == [])
    and ($jobs.publish["if"] | contains("github.event_name == '\''push'\''"))
    and ([$jobs.images.steps[] | select(.name == "Match the dry run'\''s images" or .with["run-id"] == "${{ needs.locate.outputs.dry-run-id }}")] == [])
    and ([$jobs.images.steps[] | select(.name == "Build the images") | .run | contains("nix build -L --rebuild")] == [true])
' >/dev/null
config_step="$scratch/config.sh"
yq -er '.jobs.amis.steps[] | select(.id == "config") | .run' "$workflow" >"$config_step"
export AMI_CONFIG_REGION=fixture-region GITHUB_ENV="$scratch/env"
aws() {
  [[ "$*" == "ssm get-parameter --region fixture-region --name /leadingterm/public-artifacts/gang --query Parameter.Value --output text" ]] || return 1
  printf '%s\n' "$CONFIG_VALUE"
}
export -f aws
export CONFIG_VALUE
for CONFIG_VALUE in \
  '{"home_region":"home-1","image_regions":["home-1","copy-2"]}' \
  '{"home_region":"bad region","image_regions":["home-1"]}' \
  '{"home_region":"home-1","image_regions":[]}' \
  '{"home_region":"home-1","image_regions":["copy-2\nbad"]}'; do
  : >"$GITHUB_ENV"
  : >"$scratch/config-output"
  status=0
  GITHUB_OUTPUT="$scratch/config-output" bash --noprofile --norc -euo pipefail "$config_step" >"$scratch/case.log" 2>&1 || status=$?
  if [[ $CONFIG_VALUE == '{"home_region":"home-1","image_regions":["home-1","copy-2"]}' ]]; then
    (( status == 0 ))
    [[ $(<"$GITHUB_ENV") == $'IMAGE_HOME_REGION=home-1\nIMAGE_REGIONS=home-1 copy-2' ]]
    [[ $(<"$scratch/config-output") == $'home-region=home-1\nimage-regions=home-1 copy-2' ]]
  else
    (( status != 0 ))
    [[ ! -s $GITHUB_ENV && ! -s $scratch/config-output ]]
  fi
done
export GH_REPO=example-org/gang SHA=0123456789012345678901234567890123456789
export OTHER_SHA=4444444444444444444444444444444444444444
export TREE=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa OTHER_TREE=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export GITHUB_OUTPUT="$scratch/output"
gh() {
  local endpoint
  [[ $1 == api ]] || return 1
  shift
  if [[ ${1:-} == --paginate ]]; then shift; fi
  [[ $# == 1 ]] || return 1
  endpoint=$1
  case "$endpoint" in
    "repos/$GH_REPO/compare/$SHA...main")
      case "$CASE" in
        compare_failure) return 23 ;;
        not_main) printf '{"status":"behind"}\n' ;;
        invalid_compare) printf '{"status":null}\n' ;;
        *) printf '{"status":"ahead"}\n' ;;
      esac ;;
    "repos/$GH_REPO/git/commits/$SHA" | "repos/$GH_REPO/git/commits/$OTHER_SHA")
      if [[ $CASE == tree_api_failure ]]; then return 23; fi
      if [[ $CASE == invalid_tree ]]; then printf '{"tree":{"sha":"invalid"}}\n'; return; fi
      if [[ $CASE == changed_tree && $endpoint == *"$OTHER_SHA" ]]; then
        printf '{"tree":{"sha":"%s"}}\n' "$OTHER_TREE"
      else
        printf '{"tree":{"sha":"%s"}}\n' "$TREE"
      fi ;;
    "repos/$GH_REPO/commits/$SHA/pulls?per_page=100")
      if [[ $CASE == prs_api_failure ]]; then return 23; fi
      if [[ $CASE == invalid_prs ]]; then printf '{"pulls":[]}\n'; return; fi
      case "$CASE" in
        pr_* | changed_tree | fork_pr | unmerged_pr | wrong_merge | wrong_base | invalid_pr)
          local merged='"2026-01-01T00:00:00Z"' repo=$GH_REPO merge=$SHA base=main head=$OTHER_SHA
          if [[ $CASE == fork_pr ]]; then repo=fork/gang; fi
          if [[ $CASE == unmerged_pr ]]; then merged=null; fi
          if [[ $CASE == wrong_merge ]]; then merge=$OTHER_SHA; fi
          if [[ $CASE == wrong_base ]]; then base=other; fi
          if [[ $CASE == invalid_pr ]]; then head=invalid; fi
          printf '[{"merged_at":%s,"base":{"ref":"%s"},"head":{"sha":"%s","repo":{"full_name":"%s"}},"merge_commit_sha":"%s"}]\n' "$merged" "$base" "$head" "$repo" "$merge" ;;
        *) printf '[]\n' ;;
      esac ;;
    "repos/$GH_REPO/actions/workflows/ci.yml/runs?head_sha=$OTHER_SHA&per_page=100")
      local event=pull_request repo=$GH_REPO run_sha=$OTHER_SHA conclusion=success
      if [[ $CASE == pr_wrong_event ]]; then event=push; fi
      if [[ $CASE == pr_wrong_repo ]]; then repo=fork/gang; fi
      if [[ $CASE == pr_wrong_sha ]]; then run_sha=$SHA; fi
      if [[ $CASE == pr_overall_failure ]]; then conclusion=failure; fi
      printf '{"workflow_runs":[{"id":101,"event":"%s","head_branch":"release/v1","head_sha":"%s","created_at":"2026-01-01","head_repository":{"full_name":"%s"},"status":"completed","conclusion":"%s"}]}\n' "$event" "$run_sha" "$repo" "$conclusion" ;;
    "repos/$GH_REPO/actions/workflows/ci.yml/runs?head_sha=$SHA&per_page=100")
      case "$CASE" in
        pr_* | changed_tree | fork_pr | unmerged_pr | wrong_merge | wrong_base | invalid_pr) printf '{"workflow_runs":[]}\n'; return ;;
      esac
      if [[ $CASE == api_failure ]]; then return 23; fi
      if [[ $CASE == invalid_runs ]]; then printf '{"workflow_runs":null}\n'; return; fi
      if [[ $CASE == malformed_json ]]; then printf '{\n'; return; fi
      local event=push run_sha=$SHA branch=main head_repo=",\"head_repository\":{\"full_name\":\"$GH_REPO\"}"
      if [[ $CASE == ci_dispatch ]]; then event=workflow_dispatch; fi
      if [[ $CASE == same_repo_pr ]]; then event=pull_request; fi
      if [[ $CASE == fork_pr ]]; then event=pull_request; head_repo=',"head_repository":{"full_name":"fork/gang"}'; fi
      if [[ $CASE == wrong_sha ]]; then run_sha=$OTHER_SHA; fi
      if [[ $CASE == wrong_branch ]]; then branch=other; fi
      if [[ $CASE == wrong_repo ]]; then head_repo=',"head_repository":{"full_name":"fork/gang"}'; fi
      if [[ $CASE == missing_repo ]]; then head_repo=''; fi
      if [[ $CASE == null_repo ]]; then head_repo=',"head_repository":null'; fi
      printf '{"workflow_runs":[{"id":101,"event":"%s","head_branch":"%s","head_sha":"%s","created_at":"2026-01-01","status":"completed","conclusion":"success"%s}]}\n' "$event" "$branch" "$run_sha" "$head_repo"
      if [[ $CASE == latest || $CASE == fallback_required || $CASE == fallback_validation || $CASE == fallback_artifact || $CASE == fallback_api_failure ]]; then
        printf '{"workflow_runs":[{"id":202,"event":"push","head_branch":"main","head_sha":"%s","created_at":"2026-01-02","head_repository":{"full_name":"%s"},"status":"completed","conclusion":"success"}]}\n' "$SHA" "$GH_REPO"
      fi
      if [[ $CASE == newer_tag_push ]]; then
        printf '{"workflow_runs":[{"id":202,"event":"push","head_branch":"v1.2.3","head_sha":"%s","created_at":"2026-01-02","head_repository":{"full_name":"%s"},"status":"completed","conclusion":"success"}]}\n' "$SHA" "$GH_REPO"
      fi ;;
    "repos/$GH_REPO/actions/runs/101/jobs?filter=latest&per_page=100" | "repos/$GH_REPO/actions/runs/202/jobs?filter=latest&per_page=100")
      local fixture=${CASE#pr_}
      if [[ $CASE == jobs_api_failure || ( $CASE == fallback_api_failure && $endpoint == *"/202/"* ) ]]; then return 23; fi
      if [[ $CASE == invalid_jobs ]]; then printf '{"jobs":null}\n'; return; fi
      if [[ $CASE == malformed_job ]]; then printf '{"jobs":[{"name":42,"conclusion":"success"}]}\n'; return; fi
      local conclusion='"success"' validation='"success"'
      if [[ $fixture == missing_required ]]; then
        printf '{"jobs":[{"name":"release-validation","conclusion":"success"}]}\n'
        return
      fi
      if [[ $fixture == failed_required || ( $CASE == fallback_required && $endpoint == *"/202/"* ) ]]; then conclusion='"failure"'; fi
      if [[ $CASE == pending_required ]]; then conclusion=null; fi
      if [[ $fixture == duplicate_required ]]; then
        printf '{"jobs":[{"name":"required","conclusion":"success"},{"name":"required","conclusion":"success"},{"name":"release-validation","conclusion":"success"}]}\n'
        return
      fi
      if [[ $fixture == missing_validation ]]; then
        printf '{"jobs":[{"name":"required","conclusion":"success"}]}\n'
        return
      fi
      if [[ $fixture == failed_validation || ( $CASE == fallback_validation && $endpoint == *"/202/"* ) ]]; then validation='"failure"'; fi
      if [[ $CASE == cancelled_validation ]]; then validation='"cancelled"'; fi
      if [[ $CASE == skipped_validation ]]; then validation='"skipped"'; fi
      if [[ $CASE == latest_validation_failure ]]; then
        printf '{"jobs":[{"name":"required","conclusion":"success"},{"name":"release-validation","conclusion":"success"},{"name":"release-validation","conclusion":"failure"}]}\n'
        return
      fi
      printf '{"jobs":[{"name":"required","conclusion":%s},{"name":"release-validation","conclusion":%s}]}\n' "$conclusion" "$validation" ;;
    "repos/$GH_REPO/actions/runs/101/artifacts?per_page=100" | "repos/$GH_REPO/actions/runs/202/artifacts?per_page=100")
      local fixture=${CASE#pr_}
      if [[ $CASE == artifact_api_failure ]]; then return 23; fi
      if [[ $CASE == invalid_artifacts ]]; then printf '{"artifacts":null}\n'; return; fi
      if [[ $CASE == malformed_artifact ]]; then printf '{"artifacts":[{"name":"gang-x86_64-unknown-linux-musl","expired":"false"}]}\n'; return; fi
      local expired=false linux=',{"id":2,"name":"gang-aarch64-unknown-linux-musl","expired":false}' darwin=',{"id":3,"name":"gang-aarch64-apple-darwin","expired":false}'
      if [[ $fixture == missing_artifact || ( $CASE == fallback_artifact && $endpoint == *"/202/"* ) ]]; then linux=''; fi
      if [[ $CASE == no_darwin ]]; then darwin=''; fi
      if [[ $CASE == expired_darwin ]]; then darwin=',{"id":3,"name":"gang-aarch64-apple-darwin","expired":true}'; fi
      if [[ $fixture == expired_artifact ]]; then expired=true; fi
      printf '{"artifacts":[{"id":1,"name":"gang-x86_64-unknown-linux-musl","expired":%s}%s%s,{"id":4,"name":"gang-other","expired":false}]}\n' "$expired" "$linux" "$darwin" ;;
    *) printf 'unexpected gh endpoint: %s\n' "$endpoint" >&2; return 1 ;;
  esac
}
export -f gh
cases=0
for scenario in direct:101 release_dispatch:101 newer_tag_push:101 pr_merged:101 pr_overall_failure:101 pr_missing_required:fail pr_failed_required:fail pr_duplicate_required:fail pr_missing_validation:fail pr_failed_validation:fail pr_missing_artifact:fail pr_expired_artifact:fail changed_tree:fail fork_pr:fail unmerged_pr:fail wrong_merge:fail wrong_base:fail invalid_pr:fail pr_wrong_event:fail pr_wrong_repo:fail pr_wrong_sha:fail compare_failure:fail not_main:fail invalid_compare:fail tree_api_failure:fail invalid_tree:fail prs_api_failure:fail invalid_prs:fail same_repo_pr:fail ci_dispatch:fail wrong_sha:fail wrong_branch:fail wrong_repo:fail missing_repo:fail null_repo:fail api_failure:fail invalid_runs:fail malformed_json:fail failed_required:fail pending_required:fail missing_required:fail duplicate_required:fail missing_validation:fail failed_validation:fail cancelled_validation:fail skipped_validation:fail latest_validation_failure:fail jobs_api_failure:fail invalid_jobs:fail malformed_job:fail missing_artifact:fail expired_artifact:fail artifact_api_failure:fail invalid_artifacts:fail malformed_artifact:fail no_darwin:101 expired_darwin:101 latest:202 fallback_required:101 fallback_validation:101 fallback_artifact:101 fallback_api_failure:fail; do
  export CASE=${scenario%:*} EVENT=push
  expected=${scenario#*:}
  if [[ $CASE == release_dispatch ]]; then EVENT=workflow_dispatch; fi
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
for scenario in direct:1,2,3 no_darwin:1,2 expired_darwin:1,2 missing_artifact:fail expired_artifact:fail artifact_api_failure:fail invalid_artifacts:fail; do
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
