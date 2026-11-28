#!/usr/bin/env bash
# Exercise the real import step with a real OCI archive and fake network commands.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export RUNNER_TEMP="$scratch/runner" FIXTURE="$scratch/release" CALLS="$scratch/calls" CASE
export GANG_REPOSITORY=example-org/gang GANG_WORKFLOW_REF=example-org/gang/.github/workflows/deploy.yml@refs/tags/v1.2.3
mkdir -p "$RUNNER_TEMP" "$FIXTURE" "$scratch/layout/blobs/sha256"
repo=000000000000.dkr.ecr.eu-north-1.amazonaws.com/controller
printf '{"schemaVersion":2,"layers":[]}' >"$scratch/blob"
hex=$(sha256sum "$scratch/blob")
hex=${hex%% *}
export DIGEST="sha256:$hex" REPO="$repo"
cp "$scratch/blob" "$scratch/layout/blobs/sha256/$hex"
jq -n --arg digest "$DIGEST" '{schemaVersion:2,manifests:[{mediaType:"application/vnd.oci.image.manifest.v1+json",digest:$digest}]}' >"$scratch/layout/index.json"
tar -cf "$FIXTURE/gang-controller.oci.tar" -C "$scratch/layout" index.json blobs
jq -n --arg digest "$DIGEST" '{controller_image_asset:"gang-controller.oci.tar",controller_image_digest:$digest}' >"$FIXTURE/gang-controller.json"
(cd "$FIXTURE" && sha256sum gang-controller.oci.tar gang-controller.json >SHA256SUMS)
jq -n --arg repo "$repo" --arg digest "$DIGEST" '
  {planned_values:{root_module:{child_modules:[{resources:[
    {type:"aws_ecr_repository",name:"controller",values:{repository_url:$repo}},
    {type:"aws_ecs_task_definition",name:"controller",values:{container_definitions:([{name:"controller",image:($repo+"@"+$digest)}] | tojson)}}
  ]}]}}}' >"$scratch/plan.json"
gh() {
  if [[ $1 == attestation ]]; then
    [[ $* == *"--source-ref refs/tags/v0.4.1" ]]
    return
  fi
  [[ $* == "release download ${GANG_RELEASE_TAG:-v1.2.3} --repo example-org/gang --pattern gang-controller.oci.tar --pattern gang-controller.json --pattern SHA256SUMS --dir "* ]] || return 1
  printf 'gh\n' >>"$CALLS"
  local dir=${!#}
  cp "$FIXTURE/gang-controller.oci.tar" "$FIXTURE/SHA256SUMS" "$dir/"
  if [[ $CASE != missing_manifest ]]; then cp "$FIXTURE/gang-controller.json" "$dir/"; fi
  if [[ $CASE == tamper ]]; then printf x >>"$dir/gang-controller.oci.tar"; fi
  if [[ $CASE == mismatch ]]; then
    jq '.controller_image_digest = "sha256:bad"' "$dir/gang-controller.json" >"$dir/new"
    mv "$dir/new" "$dir/gang-controller.json"
    (cd "$dir" && sha256sum gang-controller.oci.tar gang-controller.json >SHA256SUMS)
  fi
  if [[ $CASE == tamper_manifest ]]; then printf x >>"$dir/gang-controller.json"; fi
}
aws() {
  printf 'aws %s\n' "$*" >>"$CALLS"
  case "$*" in
    '--region eu-north-1 ecr describe-repositories --repository-names controller --output json')
      [[ $CASE != missing_repo ]] || return 1
      jq -n --arg repo "$REPO" '{repositories:[{repositoryUri:$repo}]}' ;;
    '--region eu-north-1 ecr get-login-password') printf 'password\n' ;;
    '--region eu-north-1 ecr describe-images --repository-name controller --image-ids imageDigest='*' --output json')
      [[ $* == *"imageDigest=$DIGEST "* ]] || return 1
      if [[ $CASE == aws_error ]]; then
        echo 'An error occurred (AccessDeniedException) when calling the DescribeImages operation: denied' >&2
        return 254
      fi
      if [[ $CASE != existing && ! -e $RUNNER_TEMP/pushed ]]; then
        echo 'An error occurred (ImageNotFoundException) when calling the DescribeImages operation: missing' >&2
        return 254
      fi
      jq -n --arg digest "$DIGEST" '{imageDetails:[{imageDigest:$digest}]}' ;;
    *) return 1 ;;
  esac
}
skopeo() {
  printf 'skopeo %s\n' "$*" >>"$CALLS"
  case "$*" in
    'login --authfile '*'/auth.json --username AWS --password-stdin 000000000000.dkr.ecr.eu-north-1.amazonaws.com')
      local password; IFS= read -r password; [[ $password == password ]] ;;
    '--policy '*'/local-source-policy.json copy --authfile '*'/auth.json --preserve-digests oci-archive:'*" docker://$REPO:${DIGEST#sha256:}")
      jq -e '. == {"default":[{"type":"reject"}],"transports":{"oci-archive":{"":[{"type":"insecureAcceptAnything"}]}}}' "$2" >/dev/null || return 1
      [[ $CASE != failed_push ]] || return 1
      : >"$RUNNER_TEMP/pushed" ;;
    *) return 1 ;;
  esac
}
export -f gh aws skopeo
run_case() {
  CASE=$1
  export CASE
  : >"$CALLS"
  rm -f "$RUNNER_TEMP/pushed"
  cp "$scratch/plan.json" "$RUNNER_TEMP/infra-plan.json"
  if [[ $CASE == sqlite ]]; then printf '%s\n' '{"planned_values":{"root_module":{"resources":[]}}}' >"$RUNNER_TEMP/infra-plan.json"; fi
  if [[ $CASE == bootstrap ]]; then jq 'del(.planned_values.root_module.child_modules[0].resources[1])' "$scratch/plan.json" >"$RUNNER_TEMP/infra-plan.json"; fi
  if [[ $CASE == initial_plan ]]; then jq '(.planned_values.root_module.child_modules[0].resources[0].values.repository_url, .planned_values.root_module.child_modules[0].resources[1].values.container_definitions) = null' "$scratch/plan.json" >"$RUNNER_TEMP/infra-plan.json"; fi
  if bash "$root/infra/scripts/publish-controller-image.sh" >"$scratch/output" 2>&1; then
    [[ $CASE == success || $CASE == override || $CASE == existing || $CASE == sqlite || $CASE == bootstrap ]] || { echo "$CASE unexpectedly succeeded" >&2; exit 1; }
  else
    [[ $CASE != success && $CASE != override && $CASE != existing && $CASE != sqlite && $CASE != bootstrap ]] || { cat "$scratch/output" >&2; exit 1; }
  fi
  case "$CASE" in
    success | override)
      grep -q '^skopeo --policy .* copy --authfile .* --preserve-digests ' "$CALLS"
      [[ $(grep -c 'ecr describe-images' "$CALLS") == 2 ]] ;;
    existing)
      grep -q 'ecr describe-images' "$CALLS"
      ! grep -q '^skopeo ' "$CALLS" ;;
    initial_plan)
      grep -Fq 'controller repository URL is unknown; first review and apply a targeted bootstrap of aws_ecr_repository.controller' "$scratch/output"
      [[ ! -s $CALLS ]] ;;
    sqlite | bootstrap) [[ ! -s $CALLS ]] ;;
    failed_push) grep -q '^skopeo --policy .* copy ' "$CALLS"; [[ $(grep -c 'describe-images' "$CALLS") == 1 ]] ;;
    *) ! grep -q '^skopeo --policy .* copy ' "$CALLS" ;;
  esac
}
for case_name in success existing aws_error tamper tamper_manifest mismatch missing_repo failed_push missing_manifest sqlite bootstrap initial_plan; do run_case "$case_name"; done

GANG_RELEASE_TAG=v0.4.1 run_case override
GANG_RELEASE_TAG=bad run_case invalid
