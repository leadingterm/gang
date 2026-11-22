#!/usr/bin/env bash
# Import the release image named by the verified saved plan before applying it.
# The apply role needs ecr:GetAuthorizationToken on *, and ecr:BatchCheckLayerAvailability,
# ecr:InitiateLayerUpload, ecr:UploadLayerPart, ecr:CompleteLayerUpload, ecr:PutImage,
# ecr:DescribeRepositories and ecr:DescribeImages on the deployment's controller repository ARN
# (exported by the deployment module). Bootstrap a missing repository with a reviewed targeted
# apply first; this script never creates infrastructure.
set -euo pipefail

: "${RUNNER_TEMP:?}" "${GANG_WORKFLOW_REF:?}" "${GANG_REPOSITORY:?}"
plan="$RUNNER_TEMP/infra-plan.json"
[[ -f "$plan" ]] || { echo 'verified plan JSON is missing' >&2; exit 1; }

# Traverse planned values rather than changes: an unchanged task still needs its image.
selection=$(jq -ce '
  def resources: .resources[]?, (.child_modules[]? | resources);
  [.planned_values.root_module | resources] as $r |
  [$r[] | select(.type == "aws_ecs_task_definition" and .name == "controller")] as $tasks |
  [$r[] | select(.type == "aws_ecr_repository" and .name == "controller")] as $repos |
  if ($tasks | length) == 0 then {skip:true}
  elif ($tasks | length) != 1 or ($repos | length) != 1 then error("expected one controller task and repository")
  else
    ($repos[0].values.repository_url) as $repo |
    ($tasks[0].values.container_definitions) as $definitions |
    if ($repo | type) != "string" then error("controller repository URL is unknown; first review and apply a targeted bootstrap of aws_ecr_repository.controller")
    elif ($definitions | type) != "string" then error("controller task container definitions are unknown; create a new reviewed plan after bootstrapping aws_ecr_repository.controller")
    else
      ($definitions | fromjson | map(select(.name == "controller"))) as $containers |
      if ($containers | length) != 1 then error("unknown controller repository or image")
      else {repository:$repo, image:$containers[0].image} end
    end
  end
' "$plan")
if [[ $(jq -r '.skip // false' <<<"$selection") == true ]]; then
  echo 'No ECS controller in the reviewed plan; skipping image import.'
  exit 0
fi
repo=$(jq -er '.repository' <<<"$selection")
image=$(jq -er '.image' <<<"$selection")
[[ $repo =~ ^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?/([a-z0-9]+([._/-][a-z0-9]+)*)$ ]] || { echo 'invalid private ECR repository URL' >&2; exit 1; }
registry=${repo%%/*}
region=${BASH_REMATCH[2]}
name=${repo#*/}
[[ $image == "$repo"@sha256:* ]] || { echo 'controller image is not in its reviewed repository' >&2; exit 1; }
digest=${image#"$repo"@}
[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { echo 'invalid controller image digest' >&2; exit 1; }
ref=${GANG_WORKFLOW_REF#*@}
[[ $ref == refs/tags/* && $ref != refs/tags/ ]] || { echo 'workflow must be pinned to a release tag' >&2; exit 1; }
tag=${ref#refs/tags/}
work=$(mktemp -d "${RUNNER_TEMP}/gang-image.XXXXXXXX")
trap 'rm -rf "$work"' EXIT
gh release download "$tag" --repo "$GANG_REPOSITORY" --pattern gang-controller.oci.tar --pattern gang-controller.json --pattern SHA256SUMS --dir "$work"
for asset in gang-controller.oci.tar gang-controller.json; do
  checksum=$(jq -Rrn --arg asset "$asset" '[inputs | capture("^(?<hash>[0-9a-f]{64})  [*]?(?<name>.+)$")? | select(.name == $asset) | .hash] | if length == 1 then .[0] else error("missing or duplicate asset checksum") end' <"$work/SHA256SUMS")
  sha256sum --check --strict <<<"$checksum  $work/$asset"
done
jq -e --arg digest "$digest" '.controller_image_asset == "gang-controller.oci.tar" and .controller_image_digest == $digest' "$work/gang-controller.json" >/dev/null || { echo 'release manifest does not match reviewed image' >&2; exit 1; }
tar -xOf "$work/gang-controller.oci.tar" index.json >"$work/index.json"
index_digest=$(jq -er '.manifests | if length == 1 and .[0].mediaType == "application/vnd.oci.image.manifest.v1+json" then .[0].digest else error("expected one OCI image manifest") end' "$work/index.json")
[[ $index_digest == "$digest" ]] || { echo 'OCI index digest does not match reviewed image' >&2; exit 1; }
tar -xOf "$work/gang-controller.oci.tar" "blobs/sha256/${digest#sha256:}" >"$work/manifest-blob"
sha256sum --check --strict <<<"${digest#sha256:}  $work/manifest-blob"
aws --region "$region" ecr describe-repositories --repository-names "$name" --output json | jq -e --arg repo "$repo" '.repositories | length == 1 and .[0].repositoryUri == $repo' >/dev/null
if aws --region "$region" ecr describe-images --repository-name "$name" --image-ids "imageDigest=$digest" --output json >"$work/existing.json" 2>"$work/describe-error"; then
  jq -e --arg digest "$digest" '.imageDetails | length == 1 and .[0].imageDigest == $digest' "$work/existing.json" >/dev/null
  echo 'Reviewed image already exists in the private repository.'
  exit 0
else
  if ! grep -Fq 'An error occurred (ImageNotFoundException) when calling the DescribeImages operation' "$work/describe-error"; then
    cat "$work/describe-error" >&2
    exit 1
  fi
fi
# The only trusted source is the local archive already checked against the reviewed digest.
printf '%s\n' '{"default":[{"type":"reject"}],"transports":{"oci-archive":{"":[{"type":"insecureAcceptAnything"}]}}}' >"$work/local-source-policy.json"
aws --region "$region" ecr get-login-password | skopeo login --authfile "$work/auth.json" --username AWS --password-stdin "$registry"
skopeo --policy "$work/local-source-policy.json" copy --authfile "$work/auth.json" --preserve-digests "oci-archive:$work/gang-controller.oci.tar" "docker://$repo:${digest#sha256:}"
aws --region "$region" ecr describe-images --repository-name "$name" --image-ids "imageDigest=$digest" --output json | jq -e --arg digest "$digest" '.imageDetails | length == 1 and .[0].imageDigest == $digest' >/dev/null
