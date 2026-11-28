#!/usr/bin/env bash
# Exercise actual release selectors with a controlled GitHub boundary, without network calls.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
body="$scratch/locate.sh"
yq -er '.jobs.locate.steps[] | select(.name == "Find the CI run to release") | .run' "$script_dir/../.github/workflows/release.yml" >"$body"
validation="$scratch/validate.sh"
yq -er '.jobs.locate.steps[] | select(.name == "Validate release changelog") | .run' "$script_dir/../.github/workflows/release.yml" >"$validation"
selector="$scratch/binaries.sh"
yq -er '.jobs.publish.steps[] | select(.name == "Select available CI binaries") | .run' "$script_dir/../.github/workflows/release.yml" >"$selector"
[[ $(yq -er '.jobs.publish.steps[] | select(.id == "binaries") | .env.RUN_ID' "$script_dir/../.github/workflows/release.yml") == "\${{ needs.locate.outputs.run-id }}" ]]
[[ $(yq -er '.jobs.publish.steps[] | select(.with["artifact-ids"]) | .with["artifact-ids"]' "$script_dir/../.github/workflows/release.yml") == "\${{ steps.binaries.outputs.artifact-ids }}" ]]
[[ $(yq -er '.jobs.publish.steps[] | select(.with["artifact-ids"]) | .with["run-id"]' "$script_dir/../.github/workflows/release.yml") == "\${{ needs.locate.outputs.run-id }}" ]]
workflow="$script_dir/../.github/workflows/release.yml"
yq -o=json '.jobs' "$workflow" | jq -e '
  . as $jobs
  | ($jobs.publish.needs == ["locate", "images", "amis", "controller_image"])
    and ($jobs.controller_image.needs == "locate")
    and ($jobs.controller_image["runs-on"] == "ubuntu-24.04")
    and ([$jobs.controller_image.steps[] | select((.uses // "") | startswith("actions/download-artifact@")) | [.with.name, .with["run-id"]]] == [["gang-aarch64-unknown-linux-musl", "${{ needs.locate.outputs.run-id }}"]])
    and ($jobs.controller_image.permissions == {"contents":"read", "actions":"read"})
    and ([$jobs.controller_image.steps[] | select((.uses // "") | startswith("aws-actions/"))] == [])
    and ([$jobs.controller_image.steps[] | select(.name == "Build controller image") | .run | (contains("gang-controller.oci.tar") and contains("gang-controller.json") and contains("gang-binaries/aarch64-linux/gang") and contains("nix build -L --no-write-lock-file --override-input gang-binaries"))] == [true])
    and ([$jobs.controller_image.steps[] | select(.with.name == "gang-controller") | [.["if"], .with.name, (.with.path | contains("gang-controller.oci.tar"))]] == [["env.PUBLISH == '\''true'\''", "gang-controller", true]])
    and ([$jobs.publish.steps[] | select(.name == "Write SHA256SUMS") | .run | contains("sha256sum gang-*")] == [true])
    and ([$jobs.publish.steps[] | select(.with.name == "gang-controller") | .with.path] == ["dist"])
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
    and ($jobs.locate.permissions == {"actions":"read", "contents":"read"})
    and ([$jobs.locate.steps[] | select((.uses // "") | startswith("actions/checkout@")) | .with] == [{"ref":"${{ github.sha }}", "fetch-depth":0, "persist-credentials":false}])
    and ([$jobs.locate.steps[] | select(.id == "ci") | [.env.GH_TOKEN, .env.GH_REPO, .env.SHA]] == [["${{ github.token }}", "${{ github.repository }}", "${{ github.sha }}"]])
    and ([$jobs.locate.steps[] | select(.name == "Validate release changelog") | [."if", .env.RELEASE_COMMIT, .env.TAG, .env.CHANGELOG, .env.SECTION]] == [["${{ github.event_name == '\''push'\'' }}", "${{ github.sha }}", "${{ github.ref_name }}", "${{ runner.temp }}/validated-changelog.md", "${{ runner.temp }}/validated-section.md"]])
    and (([$jobs.locate.steps[] | .name // .uses] | index("Validate release changelog")) > ([$jobs.locate.steps[] | .uses // .name] | map(startswith("actions/checkout@")) | index(true)))
    and (([$jobs.locate.steps[] | .name] | index("Validate release changelog")) < ([$jobs.locate.steps[] | .name] | index("Find the CI run to release")))
    and ([$jobs.locate.steps[] | select(.id == "dry-run")] == [])
    and ($jobs.publish["if"] | contains("github.event_name == '\''push'\''"))
    and ([$jobs.images.steps[] | select(.name == "Match the dry run'\''s images" or .with["run-id"] == "${{ needs.locate.outputs.dry-run-id }}")] == [])
    and ([$jobs.images.steps[] | select(.name == "Build the images" or .name == "Compress the images") | .["if"]] == [null, null])
    and ([$jobs.controller_image.steps[] | select(.name == "Build controller image") | .["if"]] == [null])
    and ([$jobs.verify_reproducibility.steps[] | select(.name == "Verify reproducibility") | [.["if"], (.run | contains("nix build -L --rebuild"))]] == [[null, true]])
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
export GITHUB_OUTPUT="$scratch/output" CALLS="$scratch/calls"
python3() {
  case ${1:-} in
    tools/queue_ci.py)
      printf '%s\n' "$@" > "$CALLS"
      [[ $# == 5 && $2 == --sha && $3 == "$SHA" && $4 == --github-output && $5 == "$GITHUB_OUTPUT" ]] || return 1
      if [[ $CASE == failure ]]; then return 23; fi
      printf 'run-id=202\n' >> "$GITHUB_OUTPUT"
      ;;
    tools/changelog.py)
      [[ $# == 12 && $2 == --repo && $3 == . && $4 == render && $5 == --ref && $6 == "$RELEASE_COMMIT" && $7 == --tag && $8 == "$TAG" && $9 == --output && ${10} == "$CHANGELOG" && ${11} == --section-output && ${12} == "$SECTION" ]] || return 1
      printf 'validated\n' > "$CALLS"
      ;;
    *) return 1 ;;
  esac
}
export -f python3
cases=0
for CASE in success failure; do
  export CASE
  : >"$GITHUB_OUTPUT"
  rm -f "$CALLS"
  status=0
  bash --noprofile --norc -euo pipefail "$body" >"$scratch/case.log" 2>&1 || status=$?
  [[ -s $CALLS ]]
  if [[ $CASE == failure ]]; then
    (( status != 0 ))
    [[ ! -s $GITHUB_OUTPUT ]]
  else
    (( status == 0 ))
    [[ $(<"$GITHUB_OUTPUT") == run-id=202 ]]
  fi
  ((cases += 1))
done
export RELEASE_COMMIT="$SHA" TAG=v1.2.3 CHANGELOG="$scratch/changelog.md" SECTION="$scratch/section.md"
rm -f "$CALLS"
bash --noprofile --norc -euo pipefail "$validation"
[[ $(<"$CALLS") == validated ]]
((cases += 1))
# The real renderer must stop the job before its next step for an invalid tag.
export RELEASE_COMMIT
RELEASE_COMMIT=$(git -C "$script_dir/.." rev-parse HEAD)
export TAG=invalid
cat "$validation" > "$scratch/validation-job.sh"
printf '\nprintf reached > "%s"\n' "\$CALLS" >> "$scratch/validation-job.sh"
rm -f "$CALLS"
status=0
(unset -f python3; cd "$script_dir/.." && bash --noprofile --norc -euo pipefail "$scratch/validation-job.sh") >"$scratch/case.log" 2>&1 || status=$?
(( status != 0 ))
[[ ! -e $CALLS ]]
((cases += 1))
gh() {
  [[ $* == "api --paginate repos/$GH_REPO/actions/runs/$RUN_ID/artifacts?per_page=100" ]] || return 1
  case "$CASE" in
    artifact_api_failure) return 23 ;;
    invalid_artifacts) printf '{"artifacts":null}\n'; return ;;
  esac
  local x86='{"id":1,"name":"gang-x86_64-unknown-linux-musl","expired":false}'
  local arm=',{"id":2,"name":"gang-aarch64-unknown-linux-musl","expired":false}'
  local darwin=',{"id":3,"name":"gang-aarch64-apple-darwin","expired":false}'
  if [[ $CASE == missing_artifact ]]; then arm=''; fi
  if [[ $CASE == expired_artifact ]]; then x86='{"id":1,"name":"gang-x86_64-unknown-linux-musl","expired":true}'; fi
  if [[ $CASE == no_darwin ]]; then darwin=''; fi
  if [[ $CASE == expired_darwin ]]; then darwin=',{"id":3,"name":"gang-aarch64-apple-darwin","expired":true}'; fi
  printf '{"artifacts":[%s%s%s]}\n' "$x86" "$arm" "$darwin"
}
export -f gh
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
# Exercise the real bootstrap with isolated filesystem paths and fake privileged commands.
mkdir -p "$scratch/bin" "$scratch/etc/gang"
entrypoint=$(<"$script_dir/../infra/container/entrypoint.sh")
gang_dir=/etc/gang
entrypoint=${entrypoint//"$gang_dir"/"$scratch/etc/gang"}
printf '%s\n' "$entrypoint" >"$scratch/entrypoint.sh"
cat >"$scratch/bin/chown" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$scratch/bin/su-exec" <<'EOF'
#!/bin/sh
[ -z "${GANG_CONFIG+x}${GANG_SECRET_0+x}${GANG_SECRET_1+x}${GANG_SECRET_FILES+x}" ] || exit 1
printf '%s\n' "$@" > "$TRACE_EXEC"
printf '%s' "${GANG_CONTROLLER_IDENTITY-}" > "$TRACE_IDENTITY"
printf '%s' "${GANG_CONTROLLER_LAUNCHED_AT-}" > "$TRACE_LAUNCHED"
EOF
cat >"$scratch/bin/wget" <<'EOF'
#!/bin/sh
[ "$*" = '-q -T 10 -O - http://169.254.170.2/v4/task' ] || exit 1
printf '%s' "$METADATA_RESPONSE"
EOF
chmod +x "$scratch/bin/"*
export PATH="$scratch/bin:$PATH" TRACE_EXEC="$scratch/exec" TRACE_IDENTITY="$scratch/identity" TRACE_LAUNCHED="$scratch/launched"
unset ECS_CONTAINER_METADATA_URI_V4 GANG_CONTROLLER_IDENTITY GANG_CONTROLLER_LAUNCHED_AT
printf 'nameserver 10.0.0.2\nsearch internal\n' >"$scratch/etc/resolv.conf"
export GANG_CONFIG=$'config contents\n' GANG_SECRET_FILES=$'0=database-password\n1=token' GANG_SECRET_0=$'password\n\n' GANG_SECRET_1='token value'
sh "$scratch/entrypoint.sh"
printf 'config contents\n' >"$scratch/expected-config"
printf 'password\n\n' >"$scratch/expected-secret"
cmp "$scratch/expected-config" "$scratch/etc/gang/gang.toml"
cmp "$scratch/expected-secret" "$scratch/etc/gang/secrets/database-password"
[[ $(<"$scratch/etc/gang/secrets/token") == 'token value' ]]
[[ $(stat -c %a "$scratch/etc/gang/gang.toml") == 600 ]]
[[ $(stat -c %a "$scratch/etc/gang/secrets/token") == 600 ]]
[[ $(stat -c %a "$scratch/etc/gang") == 755 ]]
[[ $(stat -c %a "$scratch/etc/gang/secrets") == 700 ]]
[[ $(<"$TRACE_EXEC") == $'65532:65532\n/usr/local/bin/gang\nserver\n--config\n'"$scratch/etc/gang/gang.toml" ]]
[[ $(<"$scratch/etc/resolv.conf") == $'nameserver 10.0.0.2\nsearch internal' ]]
[[ ! -e "$scratch/etc/gang/upstream-resolv.conf" ]]
((cases += 1))
for bad in '../escape' '.' 'bad/name' 'bad name'; do
  export GANG_SECRET_FILES="0=$bad"
  if sh "$scratch/entrypoint.sh" >"$scratch/case.log" 2>&1; then
    printf 'unsafe secret filename accepted: %s\n' "$bad" >&2
    exit 1
  fi
  ((cases += 1))
done
export ECS_CONTAINER_METADATA_URI_V4=http://169.254.170.2/v4
export GANG_SECRET_FILES=$'0=database-password\n1=token'
export METADATA_RESPONSE='{"TaskARN":"arn:aws:ecs:example:123:task/cluster/stable","Containers":[{"Name":"controller","CreatedAt":"2026-11-22T12:34:56.123Z"}]}'
expected_identity=$(printf '%s' 'arn:aws:ecs:example:123:task/cluster/stable' | sha256sum)
expected_identity=${expected_identity%% *}
sh "$scratch/entrypoint.sh"
[[ $(<"$TRACE_IDENTITY") == "$expected_identity" ]]
[[ $(<"$TRACE_LAUNCHED") == '2026-11-22T12:34:56.123Z' ]]
sh "$scratch/entrypoint.sh"
[[ $(<"$TRACE_IDENTITY") == "$expected_identity" ]]
[[ $(<"$TRACE_LAUNCHED") == '2026-11-22T12:34:56.123Z' ]]
((cases += 1))
responses=(
  '{}'
  '{"TaskARN":"invalid"}'
  'not-json'
  '{"TaskARN":"arn:task","Containers":[]}'
  '{"TaskARN":"arn:task","Containers":[{"Name":"controller","CreatedAt":"invalid"}]}'
  '{"TaskARN":"arn:task","Containers":[{"Name":"controller"}]}'
  '{"TaskARN":"arn:task","Containers":[{"Name":"controller","CreatedAt":"2026-11-22T12:34:56Z"},{"Name":"controller","CreatedAt":"2026-11-22T12:34:56Z"}]}'
)
for response in "${responses[@]}"; do
  export METADATA_RESPONSE="$response"
  rm -f "$TRACE_EXEC"
  if sh "$scratch/entrypoint.sh" >"$scratch/case.log" 2>&1 || [[ -e $TRACE_EXEC ]]; then
    printf 'invalid task metadata reached controller\n' >&2
    exit 1
  fi
  ((cases += 1))
done
printf '%d cases, 0 failures\n' "$cases"
