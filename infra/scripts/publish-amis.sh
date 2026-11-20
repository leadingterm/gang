#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

# Glue for public AMI publication: gang makes the decisions; this script observes and executes.

: "${GANG:?GANG is required}"
: "${IMAGE_HOME_REGION:?IMAGE_HOME_REGION is required}"
: "${IMAGE_REGIONS:?IMAGE_REGIONS is required}"

script_dir=$(dirname -- "${BASH_SOURCE[0]}")
temp_dir=$(mktemp -d)
trap 'rm -rf -- "$temp_dir"' EXIT
publication_scope=${PUBLICATION_SCOPE:-catalog}
case "$publication_scope" in
  release|catalog) ;;
  *) printf 'publish-amis: invalid publication scope: %s\n' "$publication_scope" >&2; exit 1 ;;
esac
if [[ ! "$IMAGE_HOME_REGION" =~ ^[a-z0-9-]+$ ]]; then
  printf 'publish-amis: invalid home region: %s\n' "$IMAGE_HOME_REGION" >&2
  exit 1
fi
read -r -a configured_input <<< "${IMAGE_REGIONS//$'\n'/ }"
configured_regions=()
for region in ${configured_input[@]+"${configured_input[@]}"}; do
  if [[ ! "$region" =~ ^[a-z0-9-]+$ ]]; then
    printf 'publish-amis: invalid region: %s\n' "$region" >&2
    exit 1
  fi
  found=0
  for existing in ${configured_regions[@]+"${configured_regions[@]}"}; do
    [[ "$existing" != "$region" ]] || found=1
  done
  if [[ $found == 0 ]]; then configured_regions+=("$region"); fi
done
regions=()
if [[ "$publication_scope" == catalog ]]; then
  found=0
  for region in ${configured_regions[@]+"${configured_regions[@]}"}; do
    [[ "$region" != "$IMAGE_HOME_REGION" ]] || found=1
    regions+=("$region")
  done
  if [[ $found == 0 ]]; then
    printf 'publish-amis: catalog regions must include home region\n' >&2
    exit 1
  fi
else
  regions=("$IMAGE_HOME_REGION")
  deploy_input=${AMI_DEPLOY_REGIONS:-}
  read -r -a deploy_regions <<< "${deploy_input//$'\n'/ }"
  for region in ${deploy_regions[@]+"${deploy_regions[@]}"}; do
    if [[ ! "$region" =~ ^[a-z0-9-]+$ ]]; then
      printf 'publish-amis: invalid deploy region: %s\n' "$region" >&2
      exit 1
    fi
    allowed=0
    [[ "$region" != "$IMAGE_HOME_REGION" ]] || allowed=1
    for existing in ${configured_regions[@]+"${configured_regions[@]}"}; do
      [[ "$existing" != "$region" ]] || allowed=1
    done
    if [[ $allowed == 0 ]]; then
      printf 'publish-amis: deploy region is not configured: %s\n' "$region" >&2
      exit 1
    fi
    found=0
    for existing in "${regions[@]}"; do
      [[ "$existing" != "$region" ]] || found=1
    done
    if [[ $found == 0 ]]; then regions+=("$region"); fi
  done
fi
release_args=()
if [[ "$publication_scope" == release ]]; then
  : "${RELEASE:?RELEASE is required in release scope}"
fi
if [[ -n "${RELEASE:-}" ]]; then
  : "${IMAGES_DIR:?IMAGES_DIR is required with RELEASE}"
  : "${IMAGE_SHA256_x86_64:?IMAGE_SHA256_x86_64 is required with RELEASE}"
  : "${IMAGE_SHA256_aarch64:?IMAGE_SHA256_aarch64 is required with RELEASE}"
  release_args=(--release "$RELEASE" --image "x86_64=$IMAGE_SHA256_x86_64" --image "aarch64=$IMAGE_SHA256_aarch64")
fi
mkdir -p "$temp_dir/known" "$temp_dir/imports"
for region in ${regions[@]+"${regions[@]}"}; do mkdir -p "$temp_dir/known/$region"; done

wait_all() {
  local pid failed=0
  for pid in "$@"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  return "$failed"
}

collect_region() {
  local region="$1" index="$2" handle
  local -a known_ids=()
  aws ec2 describe-images --region "$region" --owners self --include-deprecated --include-disabled \
    --output json > "$temp_dir/images-$index.json" || return
  for handle in "$temp_dir/known/$region/"ami-*; do
    [[ -f "$handle" ]] || continue
    known_ids+=("${handle##*/}")
  done
  if [[ -n "${known_ids[0]:-}" ]]; then
    aws ec2 describe-images --region "$region" --image-ids "${known_ids[@]}" \
      --include-deprecated --include-disabled --output json > "$temp_dir/known-images-$index.json" || return
    jq -s '.[0].Images as $fresh | .[1] | .Images = ([.Images[] | select(.ImageId as $id | $fresh | all(.ImageId != $id))] + $fresh)' \
      "$temp_dir/known-images-$index.json" "$temp_dir/images-$index.json" > "$temp_dir/merged-images-$index.json" || return
    mv -- "$temp_dir/merged-images-$index.json" "$temp_dir/images-$index.json" || return
  fi
  aws ec2 describe-snapshots --region "$region" --owner-ids self \
    --filters Name=tag-key,Values=ec2:SharedTag/GangRetiredImage \
    --output json > "$temp_dir/snapshots-$index.json" &&
    aws service-quotas get-service-quota --region "$region" --service-code ec2 \
      --quota-code L-0E3CBAB9 --query Quota.Value --output json > "$temp_dir/quota-$index.json"
}

collect_regions() {
  local region index=0 separator=''
  local -a pids=()
  for region in ${regions[@]+"${regions[@]}"}; do
    # All regions are validated before any AWS calls.
    collect_region "$region" "$index" &
    pids+=("$!")
    index=$((index + 1))
  done
  wait_all ${pids[@]+"${pids[@]}"}
  {
    printf '{'
    index=0
    for region in ${regions[@]+"${regions[@]}"}; do
      printf '%s"%s":{"quota":%s,"images":%s,"snapshots":%s}' "$separator" "$region" \
        "$(<"$temp_dir/quota-$index.json")" "$(<"$temp_dir/images-$index.json")" \
        "$(<"$temp_dir/snapshots-$index.json")"
      separator=','
      index=$((index + 1))
    done
    printf '}\n'
  } > "$temp_dir/regions.json"
}

log_step() {
  printf '%s\n' "$1"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s\n' "$1" >> "$GITHUB_STEP_SUMMARY"
  fi
}

wait_image() {
  local region="$1" image_id="$2" error
  if error=$(aws ec2 wait image-available --region "$region" --image-ids "$image_id" 2>&1); then
    return 0
  fi
  printf '%s\n' "$error" >&2
  if [[ "$error" == *'Max attempts exceeded'* ]]; then
    return 0
  fi
  return 1
}

record_image() {
  local region="$1" image_id="$2"
  if [[ ! "$region" =~ ^[a-z0-9-]+$ || ! -d "$temp_dir/known/$region" || ! "$image_id" =~ ^ami-[a-f0-9]+$ ]]; then
    printf 'publish-amis: invalid image handle: %s %s\n' "$region" "$image_id" >&2
    return 1
  fi
  : > "$temp_dir/known/$region/$image_id"
}

copy_image() {
  local region="$1" source="$2" name="$3" description="$4" image_id
  if [[ ! "$region" =~ ^[a-z0-9-]+$ || ! -d "$temp_dir/known/$region" ]]; then
    printf 'publish-amis: invalid copy region: %s\n' "$region" >&2
    return 1
  fi
  image_id=$(aws ec2 copy-image --region "$region" --source-region "$IMAGE_HOME_REGION" \
    --source-image-id "$source" --name "$name" --description "$description" --copy-image-tags \
    --query ImageId --output text) || return
  record_image "$region" "$image_id" || return
  log_step "copied $region $image_id"
  wait_image "$region" "$image_id"
}

execute_steps() {
  local line kind a b c d e image_id handle count
  local -a background=()
  while IFS= read -r line; do
    IFS=$'\t' read -r kind a b c d e <<< "$line"
    log_step "$line"
    case "$kind" in
      upload)
        image_id=$(AWS_REGION="$IMAGE_HOME_REGION" AWS_DEFAULT_REGION="$IMAGE_HOME_REGION" \
          "$script_dir/import-image.sh" "$IMAGES_DIR/gang-image-$a.raw.zst" \
          "$IMAGES_DIR/gang-image-$a.json" "$b" "$a" "$c" "$d" "$e") || return
        if [[ ! "$image_id" =~ ^ami-[a-f0-9]+$ ]]; then
          printf 'publish-amis: invalid imported image ID: %s\n' "$image_id" >&2
          return 1
        fi
        count=0
        for handle in "$temp_dir/imports/"ami-*; do
          [[ -f "$handle" ]] || continue
          count=$((count + 1))
          if [[ "${handle##*/}" == "$image_id" ]]; then
            printf 'publish-amis: duplicate imported image ID: %s\n' "$image_id" >&2
            return 1
          fi
        done
        if (( count >= 2 )); then
          printf 'publish-amis: too many imported images\n' >&2
          return 1
        fi
        record_image "$IMAGE_HOME_REGION" "$image_id" || return
        : > "$temp_dir/imports/$image_id"
        log_step "imported $image_id"
        wait_image "$IMAGE_HOME_REGION" "$image_id" || return
        ;;
      copy)
        copy_image "$a" "$b" "$c" "$d" &
        background+=("$!")
        ;;
      wait)
        wait_image "$a" "$b" &
        background+=("$!")
        ;;
      protect) aws ec2 enable-image-deregistration-protection --region "$a" --image-id "$b" --with-cooldown ;;
      publish) aws ec2 modify-image-attribute --region "$a" --image-id "$b" --launch-permission 'Add=[{Group=all}]' ;;
      deprecate) aws ec2 enable-image-deprecation --region "$a" --image-id "$b" --deprecate-at "$c" ;;
      unprotect) aws ec2 disable-image-deregistration-protection --region "$a" --image-id "$b" ;;
      retire-snapshot) aws ec2 create-tags --region "$a" --resources "$b" --tags "Key=ec2:SharedTag/GangRetiredImage,Value=$c" ;;
      deregister) aws ec2 deregister-image --region "$a" --image-id "$b" ;;
      delete-snapshot) aws ec2 delete-snapshot --region "$a" --snapshot-id "$b" ;;
      *) printf 'publish-amis: unknown step: %s\n' "$kind" >&2; return 1 ;;
    esac
  done <<< "$1"
  wait_all ${background[@]+"${background[@]}"}
}

main() {
  local round now steps operation=ami-steps
  if [[ "$publication_scope" == release ]]; then operation=ami-release-steps; fi
  for ((round = 1; round <= 40; round++)); do
    collect_regions
    now=$(date +%s)
    steps=$("$GANG" release "$operation" --home-region "$IMAGE_HOME_REGION" --now "$now" \
      ${release_args[@]+"${release_args[@]}"} < "$temp_dir/regions.json")
    if [[ -z "$steps" ]]; then
      if [[ "$publication_scope" == release ]]; then
        "$GANG" release ami-manifest --home-region "$IMAGE_HOME_REGION" --now "$now" \
          ${release_args[@]+"${release_args[@]}"} < "$temp_dir/regions.json" > gang-amis.json
      fi
      return 0
    fi
    execute_steps "$steps"
  done
  printf 'publish-amis: did not converge\n' >&2
  return 1
}

main "$@"
