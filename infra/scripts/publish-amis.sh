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
read -r -a regions <<< "$IMAGE_REGIONS"
release_args=()
if [[ -n "${RELEASE:-}" ]]; then
  : "${IMAGES_DIR:?IMAGES_DIR is required with RELEASE}"
  : "${IMAGE_SHA256_x86_64:?IMAGE_SHA256_x86_64 is required with RELEASE}"
  : "${IMAGE_SHA256_aarch64:?IMAGE_SHA256_aarch64 is required with RELEASE}"
  release_args=(--release "$RELEASE" --image "x86_64=$IMAGE_SHA256_x86_64" --image "aarch64=$IMAGE_SHA256_aarch64")
fi

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
  local region="$1" index="$2"
  aws ec2 describe-images --region "$region" --owners self --include-deprecated \
    --output json > "$temp_dir/images-$index.json" &&
    aws service-quotas get-service-quota --region "$region" --service-code ec2 \
      --quota-code L-0E3CBAB9 --query Quota.Value --output json > "$temp_dir/quota-$index.json"
}

collect_regions() {
  local region index=0 separator=''
  local -a pids=()
  for region in ${regions[@]+"${regions[@]}"}; do
    if [[ ! "$region" =~ ^[a-z0-9-]+$ ]]; then
      printf 'publish-amis: invalid region: %s\n' "$region" >&2
      return 1
    fi
    collect_region "$region" "$index" &
    pids+=("$!")
    index=$((index + 1))
  done
  wait_all ${pids[@]+"${pids[@]}"}
  {
    printf '{'
    index=0
    for region in ${regions[@]+"${regions[@]}"}; do
      printf '%s"%s":{"quota":%s,"images":%s}' "$separator" "$region" \
        "$(<"$temp_dir/quota-$index.json")" "$(<"$temp_dir/images-$index.json")"
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

execute_steps() {
  local line kind a b c d e
  local -a background=()
  while IFS= read -r line; do
    IFS=$'\t' read -r kind a b c d e <<< "$line"
    log_step "$line"
    case "$kind" in
      upload)
        AWS_REGION="$IMAGE_HOME_REGION" AWS_DEFAULT_REGION="$IMAGE_HOME_REGION" \
          "$script_dir/import-image.sh" "$IMAGES_DIR/gang-image-$a.raw.zst" \
          "$IMAGES_DIR/gang-image-$a.json" "$b" "$a" "$c" "$d" "$e"
        ;;
      copy)
        aws ec2 copy-image --region "$a" --source-region "$IMAGE_HOME_REGION" \
          --source-image-id "$b" --name "$c" --description "$d" --copy-image-tags &
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
      deregister) aws ec2 deregister-image --region "$a" --image-id "$b" --delete-associated-snapshots ;;
      *) printf 'publish-amis: unknown step: %s\n' "$kind" >&2; return 1 ;;
    esac
  done <<< "$1"
  wait_all ${background[@]+"${background[@]}"}
}

main() {
  local round now steps
  for ((round = 1; round <= 40; round++)); do
    collect_regions
    now=$(date +%s)
    steps=$("$GANG" release ami-steps --home-region "$IMAGE_HOME_REGION" --now "$now" \
      ${release_args[@]+"${release_args[@]}"} < "$temp_dir/regions.json")
    if [[ -z "$steps" ]]; then
      if [[ -n "${RELEASE:-}" ]]; then
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
