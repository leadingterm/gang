#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

# The single owner of EC2 image registration parameters, shared by release and custom imports.

temp_dir=''

main() {
  if [[ $# -ne 7 ]]; then
    printf 'usage: import-image.sh <image.raw.zst> <image-info.json> <name> <arch> <sha256> <tag> <description>\n' >&2
    return 1
  fi

  local image="$1" info="$2" name="$3" arch="$4" sha="$5" tag="$6" description="$7"
  local aws_arch actual boot_mode snapshot tags
  case "$arch" in
    x86_64) aws_arch=x86_64 ;;
    aarch64) aws_arch=arm64 ;;
    *) printf 'import-image: unknown architecture: %s\n' "$arch" >&2; return 1 ;;
  esac

  actual=$(sha256sum < "$image")
  actual=${actual%% *}
  if [[ "$actual" != "$sha" ]]; then
    printf 'import-image: sha256 mismatch for %s\n' "$image" >&2
    return 1
  fi
  boot_mode=$(jq -er '.boot_mode | strings | select(length > 0)' "$info")
  temp_dir=$(mktemp -d)
  trap 'rm -rf -- "$temp_dir"' EXIT
  zstd -d --stdout -- "$image" > "$temp_dir/image.raw"
  snapshot=$(coldsnap upload --wait --no-progress --omit-zero-blocks --description "$name" "$temp_dir/image.raw")
  tags=$(jq -cn --arg sha "$sha" --arg tag "$tag" --arg arch "$arch" \
    '[{ResourceType:"image",Tags:[{Key:"ec2:SharedTag/ImageSha256",Value:$sha},{Key:"ec2:SharedTag/GangRelease",Value:$tag},{Key:"ec2:SharedTag/GangImage",Value:("gang-image-" + $arch)}]}]')
  aws ec2 register-image --architecture "$aws_arch" --boot-mode "$boot_mode" \
    --ena-support --sriov-net-support simple --virtualization-type hvm --imds-support v2.0 \
    --root-device-name /dev/xvda \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={SnapshotId=$snapshot,VolumeType=gp3,DeleteOnTermination=true}" \
    --name "$name" --description "$description" --tag-specifications "$tags" \
    --query ImageId --output text
}

main "$@"
