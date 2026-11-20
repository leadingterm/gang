#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

# The single owner of EC2 image registration parameters, shared by release and custom imports.

temp_dir=''

registration_rejected() {
  local status="$1" stderr="$2" code
  case "$status" in
    252|253) return 0 ;;
    254) ;;
    *) return 1 ;;
  esac
  if [[ $stderr =~ (^|$'\n')An\ error\ occurred\ \(([^()]*)\)\ when\ calling\ the\ RegisterImage\ operation(\ \(reached\ max\ retries:\ 0\))?: ]]; then
    code=${BASH_REMATCH[2]}
    case "$code" in
      AuthFailure|Blocked|IncompleteSignature|InvalidAction|InvalidCharacter|InvalidClientTokenId|InvalidParameter|InvalidParameterCombination|InvalidParameterDependency|InvalidParameterValue|InvalidQueryParameter|MalformedQueryString|MissingAction|MissingAuthenticationToken|MissingParameter|OptInRequired|PendingVerification|RequestExpired|TagPolicyViolation|UnauthorizedOperation|UnknownParameter|UnsupportedOperation|UnsupportedProtocol|ValidationError|InvalidAMIName.Duplicate|InvalidAMIName.Malformed|InvalidBlockDeviceMapping|InvalidSnapshot.NotFound|InvalidSnapshotID.Malformed|InvalidSnapshotID.NotFound|InvalidKernelId.Malformed)
        return 0 ;;
    esac
  fi
  return 1
}

main() {
  if [[ $# -ne 7 ]]; then
    printf 'usage: import-image.sh <image.raw.zst> <image-info.json> <name> <arch> <sha256> <tag> <description>\n' >&2
    return 1
  fi

  local image="$1" info="$2" name="$3" arch="$4" sha="$5" tag="$6" description="$7"
  local aws_arch actual boot_mode action_environment snapshot tags status images image_id
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
  if ! action_environment=$(jq -er '.action_environment | strings | select(test("^[0-9a-f]{64}\\z"))' "$info"); then
    printf 'import-image: image-info action_environment must be 64 lowercase hexadecimal digits\n' >&2
    return 1
  fi
  images=$(aws ec2 describe-images --owners self --include-deprecated --include-disabled \
    --filters "Name=name,Values=$name" --output json)
  if ! image_id=$(jq -er --arg name "$name" --arg sha "$sha" --arg tag "$tag" \
    --arg arch "$arch" --arg aws_arch "$aws_arch" --arg boot_mode "$boot_mode" --arg action_environment "$action_environment" '
      if (.Images | type) != "array" then error("invalid image inventory")
      elif (.Images | length) == 0 then ""
      elif (.Images | length) != 1 then error("multiple images with the requested name")
      else .Images[0] |
        def tag_value($key): [.Tags[]? | select(.Key == $key) | .Value];
        if .Name == $name and .Architecture == $aws_arch and .BootMode == $boot_mode
          and (.State == "pending" or .State == "available")
          and (tag_value("ec2:SharedTag/ImageSha256") == [$sha])
          and (tag_value("ec2:SharedTag/ActionEnvironment") == [$action_environment])
          and (tag_value("ec2:SharedTag/GangRelease") == [$tag])
          and (tag_value("ec2:SharedTag/GangImage") == ["gang-image-" + $arch])
          and (.ImageId | type == "string" and length > 0)
        then .ImageId
        else error("existing image metadata does not match")
        end
      end' <<<"$images"); then
    printf 'import-image: cannot safely adopt existing image %s\n' "$name" >&2
    return 1
  fi
  if [[ -n $image_id ]]; then
    printf '%s\n' "$image_id"
    return 0
  fi
  temp_dir=$(mktemp -d)
  trap 'rm -rf -- "$temp_dir"' EXIT
  zstd -d --stdout -- "$image" > "$temp_dir/image.raw"
  snapshot=$(coldsnap upload --wait --no-progress --omit-zero-blocks --description "$name" \
    --tag "Key=ec2:SharedTag/GangImportImage,Value=$name" "$temp_dir/image.raw")
  printf 'import-image: snapshot %s owned by import %s\n' "$snapshot" "$name" >&2
  tags=$(jq -cn --arg sha "$sha" --arg tag "$tag" --arg arch "$arch" --arg action_environment "$action_environment" \
    '[{ResourceType:"image",Tags:[{Key:"ec2:SharedTag/ImageSha256",Value:$sha},{Key:"ec2:SharedTag/ActionEnvironment",Value:$action_environment},{Key:"ec2:SharedTag/GangRelease",Value:$tag},{Key:"ec2:SharedTag/GangImage",Value:("gang-image-" + $arch)}]}]')
  # A single attempt makes a client rejection definitive; ambiguous outcomes retain the snapshot.
  if AWS_MAX_ATTEMPTS=1 aws ec2 register-image --architecture "$aws_arch" --boot-mode "$boot_mode" \
    --ena-support --sriov-net-support simple --virtualization-type hvm --imds-support v2.0 \
    --root-device-name /dev/xvda \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={SnapshotId=$snapshot,VolumeType=gp3,DeleteOnTermination=true}" \
    --name "$name" --description "$description" --tag-specifications "$tags" \
    --query ImageId --output text 2>"$temp_dir/register-image.stderr"; then
    return 0
  else
    status=$?
  fi
  cat "$temp_dir/register-image.stderr" >&2
  if registration_rejected "$status" "$(<"$temp_dir/register-image.stderr")"; then
    if aws ec2 delete-snapshot --snapshot-id "$snapshot" >/dev/null; then
      printf 'import-image: deleted snapshot %s after registration rejection for %s\n' "$snapshot" "$name" >&2
    else
      printf 'import-image: failed to delete snapshot %s for %s; retained for recovery\n' "$snapshot" "$name" >&2
    fi
  else
    printf 'import-image: snapshot %s for %s retained; resolve image association before cleanup\n' "$snapshot" "$name" >&2
  fi
  return "$status"
}

main "$@"
