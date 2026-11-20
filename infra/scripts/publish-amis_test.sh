#!/usr/bin/env bash
# Exercise publication glue with stateful fake AWS and gang; jq is a pinned runfile.
set -euo pipefail

case "$PUBLISH_AMIS" in /*) script=$PUBLISH_AMIS ;; *) script="$PWD/$PUBLISH_AMIS" ;; esac
case "$PUBLISH_JQ" in /*) jq_path=$PUBLISH_JQ ;; *) jq_path="$PWD/$PUBLISH_JQ" ;; esac
case_dir="$TEST_TMPDIR/publication"
fixtures="$case_dir/fixtures"
mkdir -p "$fixtures" "$case_dir/scripts" "$case_dir/images"
cp "$script" "$case_dir/scripts/publish-amis.sh"
PATH="$fixtures:$(dirname -- "$jq_path"):$PATH"
export PATH CASE_DIR="$case_dir"

cat >"$fixtures/aws" <<'AWS'
#!/usr/bin/env bash
set -euo pipefail
[[ ${AWS_PAGER-unset} == '' ]] || exit 90
printf -v call '%q ' "$@"
printf '%s\n' "$call" >>"$CASE_DIR/aws-calls"
case "$1 $2" in
  'ec2 describe-images')
    if [[ $5 == --image-ids ]]; then
      case "$*" in
        'ec2 describe-images --region region-a --image-ids ami-abcdef --include-deprecated --include-disabled --output json')
          [[ -f $CASE_DIR/imported ]] || exit 92
          printf '{"Images":[{"ImageId":"ami-abcdef","State":"available"}]}\n' ;;
        'ec2 describe-images --region region-a --image-ids ami-abcdef ami-fedcba --include-deprecated --include-disabled --output json')
          [[ -f $CASE_DIR/imported && -f $CASE_DIR/copied-region-a ]] || exit 92
          printf '{"Images":[{"ImageId":"ami-abcdef","State":"available"},{"ImageId":"ami-fedcba","State":"available"}]}\n' ;;
        'ec2 describe-images --region region-b --image-ids ami-fedcba --include-deprecated --include-disabled --output json')
          [[ -f $CASE_DIR/copied-region-b ]] || exit 92
          printf '{"Images":[{"ImageId":"ami-fedcba","State":"available"}]}\n' ;;
        *) exit 92 ;;
      esac
    else
      [[ $# == 10 && $3 == --region && $5 == --owners && $6 == self && $7 == --include-deprecated && $8 == --include-disabled && $9 == --output && ${10} == json ]] || exit 92
      if [[ ${WRONG_ENVIRONMENT:-} == 1 ]]; then
        printf '{"Images":[{"Name":"gang-x86_64-sha-x","Tags":[{"Key":"ec2:SharedTag/ActionEnvironment","Value":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}]}]}\n'
      elif [[ $4 == region-a && -f $CASE_DIR/image ]]; then
        printf '{"Images":[{"ImageId":"ami-old","State":"disabled"}]}\n'
      elif [[ $4 == region-b && -f $CASE_DIR/copied-region-b && ${STALE_LISTING:-} == 1 ]]; then
        printf '{"Images":[{"ImageId":"ami-fedcba","State":"pending"}]}\n'
      else printf '{"Images":[]}\n'; fi
    fi ;;
  'ec2 describe-snapshots')
    [[ $# == 10 && $3 == --region && $5 == --owner-ids && $6 == self && $7 == --filters && $8 == Name=tag-key,Values=ec2:SharedTag/GangRetiredImage && $9 == --output && ${10} == json ]] || exit 93
    if [[ $4 == region-a && -f $CASE_DIR/retired && -f $CASE_DIR/snapshot ]]; then printf '{"Snapshots":[{"SnapshotId":"snap-old","Tags":[{"Key":"ec2:SharedTag/GangRetiredImage","Value":"ami-old"}]}]}\n'; else printf '{"Snapshots":[]}\n'; fi ;;
  'service-quotas get-service-quota')
    [[ $# == 12 && $3 == --region && $5 == --service-code && $6 == ec2 && $7 == --quota-code && $8 == L-0E3CBAB9 && $9 == --query && ${10} == Quota.Value && ${11} == --output && ${12} == json ]] || exit 94
    printf '20.0\n' ;;
  'ec2 create-tags')
    [[ "$*" == 'ec2 create-tags --region region-a --resources snap-old --tags Key=ec2:SharedTag/GangRetiredImage,Value=ami-old' && -f $CASE_DIR/snapshot && -f $CASE_DIR/image ]] || exit 95
    : >"$CASE_DIR/retired" ;;
  'ec2 deregister-image')
    [[ "$*" == 'ec2 deregister-image --region region-a --image-id ami-old' && -f $CASE_DIR/retired && -f $CASE_DIR/image ]] || exit 96
    rm -- "$CASE_DIR/image" ;;
  'ec2 delete-snapshot')
    [[ "$*" == 'ec2 delete-snapshot --region region-a --snapshot-id snap-old' && -f $CASE_DIR/retired && -f $CASE_DIR/snapshot && ! -f $CASE_DIR/image ]] || exit 97
    if [[ ! -f $CASE_DIR/delete-failed ]]; then : >"$CASE_DIR/delete-failed"; exit 1; fi
    rm -- "$CASE_DIR/snapshot" ;;
  'ec2 copy-image')
    [[ "$*" == "ec2 copy-image --region $4 --source-region region-a --source-image-id ami-source --name copy-name --description copy description --copy-image-tags --query ImageId --output text" && ( $4 == region-a || $4 == region-b ) ]] || exit 98
    : >"$CASE_DIR/copied-$4"
    printf '%s\n' copy >>"$CASE_DIR/copy-count"
    printf '%s\n' ami-fedcba ;;
  'ec2 wait')
    [[ "$*" == 'ec2 wait image-available --region region-a --image-ids ami-wait' ||
       "$*" == 'ec2 wait image-available --region region-a --image-ids ami-abcdef' ||
       "$*" == 'ec2 wait image-available --region region-a --image-ids ami-fedcba' ||
       "$*" == 'ec2 wait image-available --region region-b --image-ids ami-fedcba' ]] || exit 98 ;;
  'ec2 enable-image-deregistration-protection')
    [[ "$*" == 'ec2 enable-image-deregistration-protection --region region-a --image-id ami-protect --with-cooldown' ]] || exit 98 ;;
  'ec2 modify-image-attribute')
    [[ "$*" == 'ec2 modify-image-attribute --region region-a --image-id ami-publish --launch-permission Add=[{Group=all}]' ]] || exit 98 ;;
  'ec2 enable-image-deprecation')
    [[ "$*" == 'ec2 enable-image-deprecation --region region-a --image-id ami-deprecate --deprecate-at 2030-01-02T03:04:05Z' ]] || exit 98 ;;
  'ec2 disable-image-deregistration-protection')
    [[ "$*" == 'ec2 disable-image-deregistration-protection --region region-a --image-id ami-unprotect' ]] || exit 98 ;;
  *) printf 'unexpected AWS call: %s\n' "$*" >&2; exit 98 ;;
esac
AWS
cat >"$fixtures/gang" <<'GANG'
#!/usr/bin/env bash
set -euo pipefail
[[ $1 == release && ( $2 == ami-steps || $2 == ami-release-steps || $2 == ami-manifest ) ]] || exit 90
operation=$2
shift 2
[[ $# == 10 && $1 == --home-region && $2 == region-a && $3 == --now && $5 == --release && $6 == v1 && $7 == --image && $8 == x86_64=sha-x && $9 == --image && ${10} == aarch64=sha-a ]] || exit 91
input=$(cat)
printf '%s\n' "$input" >>"$CASE_DIR/observations"
printf '%s\n' "$operation" >>"$CASE_DIR/operations"
if [[ ${CATALOG_SCENARIO:-} == 1 ]]; then
  [[ $operation == ami-steps ]] || exit 94
  jq -e 'keys == ["region-a","region-b","region-c"]' <<<"$input" >/dev/null || exit 95
  exit 0
fi
if [[ ${SCOPE_SCENARIO:-} == 1 ]]; then
  [[ $operation == ami-release-steps || $operation == ami-manifest ]] || exit 94
  jq -e --argjson expected "${EXPECTED_REGIONS:-[\"region-a\",\"region-b\"]}" 'keys == $expected' <<<"$input" >/dev/null || exit 95
  if [[ $operation == ami-manifest ]]; then
    printf '{"manifest":"scoped"}\n'
  elif ! jq -e '.["region-a"].images.Images | any(.ImageId == "ami-abcdef" and .State == "available")' <<<"$input" >/dev/null; then
    printf '%s\n' $'upload\tx86_64\tgang-name\tsha-value\tv1\timage description'
  elif [[ ${COPY_SCENARIO:-} == 1 ]] && ! jq -e '.["region-b"].images.Images | any(.ImageId == "ami-fedcba" and .State == "available")' <<<"$input" >/dev/null; then
    printf '%s\n' $'copy\tregion-b\tami-source\tcopy-name\tcopy description'
  elif [[ ${COPY_SCENARIO:-} == 1 ]]; then
    jq -e '[.["region-b"].images.Images[] | select(.ImageId == "ami-fedcba")] | length == 1' <<<"$input" >/dev/null || exit 96
  fi
  exit 0
fi
jq -e '. == {"region-a": {quota: 20, images: {Images: ((if $image then [{ImageId:"ami-old", State:"disabled"}] else [] end) + (if $imported then [{ImageId:"ami-abcdef", State:"available"},{ImageId:"ami-fedcba", State:"available"}] else [] end) | sort_by(.ImageId))}, snapshots: {Snapshots: (if $retired and $snapshot then [{SnapshotId:"snap-old", Tags:[{Key:"ec2:SharedTag/GangRetiredImage",Value:"ami-old"}]}] else [] end)}}}' \
  --argjson imported "$(if [[ -f $CASE_DIR/imported && ${FULL_SCENARIO:-} == 1 ]]; then echo true; else echo false; fi)" \
  --argjson image "$(if [[ -f $CASE_DIR/image ]]; then echo true; else echo false; fi)" \
  --argjson retired "$(if [[ -f $CASE_DIR/retired ]]; then echo true; else echo false; fi)" \
  --argjson snapshot "$(if [[ -f $CASE_DIR/snapshot ]]; then echo true; else echo false; fi)" <<<"$input" >/dev/null || exit 92
if [[ ${FULL_SCENARIO:-} == 1 && $operation == ami-steps ]]; then
  if [[ ! -f $CASE_DIR/full-steps ]]; then
    : >"$CASE_DIR/full-steps"
    printf '%s\n' \
      $'upload\tx86_64\tgang-name\tsha-value\tv1\timage description' \
      $'copy\tregion-a\tami-source\tcopy-name\tcopy description' \
      $'wait\tregion-a\tami-wait' \
      $'protect\tregion-a\tami-protect' \
      $'publish\tregion-a\tami-publish' \
      $'deprecate\tregion-a\tami-deprecate\t2030-01-02T03:04:05Z' \
      $'unprotect\tregion-a\tami-unprotect'
  fi
  exit 0
fi
if [[ $operation == ami-manifest ]]; then
  [[ ! -f $CASE_DIR/image && ! -f $CASE_DIR/snapshot ]] || exit 93
  printf '{"manifest":"result"}\n'
elif [[ -f $CASE_DIR/image ]]; then
  printf '%s\n' $'retire-snapshot\tregion-a\tsnap-old\tami-old' $'deregister\tregion-a\tami-old'
elif [[ -f $CASE_DIR/snapshot ]]; then
  printf '%s\n' $'delete-snapshot\tregion-a\tsnap-old'
fi
GANG
cat >"$case_dir/scripts/import-image.sh" <<'IMPORT'
#!/usr/bin/env bash
set -euo pipefail
[[ $AWS_REGION == region-a && $AWS_DEFAULT_REGION == region-a && $# == 7 &&
  $1 == "$IMAGES_DIR/gang-image-x86_64.raw.zst" && $2 == "$IMAGES_DIR/gang-image-x86_64.json" &&
  $3 == gang-name && $4 == x86_64 && $5 == sha-value && $6 == v1 && $7 == 'image description' ]] || exit 99
: >"$CASE_DIR/imported"
printf '%s\n' import >>"$CASE_DIR/import-count"
printf '%s\n' ami-abcdef
IMPORT
chmod +x "$case_dir/scripts/import-image.sh"
chmod +x "$fixtures/aws" "$fixtures/gang"
: >"$case_dir/aws-calls"
: >"$case_dir/observations"
: >"$case_dir/image"
: >"$case_dir/snapshot"
export GANG="$fixtures/gang" IMAGE_HOME_REGION=region-a IMAGE_REGIONS=region-a RELEASE=v1
export IMAGES_DIR="$case_dir/images" IMAGE_SHA256_x86_64=sha-x IMAGE_SHA256_aarch64=sha-a
printf '{"action_environment":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}\n' >"$IMAGES_DIR/gang-image-x86_64.json"
printf '{"action_environment":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}\n' >"$IMAGES_DIR/gang-image-aarch64.json"

for bad_info in '{}' '{"action_environment":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}'; do
  printf '%s\n' "$bad_info" >"$IMAGES_DIR/gang-image-aarch64.json"
  : >"$case_dir/aws-calls"
  if PUBLICATION_SCOPE=release "$case_dir/scripts/publish-amis.sh" >"$case_dir/out" 2>"$case_dir/err"; then
    printf 'accepted missing or mismatched image action environment\n' >&2
    exit 1
  fi
  [[ ! -s "$case_dir/aws-calls" ]] || { printf 'AWS called before rejecting image metadata\n' >&2; exit 1; }
done
printf '{"action_environment":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}\n' >"$IMAGES_DIR/gang-image-aarch64.json"
: >"$case_dir/operations"
if PUBLICATION_SCOPE=release WRONG_ENVIRONMENT=1 "$case_dir/scripts/publish-amis.sh" >"$case_dir/out" 2>"$case_dir/err"; then
  printf 'accepted an AMI with an unrelated action environment\n' >&2
  exit 1
fi
[[ ! -s "$case_dir/operations" ]] || { printf 'planned publication before checking AMI identity\n' >&2; exit 1; }

cd "$case_dir"
if "$case_dir/scripts/publish-amis.sh" >out 2>err; then
  printf 'publication must fail when deletion fails\n' >&2
  exit 1
fi
[[ -f snapshot && -f retired && ! -f image && -f delete-failed ]] || { printf 'failed deletion lost retry state\n' >&2; exit 1; }
"$case_dir/scripts/publish-amis.sh" >out 2>err
[[ ! -f snapshot && ! -f gang-amis.json ]] || { printf 'retry did not finish catalog publication\n' >&2; exit 1; }
count=0
while IFS= read -r call; do
  [[ $call != 'ec2 delete-snapshot --region region-a --snapshot-id snap-old ' ]] || count=$((count + 1))
done <aws-calls
[[ $count == 2 ]] || { printf 'expected two deletion attempts, got %s\n' "$count" >&2; exit 1; }

FULL_SCENARIO=1 "$case_dir/scripts/publish-amis.sh" >out 2>err
[[ -f imported ]] || { printf 'upload was not executed\n' >&2; exit 1; }
for operation in copy-image enable-image-deregistration-protection modify-image-attribute enable-image-deprecation disable-image-deregistration-protection; do
  found=0
  while IFS= read -r call; do
    [[ $call != "ec2 $operation "* ]] || found=$((found + 1))
  done <aws-calls
  [[ $found == 1 ]] || { printf 'expected one %s call, got %s\n' "$operation" "$found" >&2; exit 1; }
done
for id in ami-wait ami-abcdef ami-fedcba; do
  found=0
  while IFS= read -r call; do
    [[ $call != "ec2 wait image-available --region region-a --image-ids $id " ]] || found=$((found + 1))
  done <aws-calls
  [[ $found == 1 ]] || { printf 'expected one wait for %s, got %s\n' "$id" "$found" >&2; exit 1; }
done

# A delayed owner listing must not cause a second upload after the import succeeds.
rm -f imported import-count
: >aws-calls
: >operations
PUBLICATION_SCOPE=release SCOPE_SCENARIO=1 IMAGE_REGIONS='region-a region-b region-c region-b' \
  AMI_DEPLOY_REGIONS='region-b region-a region-b' "$case_dir/scripts/publish-amis.sh" >out 2>err
[[ $(<gang-amis.json) == '{"manifest":"scoped"}' && $(<import-count) == import ]] || {
  printf 'scoped publication did not converge with one import\n' >&2; exit 1;
}
[[ $(<operations) == $'ami-release-steps\nami-release-steps\nami-manifest' ]] || {
  printf 'unexpected release operations\n' >&2; exit 1;
}
for region in region-a region-b; do
  count=0
  while IFS= read -r call; do
    [[ $call != "ec2 describe-images --region $region --owners "* ]] || count=$((count + 1))
  done <aws-calls
  [[ $count == 2 ]] || { printf 'unexpected owner listing count for %s\n' "$region" >&2; exit 1; }
done
while IFS= read -r call; do
  [[ $call != *'--region region-c '* ]] || { printf 'release queried unrelated region\n' >&2; exit 1; }
done <aws-calls
# Copy handles survive a delayed listing and override stale listed state.
for stale in 0 1; do
  rm -f imported import-count copied-region-b copy-count
  : >aws-calls
  PUBLICATION_SCOPE=release SCOPE_SCENARIO=1 COPY_SCENARIO=1 STALE_LISTING="$stale" \
    IMAGE_REGIONS='region-a region-b' AMI_DEPLOY_REGIONS=region-b \
    "$case_dir/scripts/publish-amis.sh" >out 2>err
  [[ $(<import-count) == import && $(<copy-count) == copy ]] || {
    printf 'delayed listing caused repeated creation\n' >&2; exit 1;
  }
  found=0
  while IFS= read -r call; do
    [[ $call != 'ec2 wait image-available --region region-b --image-ids ami-fedcba ' ]] || found=$((found + 1))
  done <aws-calls
  [[ $found == 1 ]] || { printf 'expected one copied image wait\n' >&2; exit 1; }
done


rm -f imported import-count
: >aws-calls
PUBLICATION_SCOPE=release SCOPE_SCENARIO=1 EXPECTED_REGIONS='["region-a"]' \
  IMAGE_REGIONS='region-a region-b' AMI_DEPLOY_REGIONS='' \
  "$case_dir/scripts/publish-amis.sh" >out 2>err
while IFS= read -r call; do
  [[ $call != *'--region region-b '* ]] || { printf 'default release queried non-home region\n' >&2; exit 1; }
done <aws-calls

: >aws-calls
CATALOG_SCENARIO=1 IMAGE_REGIONS='region-a region-b region-c region-b' \
  "$case_dir/scripts/publish-amis.sh" >out 2>err
for region in region-a region-b region-c; do
  count=0
  while IFS= read -r call; do
    [[ $call != "ec2 describe-images --region $region --owners "* ]] || count=$((count + 1))
  done <aws-calls
  [[ $count == 1 ]] || { printf 'catalog did not query %s once\n' "$region" >&2; exit 1; }
done

for invalid in home configured deploy outside missing-home missing-release scope; do
  : >aws-calls
  case "$invalid" in
    home) overrides=(IMAGE_HOME_REGION='bad!') ;;
    configured) overrides=(IMAGE_REGIONS='region-a bad!') ;;
    deploy) overrides=(PUBLICATION_SCOPE=release AMI_DEPLOY_REGIONS='bad!') ;;
    outside) overrides=(PUBLICATION_SCOPE=release AMI_DEPLOY_REGIONS='region-z') ;;
    missing-home) overrides=(IMAGE_REGIONS=region-b) ;;
    missing-release) overrides=(PUBLICATION_SCOPE=release RELEASE=) ;;
    scope) overrides=(PUBLICATION_SCOPE=invalid) ;;
  esac
  if env "${overrides[@]}" "$case_dir/scripts/publish-amis.sh" >out 2>err; then
    printf 'accepted invalid %s input\n' "$invalid" >&2; exit 1
  fi
  [[ ! -s aws-calls ]] || { printf 'AWS called before rejecting %s\n' "$invalid" >&2; exit 1; }
done
