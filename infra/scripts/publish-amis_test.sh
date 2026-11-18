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
    [[ $# == 10 && $3 == --region && $4 == region-a && $5 == --owners && $6 == self && $7 == --include-deprecated && $8 == --include-disabled && $9 == --output && ${10} == json ]] || exit 92
    if [[ -f $CASE_DIR/image ]]; then printf '{"Images":[{"ImageId":"ami-old","State":"disabled"}]}\n'; else printf '{"Images":[]}\n'; fi ;;
  'ec2 describe-snapshots')
    [[ $# == 10 && $3 == --region && $4 == region-a && $5 == --owner-ids && $6 == self && $7 == --filters && $8 == Name=tag-key,Values=ec2:SharedTag/GangRetiredImage && $9 == --output && ${10} == json ]] || exit 93
    if [[ -f $CASE_DIR/retired && -f $CASE_DIR/snapshot ]]; then printf '{"Snapshots":[{"SnapshotId":"snap-old","Tags":[{"Key":"ec2:SharedTag/GangRetiredImage","Value":"ami-old"}]}]}\n'; else printf '{"Snapshots":[]}\n'; fi ;;
  'service-quotas get-service-quota')
    [[ "$*" == 'service-quotas get-service-quota --region region-a --service-code ec2 --quota-code L-0E3CBAB9 --query Quota.Value --output json' ]] || exit 94
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
    [[ "$*" == 'ec2 copy-image --region region-a --source-region region-a --source-image-id ami-source --name copy-name --description copy description --copy-image-tags' ]] || exit 98 ;;
  'ec2 wait')
    [[ "$*" == 'ec2 wait image-available --region region-a --image-ids ami-wait' ]] || exit 98 ;;
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
[[ $1 == release && ( $2 == ami-steps || $2 == ami-manifest ) ]] || exit 90
operation=$2
shift 2
[[ $# == 10 && $1 == --home-region && $2 == region-a && $3 == --now && $5 == --release && $6 == v1 && $7 == --image && $8 == x86_64=sha-x && $9 == --image && ${10} == aarch64=sha-a ]] || exit 91
input=$(cat)
printf '%s\n' "$input" >>"$CASE_DIR/observations"
jq -e '. == {"region-a": {quota: 20, images: {Images: (if $image then [{ImageId:"ami-old", State:"disabled"}] else [] end)}, snapshots: {Snapshots: (if $retired and $snapshot then [{SnapshotId:"snap-old", Tags:[{Key:"ec2:SharedTag/GangRetiredImage",Value:"ami-old"}]}] else [] end)}}}' \
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
IMPORT
chmod +x "$case_dir/scripts/import-image.sh"
chmod +x "$fixtures/aws" "$fixtures/gang"
: >"$case_dir/aws-calls"
: >"$case_dir/observations"
: >"$case_dir/image"
: >"$case_dir/snapshot"
export GANG="$fixtures/gang" IMAGE_HOME_REGION=region-a IMAGE_REGIONS=region-a RELEASE=v1
export IMAGES_DIR="$case_dir/images" IMAGE_SHA256_x86_64=sha-x IMAGE_SHA256_aarch64=sha-a

cd "$case_dir"
if "$case_dir/scripts/publish-amis.sh" >out 2>err; then
  printf 'publication must fail when deletion fails\n' >&2
  exit 1
fi
[[ -f snapshot && -f retired && ! -f image && -f delete-failed ]] || { printf 'failed deletion lost retry state\n' >&2; exit 1; }
"$case_dir/scripts/publish-amis.sh" >out 2>err
[[ ! -f snapshot && $(<gang-amis.json) == '{"manifest":"result"}' ]] || { printf 'retry did not finish publication\n' >&2; exit 1; }
count=0
while IFS= read -r call; do
  [[ $call != 'ec2 delete-snapshot --region region-a --snapshot-id snap-old ' ]] || count=$((count + 1))
done <aws-calls
[[ $count == 2 ]] || { printf 'expected two deletion attempts, got %s\n' "$count" >&2; exit 1; }

FULL_SCENARIO=1 "$case_dir/scripts/publish-amis.sh" >out 2>err
[[ -f imported ]] || { printf 'upload was not executed\n' >&2; exit 1; }
for operation in copy-image wait enable-image-deregistration-protection modify-image-attribute enable-image-deprecation disable-image-deregistration-protection; do
  found=0
  while IFS= read -r call; do
    [[ $call != "ec2 $operation "* ]] || found=$((found + 1))
  done <aws-calls
  [[ $found == 1 ]] || { printf 'expected one %s call, got %s\n' "$operation" "$found" >&2; exit 1; }
done
