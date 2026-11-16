#!/usr/bin/env bash
# Exercise the publication glue against fake AWS, gang, and image import commands.
set -euo pipefail

script="$PWD/$PUBLISH_AMIS"
fixtures="$TEST_TMPDIR/fixtures"
case_dir="$TEST_TMPDIR/publication"
mkdir -p "$fixtures" "$case_dir/images" "$case_dir/scripts"
cp "$script" "$case_dir/scripts/publish-amis.sh"

cat > "$fixtures/aws" <<'AWS'
#!/usr/bin/env bash
set -euo pipefail
[[ "${AWS_PAGER-unset}" == '' ]] || exit 1
printf -v call '%q ' "$@"
printf '%s\n' "$call" >> "$AWS_CALLS"
case "$1 $2" in
  'ec2 describe-images') printf '{"Images":[]}\n' ;;
  'service-quotas get-service-quota') printf '20.0\n' ;;
esac
AWS
cat > "$fixtures/gang" <<'GANG'
#!/usr/bin/env bash
set -euo pipefail
printf -v call '%q ' "$@"
printf '%s\n' "$call" >> "$GANG_CALLS"
cat > "$REGIONS_INPUT"
case "$2" in
  ami-steps)
    if [[ ! -e "$STEP_CALLED" ]]; then
      : > "$STEP_CALLED"
      printf '%s\n' \
        $'upload\tx86_64\tgang-name\tsha-value\tv1\timage description' \
        $'copy\tregion-b\tami-source\tcopy-name\tcopy description' \
        $'wait\tregion-b\tami-wait' \
        $'protect\tregion-b\tami-protect' \
        $'publish\tregion-b\tami-publish' \
        $'deprecate\tregion-b\tami-deprecate\t2030-01-02T03:04:05Z' \
        $'unprotect\tregion-b\tami-unprotect' \
        $'deregister\tregion-b\tami-deregister'
    fi
    ;;
  ami-manifest) printf '{"manifest":"result"}\n' ;;
  *) exit 1 ;;
esac
GANG
cat > "$case_dir/scripts/import-image.sh" <<'IMPORT'
#!/usr/bin/env bash
set -euo pipefail
printf -v call '%q ' "$AWS_REGION" "$AWS_DEFAULT_REGION" "$@"
printf '%s\n' "$call" >> "$IMPORT_CALLS"
IMPORT
chmod +x "$fixtures/aws" "$fixtures/gang" "$case_dir/scripts/import-image.sh"
: > "$case_dir/aws-calls"
: > "$case_dir/gang-calls"
: > "$case_dir/import-calls"
: > "$case_dir/summary"

(
  cd "$case_dir"
  PATH="$fixtures:$PATH" AWS_CALLS="$case_dir/aws-calls" GANG_CALLS="$case_dir/gang-calls" \
    IMPORT_CALLS="$case_dir/import-calls" REGIONS_INPUT="$case_dir/regions" \
    STEP_CALLED="$case_dir/step-called" GITHUB_STEP_SUMMARY="$case_dir/summary" \
    GANG="$fixtures/gang" IMAGE_HOME_REGION=region-a IMAGE_REGIONS='region-a region-b' \
    RELEASE=v1 IMAGES_DIR="$case_dir/images" IMAGE_SHA256_x86_64=sha-x \
    IMAGE_SHA256_aarch64=sha-a "$case_dir/scripts/publish-amis.sh" > "$case_dir/out" 2>&1
)

failures=0
fail() {
  printf 'FAIL: %s\n' "$*" >&2
  failures=$((failures + 1))
}

assert_call() {
  local file="$1" expected line count=0
  shift
  printf -v expected '%q ' "$@"
  while IFS= read -r line; do
    if [[ "$line" == "$expected" ]]; then
      count=$((count + 1))
    fi
  done < "$file"
  if [[ "$count" -ne 1 ]]; then
    fail "expected one call: $expected (found $count)"
  fi
}
# Each observation is made once per round for each region.
for region in region-a region-b; do
  printf -v expected '%q ' ec2 describe-images --region "$region" --owners self --include-deprecated --output json
  count=0
  while IFS= read -r line; do [[ "$line" != "$expected" ]] || count=$((count + 1)); done < "$case_dir/aws-calls"
  [[ "$count" -eq 2 ]] || fail "describe-images for $region called $count times"
  printf -v expected '%q ' service-quotas get-service-quota --region "$region" --service-code ec2 --quota-code L-0E3CBAB9 --query Quota.Value --output json
  count=0
  while IFS= read -r line; do [[ "$line" != "$expected" ]] || count=$((count + 1)); done < "$case_dir/aws-calls"
  [[ "$count" -eq 2 ]] || fail "quota for $region called $count times"
done
assert_call "$case_dir/aws-calls" ec2 copy-image --region region-b --source-region region-a --source-image-id ami-source --name copy-name --description 'copy description' --copy-image-tags
assert_call "$case_dir/aws-calls" ec2 wait image-available --region region-b --image-ids ami-wait
assert_call "$case_dir/aws-calls" ec2 enable-image-deregistration-protection --region region-b --image-id ami-protect --with-cooldown
assert_call "$case_dir/aws-calls" ec2 modify-image-attribute --region region-b --image-id ami-publish --launch-permission 'Add=[{Group=all}]'
assert_call "$case_dir/aws-calls" ec2 enable-image-deprecation --region region-b --image-id ami-deprecate --deprecate-at 2030-01-02T03:04:05Z
assert_call "$case_dir/aws-calls" ec2 disable-image-deregistration-protection --region region-b --image-id ami-unprotect
assert_call "$case_dir/aws-calls" ec2 deregister-image --region region-b --image-id ami-deregister --delete-associated-snapshots
assert_call "$case_dir/import-calls" region-a region-a "$case_dir/images/gang-image-x86_64.raw.zst" "$case_dir/images/gang-image-x86_64.json" gang-name x86_64 sha-value v1 'image description'
[[ "$(cat "$case_dir/regions")" == '{"region-a":{"quota":20.0,"images":{"Images":[]}},"region-b":{"quota":20.0,"images":{"Images":[]}}}' ]] || fail 'unexpected observed regions JSON'
[[ "$(cat "$case_dir/gang-amis.json")" == '{"manifest":"result"}' ]] || fail 'manifest not written'
[[ "$(wc -l < "$case_dir/summary")" -eq 8 ]] || fail 'summary does not contain all steps'
[[ "$(wc -l < "$case_dir/aws-calls")" -eq 15 ]] || fail 'unexpected AWS call count'
exit "$((failures > 0))"
