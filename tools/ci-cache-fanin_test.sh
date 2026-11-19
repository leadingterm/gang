#!/usr/bin/env bash
# Exercise the actual snapshot preparation step with download-artifact layouts.
set -euo pipefail
export TZ=UTC

script_dir=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$script_dir/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
body="$scratch/prepare.sh"
yq -er '.runs.steps[] | select(.name == "Prepare snapshot") | .run' \
  "$repo/.github/actions/save-bazel-cache/action.yml" >"$body"
export_body="$scratch/export.sh"
yq -er '.runs.steps[] | select(.name == "Export shard working set") | .run' \
  "$repo/.github/actions/save-bazel-cache/action.yml" >"$export_body"
cd "$repo"

key1=$(printf '%064x' 1)
key2=$(printf '%064x' 2)
key3=$(printf '%064x' 3)
key4=$(printf '%064x' 4)
key5=$(printf '%064x' 5)
key6=$(printf '%064x' 6)
fixture="$scratch/fixture"
mkdir -p "$fixture/disk/ac/00" "$fixture/repository/content_addressable/sha256/$key2"
printf 'action entry\n' >"$fixture/disk/ac/00/$key1"
printf 'repository archive\n' >"$fixture/repository/content_addressable/sha256/$key2/file"
printf 'marker\n' >"$fixture/repository/content_addressable/sha256/$key2/id-test"
touch -t 202312312359.59 "$fixture/disk/ac/00/$key1" \
  "$fixture/repository/content_addressable/sha256/$key2/"*
mkdir -p "$scratch/working-set"
tools/ci-cache.sh export "$fixture/disk" "$fixture/repository" 0 "$scratch/working-set/files"
tools/ci-cache.sh manifest "$scratch/working-set/files/disk" \
  "$scratch/working-set/files/repository" >"$scratch/working-set/used"
printf '%s\0' "disk/cas/00/$key3" \
  "repository/content_addressable/sha256/$key3/file" >>"$scratch/working-set/used"
sort -zu "$scratch/working-set/used" -o "$scratch/working-set/used"

setup_case() {
  local case_dir=$1
  export HOME="$case_dir/home" RUNNER_TEMP="$case_dir/runner"
  export BAZEL_DISK_CACHE="$HOME/.cache/bazel-disk"
  export BAZEL_REPOSITORY_CACHE="$HOME/.cache/bazel-repository"
  export GITHUB_OUTPUT="$case_dir/output" GITHUB_STEP_SUMMARY="$case_dir/summary"
  export GITHUB_ENV="$case_dir/env"
  export MERGE_PATH="$case_dir/download" MODE=merge SNAPSHOT_BUDGET=1000000
  export EVENT=pull_request BAZEL_CACHE_JOB=linux-x86_64 BAZEL_DISK_CACHE_KEY=test-key
  export BAZEL_DISK_CACHE_EPOCH_SECONDS=1704067200
  unset BAZEL_BASE_MANIFEST BAZEL_BASE_PRESENT BAZEL_ADDITIONAL_USED_MANIFEST BAZEL_RESTORED_MANIFEST
  mkdir -p "$RUNNER_TEMP" "$BAZEL_DISK_CACHE/cas/00" \
    "$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3" \
    "$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key4"
  printf 'live base\n' >"$BAZEL_DISK_CACHE/live-base"
  printf 'used restored CAS\n' >"$BAZEL_DISK_CACHE/cas/00/$key3"
  printf 'unused restored CAS\n' >"$BAZEL_DISK_CACHE/cas/00/$key4"
  printf 'used restored repository\n' >"$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3/file"
  printf 'used restored marker\n' >"$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3/id-used"
  printf 'unused restored repository\n' >"$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key4/file"
  touch -t 202312312359.59 "$BAZEL_DISK_CACHE/cas/00/"* \
    "$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3/"* \
    "$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key4/"*
  : >"$GITHUB_OUTPUT"
  : >"$GITHUB_STEP_SUMMARY"
  : >"$GITHUB_ENV"
}

run_prepare() {
  setup_case "$1"
  bash -euo pipefail "$body"
}

assert_snapshot() {
  [[ $(<"$GITHUB_OUTPUT") == $'has-snapshot=false\nhas-snapshot=true' ]]
  cmp "$fixture/disk/ac/00/$key1" "$BAZEL_DISK_CACHE/ac/00/$key1"
  cmp "$fixture/repository/content_addressable/sha256/$key2/file" \
    "$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key2/file"
  cmp "$fixture/repository/content_addressable/sha256/$key2/id-test" \
    "$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key2/id-test"
  [[ $(<"$BAZEL_DISK_CACHE/cas/00/$key3") == 'used restored CAS' ]]
  [[ -f $BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3/file ]]
  [[ -f $BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3/id-used ]]
  [[ ! -e $BAZEL_DISK_CACHE/cas/00/$key4 ]]
  [[ ! -e $BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key4 ]]
  [[ ! -e $BAZEL_DISK_CACHE/live-base ]]
}

zero="$scratch/zero"
run_prepare "$zero"
[[ $(<"$GITHUB_OUTPUT") == 'has-snapshot=false' ]]
[[ $(<"$BAZEL_DISK_CACHE/live-base") == 'live base' ]]
[[ -s $GITHUB_STEP_SUMMARY ]]
empty="$scratch/empty"
mkdir -p "$empty/download"
run_prepare "$empty"
[[ $(<"$GITHUB_OUTPUT") == 'has-snapshot=false' ]]

one="$scratch/one"
mkdir -p "$one/download"
cp -pR "$scratch/working-set/." "$one/download/"
run_prepare "$one"
assert_snapshot

two="$scratch/two"
mkdir -p "$two/download/shard-a/files/disk" "$two/download/shard-b/files/repository"
cp -pR "$scratch/working-set/files/disk/." "$two/download/shard-a/files/disk/"
cp -pR "$scratch/working-set/files/repository/." "$two/download/shard-b/files/repository/"
printf '%s\0' "disk/ac/00/$key1" "disk/cas/00/$key3" >"$two/download/shard-a/used"
printf '%s\0' "repository/content_addressable/sha256/$key3/file" \
  "repository/content_addressable/sha256/$key2/id-test" \
  "repository/content_addressable/sha256/$key2/file" \
  "disk/cas/00/$key3" >"$two/download/shard-b/used"
run_prepare "$two"
assert_snapshot
cmp "$scratch/working-set/used" "$RUNNER_TEMP/bazel-merged-used"

# The real x86 fan-in includes four checks shards plus the bundle and lint.
six="$scratch/six"
for shard in 1 2 3 4 bundle lint; do
  mkdir -p "$six/download/bazel-working-set-x86-$shard"
  cp "$scratch/working-set/used" "$six/download/bazel-working-set-x86-$shard/used"
done
cp -pR "$scratch/working-set/files" "$six/download/bazel-working-set-x86-1/"
lint_files="$six/download/bazel-working-set-x86-lint/files/disk/cas/00"
bundle_files="$six/download/bazel-working-set-x86-bundle/files/repository/content_addressable/sha256/$key6"
mkdir -p "$lint_files" "$bundle_files"
printf 'lint output\n' >"$lint_files/$key5"
printf 'bundle output\n' >"$bundle_files/file"
printf '%s\0' "disk/cas/00/$key5" >>"$six/download/bazel-working-set-x86-lint/used"
printf '%s\0' "repository/content_addressable/sha256/$key6/file" >>"$six/download/bazel-working-set-x86-bundle/used"
run_prepare "$six"
assert_snapshot
[[ $(<"$BAZEL_DISK_CACHE/cas/00/$key5") == 'lint output' ]]
[[ $(<"$BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key6/file") == 'bundle output' ]]

trusted="$scratch/trusted"
mkdir -p "$trusted/download"
cp -pR "$scratch/working-set/." "$trusted/download/"
setup_case "$trusted"
printf '%s\0' "disk/cas/00/$key3" \
  "repository/content_addressable/sha256/$key3/file" >"$trusted/base"
export BAZEL_BASE_MANIFEST="$trusted/base"
bash -euo pipefail "$body"
[[ $(<"$GITHUB_OUTPUT") == $'has-snapshot=false\nhas-snapshot=true' ]]
[[ -f $BAZEL_DISK_CACHE/ac/00/$key1 ]]
[[ -f $BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key2/file ]]
[[ ! -e $BAZEL_DISK_CACHE/cas/00/$key3 ]]
[[ ! -e $BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key3 ]]
[[ ! -e $BAZEL_DISK_CACHE/cas/00/$key4 ]]

metadata="$scratch/metadata"
for child in shard-a shard-b; do
  mkdir -p "$metadata/download/$child"
  if [[ $child == shard-b ]]; then
    tools/ci-cache.sh export "$scratch/absent-disk" "$scratch/absent-repo" 0 "$metadata/download/$child/files"
  fi
  cp "$scratch/working-set/used" "$metadata/download/$child/used"
done
run_prepare "$metadata"
[[ $(<"$GITHUB_OUTPUT") == 'has-snapshot=false' ]]
[[ $(<"$BAZEL_DISK_CACHE/live-base") == 'live base' ]]
[[ -f $BAZEL_DISK_CACHE/cas/00/$key4 ]]
[[ -f $BAZEL_REPOSITORY_CACHE/content_addressable/sha256/$key4/file ]]

truly_empty="$scratch/truly-empty"
mkdir -p "$truly_empty/download"
: >"$truly_empty/download/used"
run_prepare "$truly_empty"
[[ $(<"$GITHUB_OUTPUT") == 'has-snapshot=false' ]]

# A malformed later manifest cannot be repaired by unioning or sorting records.
bad="$scratch/bad"
mkdir -p "$bad/download"
cp -pR "$scratch/working-set/." "$bad/download/"
printf 'repository/content_addressable/sha256/%s/id-unterminated' "$key2" >"$bad/download/used"
setup_case "$bad"
if bash -euo pipefail "$body" >"$bad/rejection.log" 2>&1; then
  cat "$bad/rejection.log" >&2
  echo 'FAIL: unterminated artifact manifest accepted' >&2
  exit 1
fi
if [[ $(<"$bad/rejection.log") != '::error::Unterminated used manifest' ]]; then
  cat "$bad/rejection.log" >&2
  echo 'FAIL: malformed artifact failed for an unexpected reason' >&2
  exit 1
fi
[[ $(<"$BAZEL_DISK_CACHE/live-base") == 'live base' ]]

# Export records the complete used set before removing restored payloads.
producer="$scratch/producer"
setup_case "$producer"
cp -pR "$fixture/disk/." "$BAZEL_DISK_CACHE/"
cp -pR "$fixture/repository/." "$BAZEL_REPOSITORY_CACHE/"
tools/ci-cache.sh manifest "$BAZEL_DISK_CACHE" "$BAZEL_REPOSITORY_CACHE" >"$producer/restored"
export BAZEL_RESTORED_MANIFEST="$producer/restored"
printf 'new action\n' >"$BAZEL_DISK_CACHE/cas/00/$key5"
find "$BAZEL_DISK_CACHE" "$BAZEL_REPOSITORY_CACHE" -type f -exec touch -t 202401010000.01 {} +
tools/ci-cache.sh manifest "$BAZEL_DISK_CACHE" "$BAZEL_REPOSITORY_CACHE" >"$producer/expected-used"
bash -euo pipefail "$export_body"
cmp "$producer/expected-used" "$RUNNER_TEMP/bazel-working-set/used"
[[ $(find "$RUNNER_TEMP/bazel-working-set/files" -type f | wc -l) -eq 1 ]]
[[ -f $RUNNER_TEMP/bazel-working-set/files/disk/cas/00/$key5 ]]

# Execute the whole preparation body to observe the effective compressed cap.
# Missing main cache must never promote a PR delta to the main snapshot budget.
for event in pull_request push; do
  if [[ $event == pull_request ]]; then cap=300000000; else cap=3000000000; fi
  for base_present in true false; do
    for override in '' 9000000000 1000000; do
      budget_case="$scratch/budget-$event-$base_present-${override:-default}"
      mkdir -p "$budget_case/download"
      cp -pR "$scratch/working-set/." "$budget_case/download/"
      setup_case "$budget_case"
      export EVENT=$event BAZEL_BASE_PRESENT=$base_present SNAPSHOT_BUDGET=$override
      bash -euo pipefail "$body"
      expected=$cap
      if [[ -n $override && $override -lt $cap ]]; then expected=$override; fi
      [[ $(<"$GITHUB_OUTPUT") == $'has-snapshot=false\nhas-snapshot=true' ]]
      [[ $(<"$GITHUB_STEP_SUMMARY") == *"(budget $expected)"* ]]
    done
  done
done
printf 'Bazel cache fan-in: x86 shards, bundle and lint; artifact layouts; exact used sets; producer deltas; and compressed budget caps passed\n'
