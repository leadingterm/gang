#!/usr/bin/env bash
# Exercise the actual snapshot preparation step with download-artifact layouts.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$script_dir/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
body="$scratch/prepare.sh"
yq -er '.runs.steps[] | select(.name == "Prepare snapshot") | .run' \
  "$repo/.github/actions/save-bazel-cache/action.yml" >"$body"
cd "$repo"

key1=$(printf '%064x' 1)
key2=$(printf '%064x' 2)
fixture="$scratch/fixture"
mkdir -p "$fixture/disk/ac/00" "$fixture/repository/content_addressable/sha256/$key2"
printf 'action entry\n' >"$fixture/disk/ac/00/$key1"
printf 'repository archive\n' >"$fixture/repository/content_addressable/sha256/$key2/file"
printf 'marker\n' >"$fixture/repository/content_addressable/sha256/$key2/id-test"
tools/ci-cache.sh export "$fixture/disk" "$fixture/repository" 0 "$scratch/working-set"

run_prepare() {
  local case_dir=$1
  export HOME="$case_dir/home" RUNNER_TEMP="$case_dir/runner"
  export BAZEL_DISK_CACHE="$HOME/.cache/bazel-disk"
  export BAZEL_REPOSITORY_CACHE="$HOME/.cache/bazel-repository"
  export GITHUB_OUTPUT="$case_dir/output" GITHUB_STEP_SUMMARY="$case_dir/summary"
  export MERGE_PATH="$case_dir/download" MODE=merge SNAPSHOT_BUDGET=1000000
  export EVENT=pull_request BAZEL_CACHE_JOB=linux-x86_64 BAZEL_DISK_CACHE_KEY=test-key
  unset BAZEL_BASE_MANIFEST
  mkdir -p "$RUNNER_TEMP" "$BAZEL_DISK_CACHE"
  printf 'live base\n' >"$BAZEL_DISK_CACHE/live-base"
  : >"$GITHUB_OUTPUT"
  : >"$GITHUB_STEP_SUMMARY"
  bash -euo pipefail "$body"
}

zero="$scratch/zero"
run_prepare "$zero"
[[ $(<"$zero/output") == 'has-snapshot=false' ]]
[[ $(<"$zero/home/.cache/bazel-disk/live-base") == 'live base' ]]
[[ -s $zero/summary ]]

one="$scratch/one"
mkdir -p "$one/download"
cp -R "$scratch/working-set/." "$one/download/"
run_prepare "$one"
[[ $(<"$one/output") == $'has-snapshot=false\nhas-snapshot=true' ]]
cmp "$fixture/disk/ac/00/$key1" "$one/home/.cache/bazel-disk/ac/00/$key1"
cmp "$fixture/repository/content_addressable/sha256/$key2/file" "$one/home/.cache/bazel-repository/content_addressable/sha256/$key2/file"
cmp "$fixture/repository/content_addressable/sha256/$key2/id-test" "$one/home/.cache/bazel-repository/content_addressable/sha256/$key2/id-test"

two="$scratch/two"
mkdir -p "$two/download/shard-a/disk" "$two/download/shard-b/repository"
cp -R "$scratch/working-set/disk/." "$two/download/shard-a/disk/"
cp -R "$scratch/working-set/repository/." "$two/download/shard-b/repository/"
run_prepare "$two"
[[ $(<"$two/output") == $'has-snapshot=false\nhas-snapshot=true' ]]
cmp "$fixture/disk/ac/00/$key1" "$two/home/.cache/bazel-disk/ac/00/$key1"
cmp "$fixture/repository/content_addressable/sha256/$key2/file" "$two/home/.cache/bazel-repository/content_addressable/sha256/$key2/file"
cmp "$fixture/repository/content_addressable/sha256/$key2/id-test" "$two/home/.cache/bazel-repository/content_addressable/sha256/$key2/id-test"
printf 'Bazel cache fan-in: zero, one, and two artifacts passed\n'
