#!/usr/bin/env bash
# Exercise cache export, manifest, subtraction, merge and limit using local fixtures.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
cache="$script_dir/ci-cache.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export TZ=UTC
cases=0
failures=0

check() {
  local description=$1
  shift
  cases=$((cases + 1))
  if ! "$@"; then
    echo "FAIL: $description" >&2
    failures=$((failures + 1))
  fi
}

reject() {
  local description=$1
  shift
  cases=$((cases + 1))
  if "$@" >"$scratch/output" 2>"$scratch/error"; then
    echo "FAIL: $description" >&2
    failures=$((failures + 1))
  fi
}

key1=$(printf '%064x' 1)
key2=$(printf '%064x' 2)
key3=$(printf '%064x' 3)
key4=$(printf '%064x' 4)
key5=$(printf '%064x' 5)
key6=$(printf '%064x' 6)
epoch=1704067200

disk="$scratch/disk input"
repo="$scratch/repository input"
exported="$scratch/exported"
expected="$scratch/expected"
mkdir -p "$disk/ac/00" "$disk/ac/ff" "$disk/cas/00" "$disk/tmp" "$disk/gc" \
  "$disk/unknown/00" "$repo/content_addressable/sha256/$key1" \
  "$repo/content_addressable/sha256/$key2" "$repo/content_addressable/sha256/$key3" \
  "$expected/disk/ac/00" "$expected/disk/cas/00" \
  "$expected/repository/content_addressable/sha256/$key1" \
  "$expected/repository/content_addressable/sha256/$key2"
printf 'active ac\n' >"$disk/ac/00/$key1"
printf 'wrong shard\n' >"$disk/ac/ff/$key1"
printf 'boundary ac\n' >"$disk/ac/00/$key2"
printf 'old ac\n' >"$disk/ac/00/$key3"
printf 'active cas\n' >"$disk/cas/00/$key4"
printf 'old cas\n' >"$disk/cas/00/$key5"
printf 'temporary\n' >"$disk/tmp/$key1"
printf 'garbage\n' >"$disk/gc/$key1"
printf 'unknown\n' >"$disk/unknown/00/$key1"
printf 'bad key\n' >"$disk/ac/00/not-a-digest"
ln -s "$disk/ac/00/$key1" "$disk/ac/00/$key6"
printf 'repository active\n' >"$repo/content_addressable/sha256/$key1/file"
printf 'active marker\n' >"$repo/content_addressable/sha256/$key1/id-active"
printf 'repository boundary\n' >"$repo/content_addressable/sha256/$key2/file"
printf 'boundary marker\n' >"$repo/content_addressable/sha256/$key2/id-boundary"
printf 'repository old\n' >"$repo/content_addressable/sha256/$key3/file"
printf 'old marker\n' >"$repo/content_addressable/sha256/$key3/id-old"
touch -t 202312312359.59 "$disk/ac/00/$key3" "$disk/cas/00/$key5" \
  "$repo/content_addressable/sha256/$key3/file" \
  "$repo/content_addressable/sha256/$key1/id-active" \
  "$repo/content_addressable/sha256/$key2/id-boundary"
touch -t 202401010000.00 "$disk/ac/00/$key2" "$repo/content_addressable/sha256/$key2/file"
touch -t 202401010000.01 "$disk/ac/00/$key1" "$disk/cas/00/$key4" \
  "$repo/content_addressable/sha256/$key1/file" \
  "$repo/content_addressable/sha256/$key3/id-old"
cp "$disk/ac/00/$key1" "$disk/ac/00/$key2" "$expected/disk/ac/00/"
cp "$disk/cas/00/$key4" "$expected/disk/cas/00/"
cp "$repo/content_addressable/sha256/$key1/"* "$expected/repository/content_addressable/sha256/$key1/"
cp "$repo/content_addressable/sha256/$key2/"* "$expected/repository/content_addressable/sha256/$key2/"
check 'export succeeds' "$cache" export "$disk" "$repo" "$epoch" "$exported"
check 'export selects only active cache entries and complete repository entries' diff -r "$expected" "$exported"
check 'export preserves disk payload' cmp -s "$disk/ac/00/$key1" "$exported/disk/ac/00/$key1"
check 'export preserves repository marker' cmp -s "$repo/content_addressable/sha256/$key1/id-active" "$exported/repository/content_addressable/sha256/$key1/id-active"
check 'missing input directories export as empty' "$cache" export "$scratch/no-disk" "$scratch/no-repo" "$epoch" "$scratch/empty"
check 'empty export destination exists' test -d "$scratch/empty"
check 'empty export contains no files' test -z "$(find "$scratch/empty" -type f -print 2>/dev/null)"
reject 'export requires arguments' "$cache" export
reject 'export rejects extra arguments' "$cache" export "$disk" "$repo" "$epoch" "$scratch/extra" extra
reject 'export rejects invalid epoch' "$cache" export "$disk" "$repo" invalid "$scratch/bad-epoch"
reject 'unknown command fails' "$cache" unknown
reject 'missing command fails' "$cache"
reject 'merge requires a destination and source' "$cache" merge "$scratch/no-source"

source1="$scratch/source one"
source2="$scratch/source two"
for source in "$source1" "$source2"; do
  mkdir -p "$source/disk/ac/00" "$source/disk/cas/00" \
    "$source/repository/content_addressable/sha256/$key1"
done
printf 'old action\n' >"$source1/disk/ac/00/$key1"
printf 'new action\n' >"$source2/disk/ac/00/$key1"
printf 'same cas\n' >"$source1/disk/cas/00/$key2"
printf 'same cas\n' >"$source2/disk/cas/00/$key2"
printf 'only first\n' >"$source1/disk/cas/00/$key3"
printf 'only second\n' >"$source2/disk/cas/00/$key4"
printf 'repository payload\n' >"$source1/repository/content_addressable/sha256/$key1/file"
printf 'repository payload\n' >"$source2/repository/content_addressable/sha256/$key1/file"
printf 'first marker\n' >"$source1/repository/content_addressable/sha256/$key1/id-first"
printf 'second marker\n' >"$source2/repository/content_addressable/sha256/$key1/id-second"
touch -t 202401010000.00 "$source1/disk/ac/00/$key1"
touch -t 202401010000.01 "$source2/disk/ac/00/$key1"
cp -pR "$source1" "$scratch/source1-before"
cp -pR "$source2" "$scratch/source2-before"
merged="$scratch/merged"
check 'merge accepts disjoint and equal-content entries' "$cache" merge "$merged" "$source1" "$source2"
check 'merge picks newest action' cmp -s "$source2/disk/ac/00/$key1" "$merged/disk/ac/00/$key1"
check 'merge retains shared CAS' cmp -s "$source1/disk/cas/00/$key2" "$merged/disk/cas/00/$key2"
check 'merge retains first disjoint CAS' cmp -s "$source1/disk/cas/00/$key3" "$merged/disk/cas/00/$key3"
check 'merge retains second disjoint CAS' cmp -s "$source2/disk/cas/00/$key4" "$merged/disk/cas/00/$key4"
check 'reverse merge accepts disjoint and equal-content entries' "$cache" merge "$scratch/merged-reverse" "$source2" "$source1"
check 'newest action is independent of source order' cmp -s "$source2/disk/ac/00/$key1" "$scratch/merged-reverse/disk/ac/00/$key1"
check 'merge retains repository payload' cmp -s "$source1/repository/content_addressable/sha256/$key1/file" "$merged/repository/content_addressable/sha256/$key1/file"
check 'merge retains first repository marker' cmp -s "$source1/repository/content_addressable/sha256/$key1/id-first" "$merged/repository/content_addressable/sha256/$key1/id-first"
check 'merge retains second repository marker' cmp -s "$source2/repository/content_addressable/sha256/$key1/id-second" "$merged/repository/content_addressable/sha256/$key1/id-second"
check 'first source remains unchanged' diff -r "$scratch/source1-before" "$source1"
check 'second source remains unchanged' diff -r "$scratch/source2-before" "$source2"

# Equal timestamps must resolve identically regardless of source order.
touch -t 202401010000.00 "$source2/disk/ac/00/$key1"
check 'merge accepts equal-time actions' "$cache" merge "$scratch/tie-forward" "$source1" "$source2"
check 'reverse merge accepts equal-time actions' "$cache" merge "$scratch/tie-reverse" "$source2" "$source1"
check 'equal-time action choice is deterministic' cmp -s "$scratch/tie-forward/disk/ac/00/$key1" "$scratch/tie-reverse/disk/ac/00/$key1"

printf 'different cas\n' >"$source2/disk/cas/00/$key2"
reject 'merge rejects different content for the same CAS key' "$cache" merge "$scratch/cas-collision" "$source1" "$source2"
printf 'same cas\n' >"$source2/disk/cas/00/$key2"
printf 'different repository payload\n' >"$source2/repository/content_addressable/sha256/$key1/file"
reject 'merge rejects different repository payload for the same checksum' "$cache" merge "$scratch/repo-collision" "$source1" "$source2"
ln -s "$scratch/outside" "$source1/disk/cas/00/$key5"
reject 'merge rejects symlink entries' "$cache" merge "$scratch/symlink-merge" "$source1"
mkdir -p "$scratch/path-component"
reject 'merge rejects traversal in source path' "$cache" merge "$scratch/traversal-merge" "$scratch/path-component/../source two"

# The devShell supplies tar, zstd and coreutils for the actual compressed budget.
compressed_size() {
  tar -C "$1" -cf - . | zstd -q -3 -T0 | wc -c
}

payload="$scratch/large-payload"
for ((i = 0; i < 2300; i++)); do
  printf '%s\n' "$i" | sha256sum
done >"$payload"

limit_source="$scratch/limit-source"
mkdir -p "$limit_source/disk/cas/00"
cp "$payload" "$limit_source/disk/cas/00/$key1"
printf 'newer entry\n' >"$limit_source/disk/cas/00/$key2"
touch -t 202401010000.00 "$limit_source/disk/cas/00/$key1"
touch -t 202401010000.01 "$limit_source/disk/cas/00/$key2"
cp -pR "$limit_source" "$scratch/limit-source-before"
cp -pR "$limit_source" "$scratch/limit-under-budget"
check 'limit accepts a stage already under budget' "$cache" limit "$scratch/limit-under-budget" 1048576
check 'under-budget stage is unchanged' diff -r "$limit_source" "$scratch/limit-under-budget"
check 'under-budget stage fits compressed budget' test "$(compressed_size "$scratch/limit-under-budget")" -le 1048576

cp -pR "$limit_source" "$scratch/limit-disk"
check 'disk eviction fixture exceeds compressed budget' test "$(compressed_size "$scratch/limit-disk")" -gt 65536
check 'limit evicts disk entries to fit' "$cache" limit "$scratch/limit-disk" 65536
check 'oldest disk entry is evicted first' test ! -e "$scratch/limit-disk/disk/cas/00/$key1"
check 'newer disk entry remains' cmp -s "$limit_source/disk/cas/00/$key2" "$scratch/limit-disk/disk/cas/00/$key2"
check 'disk stage fits compressed budget' test "$(compressed_size "$scratch/limit-disk")" -le 65536
check 'limiting stage leaves source unchanged' diff -r "$scratch/limit-source-before" "$limit_source"

repo_stage="$scratch/limit-repository"
mkdir -p "$repo_stage/repository/content_addressable/sha256/$key1" \
  "$repo_stage/repository/content_addressable/sha256/$key2"
cp "$payload" "$repo_stage/repository/content_addressable/sha256/$key1/file"
printf 'old marker\n' >"$repo_stage/repository/content_addressable/sha256/$key1/id-old"
printf 'new repository payload\n' >"$repo_stage/repository/content_addressable/sha256/$key2/file"
printf 'new marker one\n' >"$repo_stage/repository/content_addressable/sha256/$key2/id-one"
printf 'new marker two\n' >"$repo_stage/repository/content_addressable/sha256/$key2/id-two"
touch -t 202401010000.00 "$repo_stage/repository/content_addressable/sha256/$key1/file" \
  "$repo_stage/repository/content_addressable/sha256/$key1/id-old"
touch -t 202401010000.01 "$repo_stage/repository/content_addressable/sha256/$key2/file" \
  "$repo_stage/repository/content_addressable/sha256/$key2/id-one" \
  "$repo_stage/repository/content_addressable/sha256/$key2/id-two"
cp -pR "$repo_stage" "$scratch/repository-before"
check 'repository eviction fixture exceeds compressed budget' test "$(compressed_size "$repo_stage")" -gt 65536
check 'limit evicts repository entries to fit' "$cache" limit "$repo_stage" 65536
check 'evicted repository checksum has no payload or markers' test ! -e "$repo_stage/repository/content_addressable/sha256/$key1"
check 'retained repository checksum keeps payload and every marker' diff -r \
  "$scratch/repository-before/repository/content_addressable/sha256/$key2" \
  "$repo_stage/repository/content_addressable/sha256/$key2"
check 'repository stage fits compressed budget' test "$(compressed_size "$repo_stage")" -le 65536

reject 'limit requires a budget' "$cache" limit "$scratch/limit-under-budget"
reject 'limit rejects extra arguments' "$cache" limit "$scratch/limit-under-budget" 65536 extra
reject 'limit rejects zero budget' "$cache" limit "$scratch/limit-under-budget" 0
reject 'limit rejects negative budget' "$cache" limit "$scratch/limit-under-budget" -1
reject 'limit rejects non-integer budget' "$cache" limit "$scratch/limit-under-budget" 1.5
reject 'limit rejects nonnumeric budget' "$cache" limit "$scratch/limit-under-budget" invalid
mkdir -p "$scratch/limit-symlink/disk/cas/00"
ln -s "$payload" "$scratch/limit-symlink/disk/cas/00/$key1"
reject 'limit rejects symlink entries' "$cache" limit "$scratch/limit-symlink" 65536

# Manifest captures all reusable entries regardless of age, but ignores noise and symlinks.
manifest="$scratch/base-manifest"
check 'manifest accepts live cache roots' "$cache" manifest "$disk" "$repo" >"$scratch/live-manifest"
printf '%s\0' \
  "disk/ac/00/$key1" "disk/ac/00/$key2" "disk/ac/00/$key3" \
  "disk/cas/00/$key4" "disk/cas/00/$key5" \
  "repository/content_addressable/sha256/$key1/file" \
  "repository/content_addressable/sha256/$key1/id-active" \
  "repository/content_addressable/sha256/$key2/file" \
  "repository/content_addressable/sha256/$key2/id-boundary" \
  "repository/content_addressable/sha256/$key3/file" \
  "repository/content_addressable/sha256/$key3/id-old" >"$scratch/expected-manifest"
check 'manifest is sorted and contains only reusable entries' cmp -s "$scratch/expected-manifest" "$scratch/live-manifest"
mkdir -p "$repo/content_addressable/sha256/$key4" "$repo/content_addressable/sha256/$key5"
printf 'orphan' >"$repo/content_addressable/sha256/$key4/id-orphan"
ln -s "$repo/content_addressable/sha256/$key1/file" "$repo/content_addressable/sha256/$key5/file"
printf 'linked payload marker' >"$repo/content_addressable/sha256/$key5/id-linked"
"$cache" manifest "$disk" "$repo" >"$scratch/no-orphans"
check 'manifest excludes markers without regular payloads' cmp -s "$scratch/expected-manifest" "$scratch/no-orphans"
"$cache" manifest "$exported/disk" "$exported/repository" >"$manifest"
cp "$manifest" "$scratch/base-manifest-before"
cp -pR "$exported" "$scratch/base-before"
cp -pR "$exported" "$scratch/delta"
printf 'new CAS\n' >"$scratch/delta/disk/cas/00/$key6"
printf 'new marker\n' >"$scratch/delta/repository/content_addressable/sha256/$key2/id-new"
check 'subtract accepts valid base manifest' "$cache" subtract "$scratch/delta" "$manifest"
check 'subtract removes duplicate action' test ! -e "$scratch/delta/disk/ac/00/$key1"
check 'subtract removes duplicate CAS' test ! -e "$scratch/delta/disk/cas/00/$key4"
check 'subtract keeps new CAS' test -f "$scratch/delta/disk/cas/00/$key6"
check 'subtract removes whole base repository unit' test ! -e "$scratch/delta/repository/content_addressable/sha256/$key1"
check 'new marker keeps repository payload and old marker' cmp -s \
  "$exported/repository/content_addressable/sha256/$key2/file" \
  "$scratch/delta/repository/content_addressable/sha256/$key2/file"
check 'new marker keeps old marker' test -f "$scratch/delta/repository/content_addressable/sha256/$key2/id-boundary"
check 'new marker remains' test -f "$scratch/delta/repository/content_addressable/sha256/$key2/id-new"
check 'base cache remains unchanged' diff -r "$scratch/base-before" "$exported"
check 'base manifest remains unchanged' cmp -s "$scratch/base-manifest-before" "$manifest"
cp -pR "$exported" "$scratch/all-base"
check 'all-base subtraction succeeds' "$cache" subtract "$scratch/all-base" "$manifest"
check 'all-base subtraction leaves no files' test -z "$(find "$scratch/all-base" -type f -print)"
check 'empty delta retains disk root' test -d "$scratch/all-base/disk"
check 'empty delta retains repository root' test -d "$scratch/all-base/repository/content_addressable"
: >"$scratch/empty-manifest"
cp -pR "$exported" "$scratch/no-base"
check 'empty manifest subtracts nothing' "$cache" subtract "$scratch/no-base" "$scratch/empty-manifest"
check 'empty manifest retains all files' diff -r "$exported" "$scratch/no-base"
reject 'manifest requires both roots' "$cache" manifest "$disk"
reject 'manifest rejects filesystem root' "$cache" manifest / "$repo"
reject 'manifest rejects root traversal' "$cache" manifest "$scratch/path-component/../disk input" "$repo"
ln -s "$disk" "$scratch/disk-link"
reject 'manifest rejects symlink root' "$cache" manifest "$scratch/disk-link" "$repo"
reject 'subtract rejects staging root symlink' "$cache" subtract "$scratch/disk-link" "$manifest"
ln -s "$manifest" "$scratch/manifest-link"
reject 'subtract rejects manifest symlink' "$cache" subtract "$scratch/no-base" "$scratch/manifest-link"
reject 'subtract rejects staging symlink entries' "$cache" subtract "$scratch/limit-symlink" "$manifest"
reject 'subtract rejects traversal in staging root' "$cache" subtract "$scratch/path-component/../no-base" "$manifest"
for invalid in "/disk/ac/00/$key1" "disk/../disk/ac/00/$key1" \
  "disk/ac/ff/$key1" "disk/tmp/$key1" "repository/content_addressable/sha256/$key1" ""; do
  printf '%s\0' "$invalid" >"$scratch/invalid-manifest"
  reject 'subtract rejects invalid manifest path' "$cache" subtract "$scratch/no-base" "$scratch/invalid-manifest"
done
printf 'disk/ac/00/%s' "$key1" >"$scratch/invalid-manifest"
reject 'subtract rejects unterminated manifest' "$cache" subtract "$scratch/no-base" "$scratch/invalid-manifest"
check 'invalid manifests do not mutate stage' diff -r "$exported" "$scratch/no-base"

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
