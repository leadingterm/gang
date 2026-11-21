#!/usr/bin/env bash
# Exercise change classification against actual Git revisions.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
detector="$script_dir/ci-changes.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/repo"
cd "$scratch/repo"
# Git is the behavior under test: these local fixtures require no network.
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
git init -q
git config user.name 'CI Test'
git config user.email 'ci@example.com'
mkdir -p docs notes src
printf 'readme\n' >README.md
printf 'API\n' >docs/API.md
printf 'plan\n' >docs/PLAN.md
printf 'source\n' >src/main.rs
git add .
git commit -qm initial
base=$(git rev-parse HEAD)
failures=0
cases=0

expect() {
  local expected=$1 description=$2
  shift 2
  cases=$((cases + 1))
  printf 'heavy=%s\n' "$expected" >"$scratch/expected"
  if ! "$detector" "$@" >"$scratch/output" ||
    ! cmp -s "$scratch/expected" "$scratch/output"; then
    echo "FAIL: $description" >&2
    failures=$((failures + 1))
  fi
}

expect_failure() {
  local description=$1
  shift
  cases=$((cases + 1))
  if "$detector" "$@" >"$scratch/output" 2>"$scratch/error"; then
    echo "FAIL: $description unexpectedly succeeded" >&2
    failures=$((failures + 1))
  fi
}

expect false 'empty diff' "$base" "$base"
printf 'documentation\n' >docs/guide.md
mkdir -p notes/nested
printf 'notes\n' >notes/nested/details.md
git add . && git commit -qm docs
docs=$(git rev-parse HEAD)
expect false 'docs-only changes' "$base" "$docs"
printf 'updated API\n' >>docs/API.md
git add . && git commit -qm api
api=$(git rev-parse HEAD)
expect true 'tested API documentation change' "$docs" "$api"
printf 'updated plan\n' >>docs/PLAN.md
git add . && git commit -qm plan
plan=$(git rev-parse HEAD)
expect true 'tested plan documentation change' "$api" "$plan"
printf 'updated\n' >>README.md
git add . && git commit -qm readme
readme=$(git rev-parse HEAD)
expect false 'README change' "$plan" "$readme"
printf 'license\n' >LICENSE-extra.md
git add . && git commit -qm license
license=$(git rev-parse HEAD)
expect false 'root license file' "$readme" "$license"
mkdir LICENSE-extra
printf 'source\n' >LICENSE-extra/source.rs
git add . && git commit -qm license-directory
license_dir=$(git rev-parse HEAD)
expect true 'source under license-named directory' "$license" "$license_dir"
printf 'updated\n' >>src/main.rs
git add . && git commit -qm source
source_rev=$(git rev-parse HEAD)
expect true 'source change' "$license_dir" "$source_rev"
expect true 'mixed changes' "$base" "$source_rev"
printf 'image\n' >docs/image.svg
git add . && git commit -qm image
image=$(git rev-parse HEAD)
expect true 'non-markdown documentation' "$source_rev" "$image"
git mv src/main.rs docs/code.md
git commit -qm rename
renamed=$(git rev-parse HEAD)
expect true 'source renamed into docs' "$image" "$renamed"
printf 'other\n' >src/other.rs
git add . && git commit -qm add-source
added=$(git rev-parse HEAD)
git rm -q src/other.rs
git commit -qm delete-source
deleted=$(git rev-parse HEAD)
expect true 'deleted source' "$added" "$deleted"
printf 'hostile\n' >$'docs/safe.md\nsource.rs'
git add . && git commit -qm newline
newline=$(git rev-parse HEAD)
expect true 'newline in filename' "$deleted" "$newline"
expect true 'initial zero base' 0000000000000000000000000000000000000000 "$newline"
expect_failure 'invalid revision' invalid-revision "$newline"
expect_failure 'missing arguments'

# Model two speculative queue groups while origin still points at the real base.
git checkout -q --detach "$base"
git update-ref refs/remotes/origin/main "$base"
git update-ref refs/remotes/origin/release/stable "$base"
printf 'queue source\n' >>src/main.rs
git add . && git commit -qm queue-source
group_source=$(git rev-parse HEAD)
printf 'queue documentation\n' >docs/queue.md
git add . && git commit -qm queue-docs
group_source_docs=$(git rev-parse HEAD)
expect true 'stacked source then docs against main' --merge-group refs/heads/gh-readonly-queue/main/group-2 "$group_source_docs"
expect true 'stacked source then docs against slash target' --merge-group refs/heads/gh-readonly-queue/release/stable/group-2 "$group_source_docs"

# Once the real target contains the source group, only the docs remain.
git update-ref refs/remotes/origin/main "$group_source"
git update-ref refs/remotes/origin/release/stable "$group_source"
expect false 'main advanced past source' --merge-group refs/heads/gh-readonly-queue/main/group-2 "$group_source_docs"
expect false 'slash target advanced past source' --merge-group refs/heads/gh-readonly-queue/release/stable/group-2 "$group_source_docs"

git checkout -q --detach "$base"
git update-ref refs/remotes/origin/main "$base"
git update-ref refs/remotes/origin/release/stable "$base"
printf 'first docs\n' >docs/first.md
git add . && git commit -qm first-docs
printf 'second docs\n' >docs/second.md
git add . && git commit -qm second-docs
group_docs=$(git rev-parse HEAD)
expect false 'stacked docs against main' --merge-group refs/heads/gh-readonly-queue/main/group-2 "$group_docs"
expect false 'stacked docs against slash target' --merge-group refs/heads/gh-readonly-queue/release/stable/group-2 "$group_docs"

expect_failure 'wrong queue prefix' --merge-group refs/heads/main/group-1 "$group_docs"
expect_failure 'missing target' --merge-group refs/heads/gh-readonly-queue//group-1 "$group_docs"
expect_failure 'missing group leaf' --merge-group refs/heads/gh-readonly-queue/main/ "$group_docs"
expect_failure 'missing target and group leaf' --merge-group refs/heads/gh-readonly-queue/ "$group_docs"
expect_failure 'invalid target' --merge-group refs/heads/gh-readonly-queue/bad..branch/group-1 "$group_docs"
expect_failure 'invalid group leaf' --merge-group 'refs/heads/gh-readonly-queue/main/bad leaf' "$group_docs"
expect_failure 'missing origin target' --merge-group refs/heads/gh-readonly-queue/missing/group-1 "$group_docs"
expect_failure 'invalid group head' --merge-group refs/heads/gh-readonly-queue/main/group-1 invalid-revision
expect_failure 'zero group head' --merge-group refs/heads/gh-readonly-queue/main/group-1 0000000000000000000000000000000000000000
expect_failure 'missing group head' --merge-group refs/heads/gh-readonly-queue/main/group-1

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
