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
  local expected=$1 description=$2 base_rev=$3 head_rev=$4
  cases=$((cases + 1))
  printf 'heavy=%s\n' "$expected" >"$scratch/expected"
  if ! "$detector" "$base_rev" "$head_rev" >"$scratch/output" ||
    ! cmp -s "$scratch/expected" "$scratch/output"; then
    echo "FAIL: $description" >&2
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
cases=$((cases + 1))
if "$detector" invalid-revision "$newline" >"$scratch/output" 2>"$scratch/error"; then
  echo 'FAIL: invalid revision unexpectedly succeeded' >&2
  failures=$((failures + 1))
fi
cases=$((cases + 1))
if "$detector" >"$scratch/output" 2>"$scratch/error"; then
  echo 'FAIL: missing arguments unexpectedly succeeded' >&2
  failures=$((failures + 1))
fi

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
