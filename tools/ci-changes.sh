#!/usr/bin/env bash
# Classify changes that require the heavy CI jobs.
set -euo pipefail

if (( $# == 3 )) && [[ $1 == --merge-group ]]; then
  queue_ref=$2
  head=$3
  prefix=refs/heads/gh-readonly-queue/
  if [[ $queue_ref != "$prefix"* ]] || ! git check-ref-format "$queue_ref"; then
    echo 'Invalid merge-group queue ref' >&2
    exit 2
  fi
  queue_path=${queue_ref#"$prefix"}
  if [[ $queue_path != */* || ${queue_path##*/} == '' ]]; then
    echo 'Invalid merge-group queue ref' >&2
    exit 2
  fi
  target=${queue_path%/*}
  if [[ -z $target ]] || ! git check-ref-format "refs/heads/$target"; then
    echo 'Invalid merge-group target branch' >&2
    exit 2
  fi
  if ! base=$(git merge-base -- "refs/remotes/origin/$target" "$head"); then
    echo 'Failed to determine merge-group baseline' >&2
    exit 1
  fi
elif (( $# == 2 )) && [[ $1 != --merge-group ]]; then
  base=$1
  head=$2
  if [[ -n $base && $base != *[!0]* ]]; then
    echo 'heavy=true'
    exit 0
  fi
else
  echo "Usage: $0 BASE HEAD | $0 --merge-group QUEUE_REF HEAD" >&2
  exit 2
fi

changes=$(mktemp)
trap 'rm -f "$changes"' EXIT
if ! git diff --no-renames --name-only -z "$base" "$head" -- >"$changes"; then
  echo 'Failed to determine changed files' >&2
  exit 1
fi

while IFS= read -r -d '' path; do
  case "$path" in
    docs/API.md | docs/PLAN.md) echo 'heavy=true'; exit 0 ;;
    README.md | CHANGELOG.md | LICENSE | docs/*.md | notes/*.md) continue ;;
    LICENSE-*) [[ $path != */* ]] && continue ;;
  esac
  echo 'heavy=true'
  exit 0
done <"$changes"

echo 'heavy=false'
