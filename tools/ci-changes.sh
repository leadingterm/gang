#!/usr/bin/env bash
# Classify changes that require the heavy CI jobs.
set -euo pipefail

if (( $# != 2 )); then
  echo "Usage: $0 BASE HEAD" >&2
  exit 2
fi

if [[ -n $1 && $1 != *[!0]* ]]; then
  echo 'heavy=true'
  exit 0
fi

changes=$(mktemp)
trap 'rm -f "$changes"' EXIT
if ! git diff --no-renames --name-only -z "$1" "$2" -- >"$changes"; then
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
