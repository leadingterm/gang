#!/usr/bin/env bash
# Exercise the actual publish step with a controlled GitHub boundary, without publishing.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
body="$scratch/publish.sh"
[[ $(yq -er '.jobs.publish.steps[] | select(.name == "Publish the release") | .env.RELEASE_COMMIT' "$script_dir/../.github/workflows/release.yml") == "\${{ github.sha }}" ]]
yq -er '.jobs.publish.steps[] | select(.name == "Publish the release") | .run' "$script_dir/../.github/workflows/release.yml" >"$body"
export GH_REPO=example-org/gang RELEASE_COMMIT=0123456789012345678901234567890123456789
export RUN_URL=https://github.com/example-org/gang/actions/runs/123
export COMMIT_MESSAGE=$'fix: release v1.2.3\n\n## Changes\n\nKeep `code`, $variables and "quotes" intact.\n\nUpgrade warning: preserve the database.'
export capture="$scratch/capture"
gh() {
  if [[ $1 == api ]]; then
    [[ "$*" == "api repos/$GH_REPO/commits/$RELEASE_COMMIT --jq .commit.message" ]] || return 1
    if [[ ${API_FAIL:-false} == true ]]; then return 23; fi
    printf '%s\n' "$COMMIT_MESSAGE"
    return
  fi
  [[ $1 == release && $2 == create && $3 == "$TAG" ]] || return 1
  shift 3
  local notes='' generated=false prerelease=false verified=false
  while (( $# )); do
    case "$1" in
      --notes) notes=$2; shift 2 ;;
      --title) [[ $2 == "$TAG" ]] || return 1; shift 2 ;;
      --generate-notes) generated=true; shift ;;
      --prerelease) prerelease=true; shift ;;
      --verify-tag) verified=true; shift ;;
      gang-* | SHA256SUMS) shift ;;
      *) return 1 ;;
    esac
  done
  [[ $notes == "$COMMIT_MESSAGE"$'\n\n'"Binaries built and tested by CI run $RUN_URL. Public AMIs of each architecture in every image region: gang-amis.json." ]]
  [[ $generated == true && $verified == true && $prerelease == "$EXPECTED_PRERELEASE" ]]
  printf 'published\n' >"$capture"
}
export -f gh
for TAG in v1.2.3 v1.2.3-rc.1; do
  export TAG EXPECTED_PRERELEASE=false
  if [[ $TAG == *-* ]]; then EXPECTED_PRERELEASE=true; fi
  bash --noprofile --norc -euo pipefail "$body"
  [[ -f $capture ]]
  rm "$capture"
done
export API_FAIL=true
if bash --noprofile --norc -euo pipefail "$body"; then
  echo 'commit lookup failure must stop publication' >&2
  exit 1
fi
[[ ! -e $capture ]]
printf '3 cases, 0 failures\n'
