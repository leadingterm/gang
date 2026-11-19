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
  local notes='' generated=false prerelease=false verified=false darwin=0 linux_x86=0 linux_arm=0 sums=0 expected_notes
  while (( $# )); do
    case "$1" in
      --notes) notes=$2; shift 2 ;;
      --title) [[ $2 == "$TAG" ]] || return 1; shift 2 ;;
      --generate-notes) generated=true; shift ;;
      --prerelease) prerelease=true; shift ;;
      --verify-tag) verified=true; shift ;;
      gang-aarch64-apple-darwin) ((darwin += 1)); shift ;;
      gang-x86_64-unknown-linux-musl) ((linux_x86 += 1)); shift ;;
      gang-aarch64-unknown-linux-musl) ((linux_arm += 1)); shift ;;
      SHA256SUMS) ((sums += 1)); shift ;;
      *) return 1 ;;
    esac
  done
  expected_notes="$COMMIT_MESSAGE"$'\n\n'"Binaries built and tested by CI run $RUN_URL. Primary public AMIs of each architecture: gang-amis.json."
  if [[ $DARWIN == false ]]; then
    expected_notes+=$'\n\n'"The macOS binary is not included in this release."
  fi
  [[ $notes == "$expected_notes" ]] || return 1
  [[ $generated == true && $verified == true && $prerelease == "$EXPECTED_PRERELEASE" ]] || return 1
  [[ $linux_x86 == 1 && $linux_arm == 1 && $sums == 1 ]] || return 1
  if [[ $DARWIN == true ]]; then [[ $darwin == 1 ]] || return 1
  else [[ $darwin == 0 ]] || return 1; fi
  printf 'published\n' >"$capture"
}
export -f gh
cases=0
for TAG in v1.2.3 v1.2.3-rc.1; do
  export TAG EXPECTED_PRERELEASE=false
  if [[ $TAG == *-* ]]; then EXPECTED_PRERELEASE=true; fi
  for DARWIN in true false; do
    export DARWIN
    dist="$scratch/$TAG-$DARWIN/dist"
    mkdir -p "$dist"
    : >"$dist/gang-x86_64-unknown-linux-musl"
    : >"$dist/gang-aarch64-unknown-linux-musl"
    : >"$dist/SHA256SUMS"
    if [[ $DARWIN == true ]]; then : >"$dist/gang-aarch64-apple-darwin"; fi
    (cd "$dist" && bash --noprofile --norc -euo pipefail "$body")
    [[ -f $capture ]]
    rm "$capture"
    ((cases += 1))
  done
done
export API_FAIL=true
if (cd "$dist" && bash --noprofile --norc -euo pipefail "$body"); then
  echo 'commit lookup failure must stop publication' >&2
  exit 1
fi
[[ ! -e $capture ]]
((cases += 1))
printf '%d cases, 0 failures\n' "$cases"
