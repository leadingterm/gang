#!/usr/bin/env bash
# Exercise the actual release workflow steps without publishing or a network.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
workflow="$script_dir/../.github/workflows/release.yml"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
render="$scratch/render.sh"
sums="$scratch/sums.sh"
publish="$scratch/publish.sh"
yq -er '.jobs.publish.steps[] | select(.name == "Render changelog") | .run' "$workflow" >"$render"
yq -er '.jobs.publish.steps[] | select(.name == "Write SHA256SUMS") | .run' "$workflow" >"$sums"
yq -er '.jobs.publish.steps[] | select(.name == "Publish the release") | .run' "$workflow" >"$publish"
yq -o=json '.jobs.publish.steps' "$workflow" | jq -e '
  ([.[] | select((.uses // "") | startswith("actions/checkout@")) | .with] == [{"ref":"${{ github.sha }}", "path":".source", "fetch-depth":0, "persist-credentials":false}])
  and ([.[] | select(.name == "Render changelog") | [.env.RELEASE_COMMIT, .env.TAG, .env.SECTION]] == [["${{ github.sha }}", "${{ github.ref_name }}", "${{ runner.temp }}/release-section.md"]])
  and ([.[] | select(.name == "Publish the release") | [.env.SECTION, .env.NOTES, .env.RUN_URL]] == [["${{ runner.temp }}/release-section.md", "${{ runner.temp }}/release-notes.md", "${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ needs.locate.outputs.run-id }}"]])
  and ([.[] | select((.uses // "") | startswith("actions/attest-build-provenance@")) | .with["subject-checksums"]] == ["dist/SHA256SUMS"])
  and (([.[] | .name] | index("Render changelog")) < ([.[] | .name] | index("Write SHA256SUMS")))
  and (([.[] | .name] | index("Write SHA256SUMS")) < ([.[] | .name] | index("Publish the release")))
' >/dev/null
export RELEASE_COMMIT=0123456789012345678901234567890123456789
export RUN_URL=https://github.com/example-org/gang/actions/runs/123
export SECTION="$scratch/section.md" NOTES="$scratch/notes.md" capture="$scratch/capture"
export FIXTURE="$scratch/fixture.md"
printf '%s\n' '## v1.2.3' '' "Keep \`code\`, \$variables, \"quotes\", and \$(literal) intact." '' 'Upgrade warning: preserve the database.' > "$FIXTURE"
python3() {
  [[ $# == 12 && $1 == .source/tools/changelog.py && $2 == --repo && $3 == .source && $4 == render && $5 == --ref && $6 == "$RELEASE_COMMIT" && $7 == --tag && $8 == "$TAG" && $9 == --output && ${10} == dist/CHANGELOG.md && ${11} == --section-output && ${12} == "$SECTION" ]] || return 1
  if [[ ${RENDER_FAIL:-false} == true ]]; then return 23; fi
  cp "$FIXTURE" "$SECTION"
  cp "$FIXTURE" dist/CHANGELOG.md
}
export -f python3
gh() {
  [[ $1 == release && $2 == create && $3 == "$TAG" ]] || return 1
  shift 3
  local notes='' generated=false prerelease=false verified=false darwin=0 linux_x86=0 linux_arm=0 changelog=0 sums=0
  while (( $# )); do
    case "$1" in
      --notes-file) notes=$2; shift 2 ;;
      --title) [[ $2 == "$TAG" ]] || return 1; shift 2 ;;
      --generate-notes) generated=true; shift ;;
      --prerelease) prerelease=true; shift ;;
      --verify-tag) verified=true; shift ;;
      gang-aarch64-apple-darwin) ((darwin += 1)); shift ;;
      gang-x86_64-unknown-linux-musl) ((linux_x86 += 1)); shift ;;
      gang-aarch64-unknown-linux-musl) ((linux_arm += 1)); shift ;;
      CHANGELOG.md) ((changelog += 1)); shift ;;
      SHA256SUMS) ((sums += 1)); shift ;;
      *) return 1 ;;
    esac
  done
  [[ $notes == "$NOTES" ]] || return 1
  cmp "$EXPECTED_NOTES" "$notes" || return 1
  [[ $generated == true && $verified == true && $prerelease == "$EXPECTED_PRERELEASE" ]] || return 1
  [[ $linux_x86 == 1 && $linux_arm == 1 && $changelog == 1 && $sums == 1 ]] || return 1
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
    root="$scratch/$TAG-$DARWIN"
    mkdir -p "$root/dist"
    : >"$root/dist/gang-x86_64-unknown-linux-musl"
    : >"$root/dist/gang-aarch64-unknown-linux-musl"
    if [[ $DARWIN == true ]]; then : >"$root/dist/gang-aarch64-apple-darwin"; fi
    (cd "$root" && bash --noprofile --norc -euo pipefail "$render")
    cmp "$FIXTURE" "$root/dist/CHANGELOG.md"
    (cd "$root/dist" && bash --noprofile --norc -euo pipefail "$sums" && sha256sum --check --strict SHA256SUMS)
    [[ $(grep -c '  CHANGELOG.md$' "$root/dist/SHA256SUMS") == 1 ]]
    export EXPECTED_NOTES="$scratch/expected.md"
    cat "$FIXTURE" > "$EXPECTED_NOTES"
    printf '\n\n%s\n' "Binaries built and tested by CI run $RUN_URL. Primary public AMIs of each architecture: gang-amis.json." >> "$EXPECTED_NOTES"
    if [[ $DARWIN == false ]]; then printf '\n%s\n' 'The macOS binary is not included in this release.' >> "$EXPECTED_NOTES"; fi
    (cd "$root/dist" && bash --noprofile --norc -euo pipefail "$publish")
    [[ -f $capture ]]
    rm "$capture"
    ((cases += 1))
  done
done
export RENDER_FAIL=true
rm "$SECTION"
if (cd "$root" && bash --noprofile --norc -euo pipefail "$render"); then
  echo 'render failure must stop publication' >&2
  exit 1
fi
if (cd "$root/dist" && bash --noprofile --norc -euo pipefail "$publish"); then
  echo 'missing section must stop publication' >&2
  exit 1
fi
[[ ! -e $capture ]]
((cases += 1))
printf '%d cases, 0 failures\n' "$cases"
