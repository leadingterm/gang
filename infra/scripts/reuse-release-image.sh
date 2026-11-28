#!/usr/bin/env bash
# Reuse only release assets whose inputs, checksums and provenance all match.
set -euo pipefail

mode=${1:?expected disk or controller}
arch=${2:?expected architecture}
output=${3:?expected output directory}
: "${TAG:?}" "${GITHUB_SHA:?}" "${GH_REPO:?}"
case "$mode:$arch" in
  disk:x86_64|disk:aarch64)
    assets=("gang-image-$arch.raw.zst" "gang-image-$arch.json")
    inputs=(flake.nix flake.lock infra/nixos) ;;
  controller:aarch64)
    assets=(gang-controller.oci.tar gang-controller.json)
    inputs=(flake.nix flake.lock infra/container infra/nixos/rds-ca-bundle.pem) ;;
  *) echo 'invalid image mode or architecture' >&2; exit 1 ;;
esac
work=$(mktemp -d "${RUNNER_TEMP:-/tmp}/gang-reuse.XXXXXXXX") || exit 1
trap 'rm -rf "$work"' EXIT
releases=$(gh release list --repo "$GH_REPO" --limit 100 --json tagName,isDraft,publishedAt) || exit 1
prev_tag=$(jq -er --arg current "$TAG" '[.[] | select(.isDraft == false and .tagName != $current and .publishedAt != null)] | sort_by(.publishedAt) | last | .tagName // empty' <<<"$releases") || exit 1
[[ $prev_tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || exit 1
git diff --quiet "$prev_tag" "$GITHUB_SHA" -- "${inputs[@]}" || exit 1
gh release download "$prev_tag" --repo "$GH_REPO" --pattern SHA256SUMS --dir "$work" || exit 1
binary="gang-$arch-unknown-linux-musl"
checksum() {
  jq -Rrn --arg asset "$1" '[inputs | capture("^(?<hash>[0-9a-f]{64})  [*]?(?<name>.+)$")? | select(.name == $asset) | .hash] | if length == 1 then .[0] else error("missing or duplicate checksum") end' <"$work/SHA256SUMS"
}
expected=$(checksum "$binary") || exit 1
actual=$(sha256sum "staged/$binary") || exit 1
[[ ${actual%% *} == "$expected" ]] || exit 1
for asset in "$binary" "${assets[@]}"; do
  checksum "$asset" >/dev/null || exit 1
  gh release download "$prev_tag" --repo "$GH_REPO" --pattern "$asset" --dir "$work" || exit 1
done
(cd "$work" && sha256sum --check --ignore-missing --strict SHA256SUMS) || exit 1
for asset in "$binary" "${assets[@]}"; do
  gh attestation verify "$work/$asset" --repo "$GH_REPO" \
    --cert-identity "https://github.com/$GH_REPO/.github/workflows/release.yml@refs/tags/$prev_tag" \
    --cert-oidc-issuer https://token.actions.githubusercontent.com --source-ref "refs/tags/$prev_tag" || exit 1
done
mkdir -p "$output" || exit 1
for asset in "${assets[@]}"; do cp "$work/$asset" "$output/$asset" || exit 1; done
