#!/usr/bin/env bash
# Exercise real git comparisons and checksum verification against offline release fixtures.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export FIXTURE="$scratch/release" CALLS="$scratch/calls" GH_REPO=example/gang TAG=v2.0.0 RUNNER_TEMP="$scratch" CASE=ok
mkdir -p "$scratch/repo/infra/nixos" "$scratch/repo/infra/container" "$scratch/repo/staged" "$FIXTURE" "$scratch/bin"
cd "$scratch/repo"
git init -q
git config user.email test@example.com
git config user.name Test
printf 'flake' >flake.nix
printf 'lock' >flake.lock
printf 'disk' >infra/nixos/system.nix
printf 'ca' >infra/nixos/rds-ca-bundle.pem
printf 'container' >infra/container/image.nix
git add .
git commit -qm baseline
git tag v1.0.0
export GITHUB_SHA
GITHUB_SHA=$(git rev-parse HEAD)
printf binary >staged/gang-aarch64-unknown-linux-musl
cp staged/gang-aarch64-unknown-linux-musl "$FIXTURE/"
for asset in gang-image-aarch64.raw.zst gang-image-aarch64.json gang-controller.oci.tar gang-controller.json; do printf '%s' "$asset" >"$FIXTURE/$asset"; done
(cd "$FIXTURE" && sha256sum gang-* >SHA256SUMS)
cat >"$scratch/bin/gh" <<'GH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CALLS"
case "$1 $2" in
  'release list')
    [[ $CASE != missing ]] || { printf '[]\n'; exit; }
    printf '[{"tagName":"v1.0.0","isDraft":false,"publishedAt":"2026-01-01T00:00:00Z"}]\n' ;;
  'release download')
    while [[ $# -gt 0 ]]; do
      case $1 in --pattern) asset=$2; shift 2 ;; --dir) dir=$2; shift 2 ;; *) shift ;; esac
    done
    cp "$FIXTURE/$asset" "$dir/$asset"
    if [[ $CASE == tamper && $asset == *.json ]]; then printf x >>"$dir/$asset"; fi ;;
  'attestation verify')
    [[ $* == *'--source-ref refs/tags/v1.0.0' && $CASE != bad_attestation ]] ;;
  *) exit 1 ;;
esac
GH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH"
for mode in disk controller; do
  : >"$CALLS"
  "$root/infra/scripts/reuse-release-image.sh" "$mode" aarch64 "$scratch/out-$mode" >/dev/null
  [[ $(grep -c '^attestation verify ' "$CALLS") == 3 ]]
done
for CASE in missing tamper bad_attestation; do
  export CASE
  if "$root/infra/scripts/reuse-release-image.sh" disk aarch64 "$scratch/fail-$CASE" >/dev/null 2>&1; then
    echo "$CASE unexpectedly reused an image" >&2; exit 1
  fi
done
CASE=ok
printf changed >staged/gang-aarch64-unknown-linux-musl
if "$root/infra/scripts/reuse-release-image.sh" disk aarch64 "$scratch/fail-binary" >/dev/null 2>&1; then exit 1; fi
printf binary >staged/gang-aarch64-unknown-linux-musl
printf changed >infra/nixos/rds-ca-bundle.pem
git add infra/nixos/rds-ca-bundle.pem
git commit -qm 'change CA'
GITHUB_SHA=$(git rev-parse HEAD)
export GITHUB_SHA
for mode in disk controller; do
  if "$root/infra/scripts/reuse-release-image.sh" "$mode" aarch64 "$scratch/fail-change" >/dev/null 2>&1; then exit 1; fi
done
