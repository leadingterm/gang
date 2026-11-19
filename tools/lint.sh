#!/usr/bin/env bash
# The canonical CI lint aggregate covers lint-arm64 (--portable) and the separate nix-vm job (--nix-vm).
# Portable checks cover OpenTofu tests, formatting, shell scripts, workflows, flake evaluation, and macOS Clippy.
# The VM checks require x86_64 Linux/KVM. Stops at the first failure.
# Usage: nix develop -c tools/lint.sh [--portable|--nix-vm]
set -euo pipefail
cd "$(dirname "$0")/.."
portable() {
  tools/check-required-jobs_test.sh
  tools/check-opentofu-tests.sh
  tools/ci-changes_test.sh
  tools/ci-targets_test.sh
  tools/ci-cache_test.sh
  tools/ci-cache-policy_test.sh
  tools/ci-cache-fanin_test.sh
  tools/release-notes_test.sh
  tools/release-ci_test.sh
  alejandra --check .
  shellcheck infra/scripts/*.sh tools/*.sh
  # actionlint requires a .git entry beside its configuration, workflows, and local actions.
  (
    scratch=$(mktemp -d)
    trap 'rm -rf "$scratch"' EXIT
    cp -R .github .shellcheckrc "$scratch/"
    mkdir "$scratch/.git"
    cd "$scratch"
    actionlint
  )
  nix flake check --no-build --all-systems
  nix run .#darwin-check
}

nix_vm() {
  if [[ $(uname -sm) != 'Linux x86_64' ]]; then
    echo 'The Nix VM checks require Linux x86_64.' >&2
    return 1
  fi
  local names name
  local -a checks=() link_args=(--no-link)
  names=$(nix eval --json .#checks.x86_64-linux --apply builtins.attrNames | jq -er '.[]')
  if [[ -z $names ]]; then
    echo 'No x86_64-linux Nix checks found.' >&2
    return 1
  fi
  while IFS= read -r name; do
    checks+=(".#checks.x86_64-linux.$name")
  done <<< "$names"
  if [[ -n ${NIX_CHECK_RESULTS:-} ]]; then
    link_args=(--out-link "$NIX_CHECK_RESULTS")
  fi
  nix build "${link_args[@]}" --print-build-logs "${checks[@]}"
}

if (( $# == 0 )); then
  portable
  nix_vm
elif (( $# == 1 )); then
  case "$1" in
    --portable) portable ;;
    --nix-vm) nix_vm ;;
    *) echo "Usage: $0 [--portable|--nix-vm]" >&2; exit 2 ;;
  esac
else
  echo "Usage: $0 [--portable|--nix-vm]" >&2
  exit 2
fi
