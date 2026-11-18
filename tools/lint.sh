#!/usr/bin/env bash
# The two CI lint work jobs run --portable and --nix-vm, covered by the required lint aggregate; tools/gate.sh runs the full local gate.
# Portable checks cover OpenTofu tests, formatting, shell scripts, workflows, flake evaluation, and macOS Clippy.
# The VM checks require x86_64 Linux/KVM. Stops at the first failure.
# Usage: nix develop -c tools/lint.sh [--portable|--nix-vm]
set -euo pipefail
cd "$(dirname "$0")/.."
portable() {
  tools/check-required-jobs.sh
  tools/check-required-jobs_test.sh
  tools/check-opentofu-tests.sh
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
  nix flake check --all-systems --print-build-logs
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
