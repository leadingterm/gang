#!/usr/bin/env bash
# CI's lint job, which tools/gate.sh runs first (docs/SPECIFICATION.md §7 items 4 and 5): Nix formatting, the shell
# scripts, the workflows, every flake output of every system, then macOS Clippy of every Cargo target, the darwin check's
# only runner. Stops at the first failure.
# Usage: nix develop -c tools/lint.sh
set -euo pipefail
cd "$(dirname "$0")/.."
alejandra --check .
shellcheck infra/scripts/*.sh tools/*.sh
# actionlint finds its project (its configuration, every workflow and the local actions it checks) only beside a .git
# entry: it lints a copy of .github that has one, so a source tree without Git history is linted as CI's checkout is.
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cp -R .github .shellcheckrc "$scratch/"
mkdir "$scratch/.git"
(cd "$scratch" && actionlint)
nix flake check --no-build --all-systems
nix run .#darwin-check
