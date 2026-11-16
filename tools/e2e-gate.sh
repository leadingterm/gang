#!/usr/bin/env bash
# Runs the end-to-end harness the way CI's e2e jobs run it (docs/SPECIFICATION.md §7.1), so a gate on any Linux host
# sees what CI sees: a transient service of the user manager, which leads its own session without a controlling
# terminal, with delegated cgroups, in gang.slice (which a shared host may bound), from the repository root inside the
# devShell, with an empty HOME and neither XDG_CONFIG_HOME nor XDG_CACHE_HOME; Nix itself keeps the user's.
# A run passes only if the harness exits 0, as a Type=oneshot service (systemd counts a simple service killed by SIGHUP,
# SIGINT, SIGTERM or SIGPIPE as a success), and its standard output ends with a passing report: "<N> checks, 0 failed".
# Each run is bounded by the same budget as CI's e2e jobs, so a local gate fails where CI would.
# Usage: tools/e2e-gate.sh <bundle dir> <gang> <harness arguments>...
#   <bundle dir> holds the files of //crates/gang-e2e:bundle; the user manager must delegate cgroup controllers.
set -euo pipefail
# The budget is twice the slowest measured cold CI service runtime: Gate 2 additions took 4 min 0 s at this release;
# Gate 1 took 3 min 21 s and Gate 2 additions 3 min 23 s at the previous release. This keeps a loaded shared host
# from tripping the budget while staying below the CI job's 15-minute backstop. Systemd enforces it as
# TimeoutStartSec because a oneshot service's run is its start, and TimeoutStopSec bounds the stop to 10 seconds.
budget=480
bundle=$(realpath "$1")
gang=$(realpath "$2")
shift 2
cd "$(dirname "$0")/.."
XDG_RUNTIME_DIR="/run/user/$(id -u)"
export XDG_RUNTIME_DIR
unit=gang-e2e-$$
home=$(mktemp -d)
report=$(mktemp)
# An interrupted run stops its service before its HOME is removed.
trap 'systemctl --user stop "$unit.service" 2>/dev/null || true; chmod -R u+w "$home" && rm -rf "$home" "$report"' EXIT
status=0
start=$SECONDS
systemd-run --user --unit="$unit" --collect --pipe --wait --same-dir --slice=gang.slice -p Type=oneshot -p Delegate=yes -p TimeoutStartSec="$budget" -p TimeoutStopSec=10 \
  --setenv=PATH="$PATH" --setenv=GANG_E2E_GLIBC_TOOL="$bundle/loaded_probe" -- \
  nix develop -c env -u XDG_CONFIG_HOME -u XDG_CACHE_HOME HOME="$home" \
  "$bundle/local" --gang "$gang" --bazel "$bundle/bazel" \
  --config configs/gang.local.toml --sample e2e/sample --configs configs "$@" | tee "$report" || status=$?
if (( status != 0 )); then
  if (( SECONDS - start >= budget )); then
    echo "tools/e2e-gate.sh: the gate exceeded its $budget s budget" >&2
    exit 1
  fi
  exit "$status"
fi
[[ $(tail -n 1 "$report") =~ ^[1-9][0-9]*\ checks,\ 0\ failed$ ]] || {
  echo "tools/e2e-gate.sh: the harness ended without a passing report" >&2
  exit 1
}
