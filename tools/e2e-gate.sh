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
# Gate 2 additions took 4 min 34 s unsplit on CI; on a four-CPU host, shard 1 took
# 3 min 8.734 s and shard 2 took 5 min 13.144 s with failures. Each shard is one run under the
# 480 s budget. Systemd enforces it as TimeoutStartSec because a oneshot service's run is its
# start, and TimeoutStopSec bounds the stop to 10 seconds.
budget=480
bundle=$(realpath "$1")
gang=$(realpath "$2")
shift 2
cd "$(dirname "$0")/.."
bundle_file() {
  local name
  name=$(jq -er --arg key "$1" \
    'getpath($key | split(".")) | select(if type == "string" then test("^[A-Za-z0-9][A-Za-z0-9._-]*$") else false end)' \
    "$bundle/upgrade-inputs.json") || return 1
  test -f "$bundle/$name" || return 1
  printf '%s/%s\n' "$bundle" "$name"
}
previous_gang=$(bundle_file previous.binary)
previous_fixture=$(bundle_file previous.fixture)
historical_gang=$(bundle_file historical.binary)
historical_fixture=$(bundle_file historical.fixture)
calibration_gang=$(bundle_file calibration.binary)
calibration_fixture=$(bundle_file calibration.fixture)
candidate_fixture=$(bundle_file candidate_fixture)
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
  --setenv=PATH="$PATH" --setenv=GANG_E2E_GLIBC_TOOL="$bundle/loaded_probe" \
  --setenv=GANG_E2E_PREVIOUS_GANG="$previous_gang" --setenv=GANG_E2E_PREVIOUS_FIXTURE="$previous_fixture" \
  --setenv=GANG_E2E_HISTORICAL_GANG="$historical_gang" --setenv=GANG_E2E_HISTORICAL_FIXTURE="$historical_fixture" \
  --setenv=GANG_E2E_CALIBRATION_GANG="$calibration_gang" --setenv=GANG_E2E_CALIBRATION_FIXTURE="$calibration_fixture" \
  --setenv=GANG_E2E_CANDIDATE_FIXTURE="$candidate_fixture" -- \
  nix develop -c env -u XDG_CONFIG_HOME -u XDG_CACHE_HOME HOME="$home" \
  "$bundle/local" --gang "$gang" --bazel "$bundle/bazel" --postgres "$bundle/postgresql.tar.gz" \
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
