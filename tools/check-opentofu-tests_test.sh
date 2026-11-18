#!/usr/bin/env bash
# Exercise the actual shell guard with a fake Bazel; shell and coreutils come from the pinned test PATH.
set -euo pipefail

case "$GUARD_SCRIPT" in
  /*) guard=$GUARD_SCRIPT ;;
  *) guard="$PWD/$GUARD_SCRIPT" ;;
esac

run_case() {
  local name=$1 coverage=$2 expected=$3 aliases=$4
  local dir="$TEST_TMPDIR/$name" command_path status
  mkdir -p "$dir/tools" "$dir/infra/example/tests" "$dir/bin"
  cp "$guard" "$dir/tools/check-opentofu-tests.sh"
  chmod +x "$dir/tools/check-opentofu-tests.sh"
  : >"$dir/infra/example/tests/nested.tftest.hcl"
  : >"$dir/infra/example/root.tftest.hcl"
  : >"$dir/invocations"
  : >"$dir/calls"
  # Restrict the guard to the pinned test tools, with no host Bazel on PATH.
  for command_path in bash dirname find; do
    ln -s "$(command -v "$command_path")" "$dir/bin/$command_path"
  done
  cat >"$dir/bin/chosen-bazel" <<'BAZEL'
#!/usr/bin/env bash
set -euo pipefail
[[ $# == 3 && $1 == query && $3 == --output=label ]] || exit 90
printf '%s\n' "${0##*/}" >>"$FAKE_INVOCATIONS"
case "$2" in
  "kind('_opentofu_contract_test rule', attr(test_file, '^tests/nested\\.tftest\\.hcl$', //infra/example:*))")
    printf '%s\n' nested >>"$FAKE_CALLS"
    printf '%s\n' '//infra/example:nested_contract' ;;
  "kind('_opentofu_contract_test rule', attr(test_file, '^root\\.tftest\\.hcl$', //infra/example:*))")
    printf '%s\n' root >>"$FAKE_CALLS"
    if [[ $FAKE_COVERAGE == both ]]; then printf '%s\n' '//infra/example:root_contract'; fi ;;
  *) printf 'unexpected Bazel query: %s\n' "$2" >&2; exit 91 ;;
esac
BAZEL
  chmod +x "$dir/bin/chosen-bazel"
  if [[ $aliases == yes ]]; then
    ln -s chosen-bazel "$dir/bin/bazelisk"
  fi
  if PATH="$dir/bin" BAZEL="$dir/bin/chosen-bazel" FAKE_CALLS="$dir/calls" FAKE_INVOCATIONS="$dir/invocations" FAKE_COVERAGE="$coverage" \
    "$dir/tools/check-opentofu-tests.sh" >"$dir/out" 2>"$dir/err"; then
    status=0
  else
    status=$?
  fi
  if [[ $expected == success && $status != 0 ]]; then
    printf '%s: guard unexpectedly failed (%s)\n' "$name" "$status" >&2
    cat "$dir/err" >&2
    exit 1
  fi
  if [[ $expected == failure && $status == 0 ]]; then
    printf '%s: uncovered root test was accepted\n' "$name" >&2
    exit 1
  fi
  local nested=0 root=0 call
  while IFS= read -r call; do
    case "$call" in
      nested) nested=$((nested + 1)) ;;
      root) root=$((root + 1)) ;;
      *) printf '%s: unexpected call record\n' "$name" >&2; exit 1 ;;
    esac
  done <"$dir/calls"
  if [[ $expected == success && ( $nested != 1 || $root != 1 ) ]]; then
    printf '%s: expected both files queried once, got nested=%s root=%s\n' "$name" "$nested" "$root" >&2
    exit 1
  fi
  if [[ $expected == failure && $root != 1 ]]; then
    printf '%s: failure did not query the uncovered root file\n' "$name" >&2
    exit 1
  fi
  if [[ $name == override ]]; then
    while IFS= read -r call; do
      [[ $call == chosen-bazel ]] || { printf 'override: wrong Bazel command: %s\n' "$call" >&2; exit 1; }
    done <"$dir/invocations"
  fi
}

case "${GUARD_CASE:-all}" in
  override) run_case override both success no ;;
  root) run_case uncovered nested failure yes; run_case covered both success yes ;;
  all) run_case override both success no; run_case uncovered nested failure yes; run_case covered both success yes ;;
  *) printf 'unknown GUARD_CASE: %s\n' "$GUARD_CASE" >&2; exit 2 ;;
esac
