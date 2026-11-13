#!/usr/bin/env bash
# Behaviour of install-gang.sh against a fake `gh` that serves one release asset and a fake
# `uname` that names the architecture; bash and coreutils are the test PATH the dev shell pins.
set -euo pipefail

script="$PWD/$INSTALL_GANG"
fixtures="$TEST_TMPDIR/fixtures"
mkdir -p "$fixtures"

cat >"$fixtures/gh" <<'GH'
#!/usr/bin/env bash
# gh release download <tag> --repo <repository> --pattern <asset> --dir <dir>
set -euo pipefail
printf '%s\n' "$*" >>"$GH_CALLS"
mkdir -p "$9"
printf 'gang-binary\n' >"$9/$7"
GH
cat >"$fixtures/uname" <<'UNAME'
#!/usr/bin/env bash
printf '%s\n' "$FAKE_ARCH"
UNAME
chmod +x "$fixtures/gh" "$fixtures/uname"

# SHA-256 of the bytes the fake gh serves (`gang-binary\n`).
asset_sha256=dee1ef82bfb32e6718086f86d75cdcebd18d2c460cf36b9426f4d5193d79fe14
ref=example-org/gang/.github/workflows/deploy.yml@refs/tags/v1.2.3
failures=0

# run <name> <workflow ref> <pin>: runs the script in a fresh runner temp directory.
run() {
  case_dir="$TEST_TMPDIR/$1"
  mkdir -p "$case_dir/runner"
  : >"$case_dir/env"
  : >"$case_dir/gh-calls"
  PATH="$fixtures:$PATH" GH_CALLS="$case_dir/gh-calls" FAKE_ARCH=aarch64 \
    GANG_WORKFLOW_REF="$2" GANG_REPOSITORY=example-org/gang GANG_SHA256="$3" \
    RUNNER_TEMP="$case_dir/runner" GITHUB_ENV="$case_dir/env" "$script" >"$case_dir/out" 2>&1
}

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

if ! run pinned "$ref" "$asset_sha256"; then
  fail "pinned: the script failed: $(cat "$TEST_TMPDIR/pinned/out")"
fi
if [[ "$(cat "$TEST_TMPDIR/pinned/gh-calls")" != "release download v1.2.3 --repo example-org/gang --pattern gang-aarch64-unknown-linux-musl --dir $TEST_TMPDIR/pinned/runner/gang-bin" ]]; then
  fail "pinned: unexpected gh call: $(cat "$TEST_TMPDIR/pinned/gh-calls")"
fi
if [[ "$(cat "$TEST_TMPDIR/pinned/env")" != "GANG=$TEST_TMPDIR/pinned/runner/gang-bin/gang" ]]; then
  fail "pinned: GANG not exported: $(cat "$TEST_TMPDIR/pinned/env")"
fi
if [[ ! -x "$TEST_TMPDIR/pinned/runner/gang-bin/gang" || "$(cat "$TEST_TMPDIR/pinned/runner/gang-bin/gang")" != gang-binary ]]; then
  fail "pinned: the installed gang is not the executable release asset"
fi

if run mismatch "$ref" "${asset_sha256//8/9}"; then
  fail "mismatch: a binary that differs from the pin was accepted"
fi
if [[ -s "$TEST_TMPDIR/mismatch/env" ]]; then
  fail "mismatch: GANG exported for a binary that differs from the pin"
fi

if run unpinned "$ref" ""; then
  fail "unpinned: a binary was accepted without a pin"
fi
if [[ -s "$TEST_TMPDIR/unpinned/env" ]]; then
  fail "unpinned: GANG exported without a pin"
fi

if run branch example-org/gang/.github/workflows/deploy.yml@refs/heads/main "$asset_sha256"; then
  fail "branch: a workflow not pinned to a tag installed gang"
fi
if [[ -s "$TEST_TMPDIR/branch/gh-calls" ]]; then
  fail "branch: downloaded although the workflow is not pinned to a tag"
fi

exit "$((failures > 0))"
