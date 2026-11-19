#!/usr/bin/env bash
# Execute the action's Bash run block and its generated proof with fake runner tools.
set -euo pipefail

action="$PWD/$LOGIN_ACTION"
fixtures="$TEST_TMPDIR/fixtures"
mkdir -p "$fixtures" "$TEST_TMPDIR/runner"
cat >"$fixtures/gang" <<'GANG'
#!/usr/bin/env bash
exit 0
GANG
cat >"$fixtures/curl" <<'CURL'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" >"$CURL_ARGS"
printf '{"value":"fresh-proof"}'
CURL
cat >"$fixtures/jq" <<'JQ'
#!/usr/bin/env bash
printf 'fresh-proof\n'
JQ
chmod +x "$fixtures/gang" "$fixtures/curl" "$fixtures/jq"

# Extract the composite step's run block without evaluating its GitHub expressions.
run_block="$TEST_TMPDIR/login-step.sh"
found=false
while IFS= read -r line; do
  if [[ "$found" == false ]]; then
    [[ "$line" == '      run: |' ]] && found=true
  elif [[ "$line" == '        '* ]]; then
    printf '%s\n' "${line#        }" >>"$run_block"
  elif [[ -n "$line" ]]; then
    break
  else
    printf '\n' >>"$run_block"
  fi
done <"$action"
[[ "$found" == true && -s "$run_block" ]]

audience="gang audience '\$(not-a-command)"
PATH="$fixtures:$PATH" RUNNER_TEMP="$TEST_TMPDIR/runner" \
  ACTIONS_ID_TOKEN_REQUEST_URL='https://example.com/token' \
  ACTIONS_ID_TOKEN_REQUEST_TOKEN='runner-secret' \
  AUDIENCE="$audience" SERVER='https://example.com' PROVIDER=github-actions \
  GANG="$fixtures/gang" CA_PIN='' bash "$run_block"

if grep -Fq 'runner-secret' "$TEST_TMPDIR/runner/gang/job-token.sh" || \
  grep -Fq 'https://example.com/token' "$TEST_TMPDIR/runner/gang/job-token.sh"; then
  echo 'FAIL: runner credentials were written into the proof command' >&2
  exit 1
fi

if ! proof=$(env -u AUDIENCE PATH="$fixtures:$PATH" CURL_ARGS="$TEST_TMPDIR/curl-args" \
  ACTIONS_ID_TOKEN_REQUEST_URL='https://example.com/token' \
  ACTIONS_ID_TOKEN_REQUEST_TOKEN='runtime-secret' \
  "$TEST_TMPDIR/runner/gang/job-token.sh"); then
  echo 'FAIL: proof command failed without AUDIENCE' >&2
  exit 1
fi
if [[ "$proof" != fresh-proof ]] || ! grep -Fxq "audience=$audience" "$TEST_TMPDIR/curl-args" || \
  ! grep -Fxq 'Authorization: bearer runtime-secret' "$TEST_TMPDIR/curl-args" || \
  ! grep -Fxq 'https://example.com/token' "$TEST_TMPDIR/curl-args"; then
  echo 'FAIL: proof command did not request the configured audience with runtime credentials' >&2
  exit 1
fi
echo 'PASS: proof command refreshes without AUDIENCE'
