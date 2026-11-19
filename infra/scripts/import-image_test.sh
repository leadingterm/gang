#!/usr/bin/env bash
# Exercise the real import shell boundary with fake AWS subprocesses. Bash and coreutils
# come from the pinned test PATH; jq and zstd are declared runfiles from the pinned devShell repo.
set -euo pipefail

case "$IMPORT_IMAGE" in
  /*) script="$IMPORT_IMAGE" ;;
  *) script="$PWD/$IMPORT_IMAGE" ;;
esac
case "$IMPORT_JQ" in
  /*) jq_path="$IMPORT_JQ" ;;
  *) jq_path="$PWD/$IMPORT_JQ" ;;
esac
tool_dir=$(dirname -- "$jq_path")
fixtures="$TEST_TMPDIR/fixtures"
mkdir -p "$fixtures"
PATH="$fixtures:$tool_dir:$PATH"
export PATH

cat >"$fixtures/coldsnap" <<'COLDSNAP'
#!/usr/bin/env bash
set -euo pipefail
printf 'called\n' >>"$CASE_DIR/coldsnap-attempts"
if [[ $# -ne 9 || $1 != upload || $2 != --wait || $3 != --no-progress ||
  $4 != --omit-zero-blocks || $5 != --description || $6 != gang-test-import ||
  $7 != --tag || $8 != 'Key=ec2:SharedTag/GangImportImage,Value=gang-test-import' ]]; then
  printf 'unexpected coldsnap arguments\n' >&2
  exit 99
fi
if ! cmp -s -- "$9" "$CASE_DIR/image.raw"; then
  printf 'coldsnap received incorrect raw image\n' >&2
  exit 99
fi
printf '%s\n' gang-test-import >"$CASE_DIR/snap-owned"
printf '%s\n' snap-owned
COLDSNAP

cat >"$fixtures/aws" <<'AWS'
#!/usr/bin/env bash
set -euo pipefail
fail() {
  printf 'unexpected aws invocation: %s\n' "$*" >&2
  exit 99
}
[[ $# -ge 2 && $1 == ec2 ]] || fail "$@"
operation=$2
shift 2
case "$operation" in
  describe-images)
    [[ $# -eq 8 && $1 == --owners && $2 == self && $3 == --include-deprecated &&
      $4 == --include-disabled && $5 == --filters &&
      $6 == Name=name,Values=gang-test-import && $7 == --output && $8 == json ]] || fail 'describe-images arguments' "$@"
    case "$CASE_MODE" in
      adopt|adopt_pending|wrong_tag|wrong_sha|wrong_arch_tag|wrong_arch|wrong_boot|wrong_state|wrong_name|duplicate|duplicate_tag|missing_tag)
        jq -cn --arg sha "$IMAGE_SHA" --arg mode "$CASE_MODE" '
          {Images:[{ImageId:"ami-existing",Name:"gang-test-import",Architecture:"x86_64",
            BootMode:"uefi",State:"available",Tags:[
              {Key:"ec2:SharedTag/ImageSha256",Value:$sha},
              {Key:"ec2:SharedTag/GangRelease",Value:"v1.2.3"},
              {Key:"ec2:SharedTag/GangImage",Value:"gang-image-x86_64"}]}]} |
          if $mode == "adopt_pending" then .Images[0].State = "pending"
          elif $mode == "wrong_tag" then .Images[0].Tags[1].Value = "v1.2.4"
          elif $mode == "wrong_sha" then .Images[0].Tags[0].Value = ($sha[0:12] + "different")
          elif $mode == "wrong_arch_tag" then .Images[0].Tags[2].Value = "gang-image-aarch64"
          elif $mode == "wrong_arch" then .Images[0].Architecture = "arm64"
          elif $mode == "wrong_boot" then .Images[0].BootMode = "legacy-bios"
          elif $mode == "wrong_state" then .Images[0].State = "failed"
          elif $mode == "wrong_name" then .Images[0].Name = "other"
          elif $mode == "duplicate" then .Images += [.Images[0]]
          elif $mode == "duplicate_tag" then .Images[0].Tags += [.Images[0].Tags[0]]
          elif $mode == "missing_tag" then .Images[0].Tags |= map(select(.Key != "ec2:SharedTag/GangRelease"))
          else . end' ;;
      *) printf '{"Images":[]}\n' ;;
    esac ;;
  register-image)
    printf 'called\n' >>"$CASE_DIR/register-attempts"
    [[ ${AWS_MAX_ATTEMPTS:-} == 1 ]] || fail 'register-image must use one attempt'
    [[ $# -eq 25 ]] || fail 'register-image argument count' "$@"
    [[ $1 == --architecture && $2 == x86_64 && $3 == --boot-mode && $4 == uefi &&
      $5 == --ena-support && $6 == --sriov-net-support && $7 == simple &&
      $8 == --virtualization-type && $9 == hvm && ${10} == --imds-support &&
      ${11} == v2.0 && ${12} == --root-device-name && ${13} == /dev/xvda &&
      ${14} == --block-device-mappings &&
      ${15} == 'DeviceName=/dev/xvda,Ebs={SnapshotId=snap-owned,VolumeType=gp3,DeleteOnTermination=true}' &&
      ${16} == --name && ${17} == gang-test-import && ${18} == --description &&
      ${19} == 'test import description' && ${20} == --tag-specifications &&
      ${22} == --query && ${23} == ImageId && ${24} == --output && ${25} == text ]] || fail 'register-image arguments' "$@"
    jq -e --arg sha "$IMAGE_SHA" '. == [{ResourceType:"image",Tags:[{Key:"ec2:SharedTag/ImageSha256",Value:$sha},{Key:"ec2:SharedTag/GangRelease",Value:"v1.2.3"},{Key:"ec2:SharedTag/GangImage",Value:"gang-image-x86_64"}]}]' <<<"${21}" >/dev/null || fail 'register-image tags'
    [[ -f $CASE_DIR/snap-owned && $(<"$CASE_DIR/snap-owned") == gang-test-import ]] || fail 'snapshot not owned by import'
    case "$CASE_MODE" in
      success|committedtimeout)
        printf '%s\n' snap-owned >"$CASE_DIR/ami-owned"
        if [[ $CASE_MODE == success ]]; then
          printf '%s\n' ami-owned
          exit 0
        fi
        printf 'connection timed out after commit\n' >&2
        exit 255 ;;
      rejection|cleanupfailure)
        printf 'An error occurred (InvalidParameterValue) when calling the RegisterImage operation: rejected\n' >&2
        exit 254 ;;
      duplicate_rejection)
        printf 'An error occurred (InvalidAMIName.Duplicate) when calling the RegisterImage operation (reached max retries: 0): already exists\n' >&2
        exit 254 ;;
      localreject) printf 'local parameter error\n' >&2; exit 252 ;;
      credentials) printf 'credentials error\n' >&2; exit 253 ;;
      internalerror)
        printf 'An error occurred (InternalError) when calling the RegisterImage operation: uncertain\n' >&2
        exit 254 ;;
      unknown)
        printf 'unrecognized registration error\n' >&2
        exit 254 ;;
      *) fail 'unknown case mode' "$CASE_MODE" ;;
    esac ;;
  delete-snapshot)
    [[ $# -eq 2 && $1 == --snapshot-id && $2 == snap-owned ]] || fail 'delete-snapshot arguments' "$@"
    printf '%s\n' "$2" >>"$CASE_DIR/delete-attempts"
    [[ -f $CASE_DIR/snap-owned && $(<"$CASE_DIR/snap-owned") == gang-test-import ]] || fail 'snapshot not owned by import'
    if [[ -f $CASE_DIR/ami-owned && $(<"$CASE_DIR/ami-owned") == snap-owned ]]; then
      printf 'snapshot is referenced by an image\n' >&2
      exit 1
    fi
    if [[ $CASE_MODE == cleanupfailure ]]; then
      printf 'injected snapshot deletion failure\n' >&2
      exit 1
    fi
    rm -- "$CASE_DIR/snap-owned" ;;
  *) fail 'unknown operation' "$operation" ;;
esac
AWS
chmod +x "$fixtures/coldsnap" "$fixtures/aws"

failures=0
cases=0
fail() {
  printf 'FAIL: %s: %s\n' "$case_name" "$*" >&2
  failures=$((failures + 1))
}

run_case() {
  local case_name="$1" expected_status="$2" expected_snapshot="$3" expected_image="$4" diagnostic="$5" expected_deletes="$6"
  local case_dir="$TEST_TMPDIR/$case_name" image_sha status output stderr delete_attempts
  cases=$((cases + 1))
  mkdir -p "$case_dir/tmp"
  : >"$case_dir/delete-attempts"
  printf 'raw test image\n' >"$case_dir/image.raw"
  zstd -q -f -o "$case_dir/image.raw.zst" -- "$case_dir/image.raw"
  image_sha=$(sha256sum <"$case_dir/image.raw.zst")
  image_sha=${image_sha%% *}
  printf '{"boot_mode":"uefi"}\n' >"$case_dir/info.json"
  printf '%s\n' unrelated-owner >"$case_dir/snap-unrelated"
  if CASE_DIR="$case_dir" CASE_MODE="$case_name" IMAGE_SHA="$image_sha" TMPDIR="$case_dir/tmp" \
    "$script" "$case_dir/image.raw.zst" "$case_dir/info.json" gang-test-import x86_64 \
    "$image_sha" v1.2.3 'test import description' >"$case_dir/out" 2>"$case_dir/err"; then
    status=0
  else
    status=$?
  fi
  output=$(<"$case_dir/out")
  stderr=$(<"$case_dir/err")
  delete_attempts=$(wc -l <"$case_dir/delete-attempts")
  if [[ $delete_attempts -ne $expected_deletes ]]; then
    fail "expected $expected_deletes snapshot deletion attempts, got $delete_attempts"
  fi
  if [[ $status -ne $expected_status ]]; then
    fail "expected status $expected_status, got $status; stderr: $stderr"
  fi
  if [[ $case_name == success && $output != ami-owned ]]; then
    fail "expected ami-owned output, got: $output"
  fi
  if [[ $case_name == adopt || $case_name == adopt_pending ]]; then
    if [[ $output != ami-existing ]]; then
      fail "expected adopted image ID, got: $output"
    fi
  fi
  case "$case_name" in
    adopt|adopt_pending|wrong_tag|wrong_sha|wrong_arch_tag|wrong_arch|wrong_boot|wrong_state|wrong_name|duplicate|duplicate_tag|missing_tag)
      if [[ -e $case_dir/coldsnap-attempts || -e $case_dir/register-attempts ]]; then
        fail 'existing image must not trigger upload or registration'
      fi ;;
  esac
  if [[ $expected_snapshot == present ]]; then
    if [[ ! -f $case_dir/snap-owned || $(<"$case_dir/snap-owned") != gang-test-import ]]; then
      fail 'owned snapshot missing or has incorrect owner'
    fi
  elif [[ -e $case_dir/snap-owned ]]; then
    fail 'owned snapshot should have been deleted'
  fi
  if [[ $expected_image == present ]]; then
    if [[ ! -f $case_dir/ami-owned || $(<"$case_dir/ami-owned") != snap-owned ]]; then
      fail 'owned image missing or does not reference owned snapshot'
    fi
  elif [[ -e $case_dir/ami-owned ]]; then
    fail 'unexpected owned image'
  fi
  if [[ ! -f $case_dir/snap-unrelated || $(<"$case_dir/snap-unrelated") != unrelated-owner ]]; then
    fail 'unrelated snapshot was not retained unchanged'
  fi
  if [[ $diagnostic == required && ( ${stderr##*$'\n'} != *snap-owned* || ${stderr##*$'\n'} != *gang-test-import* ) ]]; then
    fail "final diagnostic must identify the owned snapshot and import; stderr: $stderr"
  fi
}

run_case success 0 present present optional 0
run_case rejection 254 absent absent optional 1
run_case duplicate_rejection 254 absent absent optional 1
run_case localreject 252 absent absent optional 1
run_case credentials 253 absent absent optional 1
run_case cleanupfailure 254 present absent required 1
run_case committedtimeout 255 present present required 0
run_case internalerror 254 present absent required 0
run_case unknown 254 present absent required 0
run_case adopt 0 absent absent optional 0
run_case adopt_pending 0 absent absent optional 0
run_case wrong_tag 1 absent absent optional 0
run_case wrong_sha 1 absent absent optional 0
run_case wrong_arch_tag 1 absent absent optional 0
run_case wrong_arch 1 absent absent optional 0
run_case wrong_boot 1 absent absent optional 0
run_case wrong_state 1 absent absent optional 0
run_case wrong_name 1 absent absent optional 0
run_case duplicate 1 absent absent optional 0
run_case duplicate_tag 1 absent absent optional 0
run_case missing_tag 1 absent absent optional 0

printf 'import-image: %d cases, %d assertion failures\n' "$cases" "$failures"
exit "$((failures > 0))"
