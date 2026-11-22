#!/usr/bin/env bash
# Exercise cache eligibility, restore isolation, and Darwin source-copy safety.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gang-ci-cache-policy.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
scratch=$(cd "$scratch" && pwd -P)
cases=0
failures=0

translate() {
  local expr=$1
  local format="format('refs/heads/{0}', github.event.repository.default_branch)"
  if [[ $expr != "\${{"*'}}' ]]; then
    printf 'Expected a GitHub expression: %s\n' "$expr" >&2
    return 1
  fi
  expr=${expr#"\${{"}
  expr=${expr%'}}'}
  expr=${expr//"$format"/"\"\$DEFAULT_REF\""}
  expr=${expr//github.event.pull_request.head.repo.full_name/"\"\$HEAD_REPO\""}
  expr=${expr//github.event_name/"\"\$EVENT\""}
  expr=${expr//github.ref/"\"\$REF\""}
  expr=${expr//github.repository/"\"\$REPO\""}
  expr=${expr//github.job/"\"\$JOB\""}
  expr=${expr//runner.arch/"\"\$ARCH\""}
  expr=${expr//env.BAZEL_CACHE_JOB/"\"\$BAZEL_CACHE_JOB\""}
  expr=${expr//env.BAZEL_CURRENT_CACHE_HIT/"\"\$CURRENT_CACHE_HIT\""}
  expr=${expr//env.BAZEL_DISK_CACHE_SAVE/"\"\$SAVE\""}
  expr=${expr//runner.os/"\"\$OS\""}
  expr=${expr//steps.restore.outputs.cache-hit/"\"\$CACHE_HIT\""}
  expr=${expr//steps.pr.outputs.cache-hit/"\"\$PR_CACHE_HIT\""}
  expr=${expr//steps.validate.outcome/"\"\$VALIDATE_OUTCOME\""}
  expr=${expr//inputs.restore-only/"\"\$RESTORE_ONLY\""}
  expr=${expr//inputs.cache-save/"\"\$CACHE_SAVE\""}
  expr=${expr//inputs.mode/"\"\$MODE\""}
  expr=${expr//inputs.shell/"\"\$CACHE_SHELL\""}
  expr=${expr//inputs.prune/"\"\$PRUNE\""}
  expr=${expr//"always()"/true}
  if [[ $expr == *github.* || $expr == *inputs.* || $expr == *steps.* || $expr == *runner.* || $expr == *env.* || $expr == *'format('* ]]; then
    printf 'Unsupported GitHub expression: %s\n' "$expr" >&2
    return 1
  fi
  expr=${expr//\(/ ( }
  expr=${expr//\)/ ) }
  printf '%s' "$expr"
}

# The push promotion job is the only workflow that writes a queue-derived cache.
promotion="$script_dir/../.github/workflows/cache-promotion.yml"
yq -o=json '.' "$promotion" | jq -e '
  .on.push.branches == ["main"] and
  .concurrency.group == "cache-promotion-${{ github.ref }}" and
  .concurrency["cancel-in-progress"] == true and
  .permissions.contents == "read" and .permissions.actions == "read" and
  ([.jobs.promote.steps[] | select(.uses == "./.github/actions/setup-nix") |
    .with["cache-save"]] == ["true"]) and
  ([.jobs.promote.steps[] | select(.uses == "./.github/actions/setup-bazel") |
    .with["cache-job"]] == ["linux-x86_64"]) and
  ([.jobs.promote.steps[] | select(.name == "Select qualified queue run") |
    .run | contains("--require-working-sets")] == [true]) and
  ([.jobs.promote.steps[] | .run // "" | select(test("bazel (build|test)"))] | length == 0)
' >/dev/null

# yq parses the actual action YAML rather than matching source formatting.
bazel=$(yq -er '.runs.steps[] | select(.name == "Mark the start of the build") | .env.SAVE' "$script_dir/../.github/actions/setup-bazel/action.yml")
nix=$(yq -o=json '.runs.steps' "$script_dir/../.github/actions/setup-nix/action.yml" | jq -er '.[] | select((.uses // "") | startswith("nix-community/cache-nix-action@")) | .with.save')
bazel=$(translate "$bazel")
nix=$(translate "$nix")
current_hit=$(yq -er '.runs.steps[] | select(.name == "Mark the start of the build") | .env.CURRENT_CACHE_HIT' "$script_dir/../.github/actions/setup-bazel/action.yml")
current_hit=$(translate "$current_hit")
mark_body="$scratch/mark-start.sh"
yq -er '.runs.steps[] | select(.name == "Mark the start of the build") | .run' "$script_dir/../.github/actions/setup-bazel/action.yml" >"$mark_body"
bazel_base=$(yq -er '.runs.steps[] | select(.id == "base") | .if' "$script_dir/../.github/actions/setup-bazel/action.yml")
bazel_pr=$(yq -er '.runs.steps[] | select(.id == "pr") | .if' "$script_dir/../.github/actions/setup-bazel/action.yml")
bazel_restore=$(yq -er '.runs.steps[] | select(.id == "restore") | .if' "$script_dir/../.github/actions/setup-bazel/action.yml")
# Step conditions omit the optional GitHub expression delimiters.
bazel_base=$(translate "\${{ $bazel_base }}")
bazel_pr=$(translate "\${{ $bazel_pr }}")
bazel_restore=$(translate "\${{ $bazel_restore }}")
nix_restore=$(yq -o=json '.runs.steps' "$script_dir/../.github/actions/setup-nix/action.yml" | jq -er '.[] | select((.uses // "") | startswith("nix-community/cache-nix-action@")) | .if')
nix_restore=$(translate "\${{ $nix_restore }}")
bazel_export=$(yq -er '.runs.steps[] | select(.name == "Export shard working set") | .if' "$script_dir/../.github/actions/save-bazel-cache/action.yml")
bazel_upload=$(yq -er '.runs.steps[] | select(.name == "Upload shard working set") | .if' "$script_dir/../.github/actions/save-bazel-cache/action.yml")
bazel_shutdown=$(yq -er '.runs.steps[] | select(.id == "shutdown") | .if' "$script_dir/../.github/actions/save-bazel-cache/action.yml")
bazel_export=$(translate "\${{ $bazel_export }}")
bazel_upload=$(translate "\${{ $bazel_upload }}")
bazel_shutdown=$(translate "\${{ $bazel_shutdown }}")

check() {
  local description=$1 expr=$2 expected=$3 actual
  cases=$((cases + 1))
  # Only the translated expression is shell code; fixture values stay quoted environment expansions.
  if ! actual=$(bash --noprofile --norc -c "if [[ $expr ]]; then printf true; else printf false; fi"); then
    printf 'FAIL: %s (expression evaluation failed)\n' "$description" >&2
    failures=$((failures + 1))
  elif [[ $actual != "$expected" ]]; then
    printf 'FAIL: %s (expected %s, got %s)\n' "$description" "$expected" "$actual" >&2
    failures=$((failures + 1))
  fi
}

check_current_hit() {
  local expected=$1 actual output name value emitted=missing
  cases=$((cases + 1))
  if ! actual=$(bash --noprofile --norc -c "if [[ $current_hit ]]; then printf true; else printf false; fi"); then
    printf 'FAIL: current-cache-hit expression evaluation failed\n' >&2
    failures=$((failures + 1))
    return
  fi
  if [[ $actual != "$expected" ]]; then
    printf 'FAIL: current-cache-hit restore=%s PR=%s (expected %s, got %s)\n' "$CACHE_HIT" "$PR_CACHE_HIT" "$expected" "$actual" >&2
    failures=$((failures + 1))
  fi
  : >"$scratch/hit.env"
  # Run the actual producer body; all touched paths belong to this fixture.
  if ! output=$(env CURRENT_CACHE_HIT="$actual" SAVE=false \
    GITHUB_ENV="$scratch/hit.env" GITHUB_STEP_SUMMARY="$scratch/hit.summary" \
    BAZEL_DISK_CACHE_EPOCH="$scratch/hit.epoch" \
    bash --noprofile --norc -e -u -o pipefail "$mark_body" 2>&1); then
    printf 'FAIL: current-cache-hit producer body: %s\n' "$output" >&2
    failures=$((failures + 1))
    return
  fi
  while IFS='=' read -r name value; do
    if [[ $name == BAZEL_CURRENT_CACHE_HIT ]]; then emitted=$value; fi
  done <"$scratch/hit.env"
  if [[ $emitted != "$expected" ]]; then
    printf 'FAIL: current-cache-hit env mapping (expected %s, got %s)\n' "$expected" "$emitted" >&2
    failures=$((failures + 1))
  fi
}


check_darwin_guard() {
  local kind=$1 fixture checkout home target source expected spy output status dir command
  local failed=false
  cases=$((cases + 1))
  fixture="$scratch/darwin-$kind"
  checkout="$fixture/checkout"
  home="$fixture/cargo-home"
  target="$fixture/cargo-target"
  expected=$checkout
  case "$kind" in
    root) source=/ ;;
    checkout-child) source="$checkout/source" ;;
    checkout-parent) source=$fixture ;;
    home-child) source="$home/source"; expected=$home ;;
    home-parent) source="$fixture/source"; home="$source/home"; expected=$home ;;
    target-child) source="$target/source"; expected=$target ;;
    target-parent) source="$fixture/source"; target="$source/target"; expected=$target ;;
  esac
  mkdir -p "$checkout" "$home" "$target" "$fixture/source" "$fixture/bin"
  for dir in "$checkout" "$home" "$target" "$fixture/source"; do
    printf '%s' unchanged >"$dir/sentinel"
  done
  spy="$fixture/commands-called"
  # Run the real guard with devShell-pinned Bash; PATH spies prevent copying or compiling on a regression.
  for command in rsync cargo; do
    cat >"$fixture/bin/$command" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "${0##*/}" >>"$DARWIN_GUARD_SPY"
exit 97
SH
    chmod +x "$fixture/bin/$command"
  done
  if output=$(
    cd "$checkout"
    export PATH="$fixture/bin:$PATH"
    export DARWIN_GUARD_SPY=$spy DARWIN_SOURCE_DIR=$source CARGO_HOME=$home CARGO_TARGET_DIR=$target
    bash --noprofile --norc "$script_dir/darwin-check.sh" 2>&1
  ); then
    status=0
  else
    status=$?
  fi
  if [[ $status != 1 || $output != "Darwin source directory must not overlap the checkout or Cargo cache: $expected" ]]; then
    printf 'FAIL: Darwin %s did not reject the overlap with its guard diagnostic (status %s): %s\n' "$kind" "$status" "$output" >&2
    failed=true
  fi
  if [[ -e $spy ]]; then
    printf 'FAIL: Darwin %s invoked a copy/build command before rejecting the overlap\n' "$kind" >&2
    failed=true
  fi
  for dir in "$checkout" "$home" "$target" "$fixture/source"; do
    if [[ ! -f $dir/sentinel ]] || [[ $(<"$dir/sentinel") != unchanged ]]; then
      printf 'FAIL: Darwin %s changed the sentinel in %s\n' "$kind" "$dir" >&2
      failed=true
    fi
  done
  if [[ $failed == true ]]; then failures=$((failures + 1)); fi
}

export EVENT=pull_request REF=refs/pull/1/merge DEFAULT_REF=refs/heads/main
export REPO=example-org/gang HEAD_REPO=example-org/gang OS=Linux ARCH=X64
export JOB=cache-x86_64 BAZEL_CACHE_JOB=linux-x86_64
export CACHE_HIT=false PR_CACHE_HIT=false RESTORE_ONLY=false CACHE_SAVE=true CACHE_SHELL=

check 'Bazel same-repository PR saves' "$bazel" true
check 'Nix same-repository PR saves' "$nix" true
HEAD_REPO=example-fork/gang
check 'Bazel fork PR does not save' "$bazel" false
check 'Nix fork PR does not save' "$nix" false
HEAD_REPO=
check 'Bazel missing head repository does not save' "$bazel" false
check 'Nix missing head repository does not save' "$nix" false
HEAD_REPO=$REPO EVENT=push REF=$DEFAULT_REF
check 'Bazel default-branch push saves' "$bazel" true
check 'Nix default-branch push saves' "$nix" true
REF=refs/heads/other
check 'Bazel non-default push does not save' "$bazel" false
check 'Nix non-default push does not save' "$nix" false
REF=refs/tags/v0.3.2
check 'Bazel release tag does not save' "$bazel" false
check 'Nix release tag does not save' "$nix" false
EVENT=workflow_dispatch REF=$DEFAULT_REF
check 'Bazel dispatch does not save' "$bazel" false
check 'Nix dispatch does not save' "$nix" false
EVENT=push OS=macOS
check 'Bazel macOS does not save' "$bazel" false
check 'Nix macOS does not save' "$nix" false
OS=Linux RESTORE_ONLY=true
check 'Bazel restore-only does not save' "$bazel" false
RESTORE_ONLY=false CACHE_SAVE=false
check 'Nix cache-save false does not save' "$nix" false
ARCH=ARM64 CACHE_SAVE=true EVENT=pull_request REF=refs/pull/1/merge
check 'Nix default shell on ARM does not save' "$nix" false
check 'Nix default shell on ARM does not restore' "$nix_restore" false
CACHE_SHELL=deploy
check 'Nix deploy shell on ARM saves' "$nix" true
check 'Nix deploy shell on ARM restores' "$nix_restore" true
ARCH=X64 CACHE_SHELL=
EVENT=push REF=$DEFAULT_REF
CACHE_SAVE=true CACHE_HIT=true
check 'Bazel exact restore hit does not save' "$bazel" false
CACHE_HIT=false PR_CACHE_HIT=true EVENT=pull_request
check 'Bazel exact PR cache hit does not save' "$bazel" false
PR_CACHE_HIT=false

# Prefix matches are not exact hits. Empty action outputs also remain false.
export RESTORED_KEY=current-prefix-match BASE_KEY=base-match PR_KEY=pr-prefix-match
for CACHE_HIT in true false ''; do
  for PR_CACHE_HIT in true false ''; do
    if [[ $CACHE_HIT == true || $PR_CACHE_HIT == true ]]; then expected=true; else expected=false; fi
    check_current_hit "$expected"
  done
done
CACHE_HIT=false PR_CACHE_HIT=false

export MODE=export PRUNE=true VALIDATE_OUTCOME=success SAVE=false CURRENT_CACHE_HIT=false
for HEAD_REPO in "$REPO" example-fork/gang; do
  for CURRENT_CACHE_HIT in true false '' prefix-match; do
    if [[ $CURRENT_CACHE_HIT == true ]]; then expected=false; else expected=true; fi
    check "Bazel export exact-hit=$CURRENT_CACHE_HIT head=$HEAD_REPO" "$bazel_export" "$expected"
    check "Bazel upload exact-hit=$CURRENT_CACHE_HIT head=$HEAD_REPO" "$bazel_upload" "$expected"
    check "Bazel shutdown export exact-hit=$CURRENT_CACHE_HIT head=$HEAD_REPO" "$bazel_shutdown" "$expected"
  done
done
HEAD_REPO=$REPO CURRENT_CACHE_HIT=false PRUNE=false
check 'Bazel failed checks do not export' "$bazel_export" false
check 'Bazel failed checks do not upload' "$bazel_upload" false
check 'Bazel failed checks do not shut down for export' "$bazel_shutdown" false
PRUNE=true VALIDATE_OUTCOME=failure
check 'Bazel invalid mode validation blocks shutdown' "$bazel_shutdown" false
VALIDATE_OUTCOME=success MODE=save CURRENT_CACHE_HIT=true SAVE=true
check 'Bazel save-mode shutdown does not depend on export hit optimization' "$bazel_shutdown" true
check 'Bazel save mode does not export shards' "$bazel_export" false
check 'Bazel save mode does not upload shards' "$bazel_upload" false
MODE=export CURRENT_CACHE_HIT=false SAVE=false

export RESTORED_KEY='' BASE_KEY='' PR_KEY=''
for OS in macOS Linux; do
  for ARCH in ARM64 X64; do
    for JOB in cache-x86_64 linux-arm64-checks cross; do
      if [[ $JOB == cache-x86_64 ]]; then BAZEL_CACHE_JOB=linux-x86_64; else BAZEL_CACHE_JOB=$JOB; fi
      for EVENT in pull_request push workflow_dispatch; do
        REF=$DEFAULT_REF
        if [[ $OS != Linux || $ARCH != X64 || $JOB != cache-x86_64 ]]; then
          check "Bazel $OS/$ARCH/$JOB/$EVENT never restores base" "$bazel_base" false
          check "Bazel $OS/$ARCH/$JOB/$EVENT never restores PR" "$bazel_pr" false
          check "Bazel $OS/$ARCH/$JOB/$EVENT never restores current" "$bazel_restore" false
          check "Bazel $OS/$ARCH/$JOB/$EVENT never saves" "$bazel" false
        fi
        if [[ $OS != Linux || $ARCH != X64 || $JOB == cross ]]; then
          check "Nix $OS/$ARCH/$JOB/$EVENT never restores" "$nix_restore" false
          check "Nix $OS/$ARCH/$JOB/$EVENT never saves" "$nix" false
        else
          check "Nix $OS/$ARCH/$JOB/$EVENT restores shared x86 store" "$nix_restore" true
        fi
      done
    done
  done
done
OS=Linux ARCH=X64 JOB=cache-x86_64 BAZEL_CACHE_JOB=linux-x86_64 EVENT=pull_request
check 'Bazel Linux PR restores a base cache' "$bazel_base" true
check 'Bazel Linux PR restores a PR cache' "$bazel_pr" true
check 'Bazel Linux PR does not restore a non-PR cache' "$bazel_restore" false
for EVENT in push workflow_dispatch; do
  check "Bazel Linux $EVENT does not restore a base cache" "$bazel_base" false
  check "Bazel Linux $EVENT does not restore a PR cache" "$bazel_pr" false
  check "Bazel Linux $EVENT restores a current cache" "$bazel_restore" true
done

for kind in root checkout-child checkout-parent home-child home-parent target-child target-parent; do
  check_darwin_guard "$kind"
done

printf '%s cases, %s failures\n' "$cases" "$failures"
(( failures == 0 ))
