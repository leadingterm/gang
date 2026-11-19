#!/usr/bin/env bash
# Exercise the cache save expressions from the composite actions with synthetic events.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
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
  expr=${expr//runner.os/"\"\$OS\""}
  expr=${expr//steps.restore.outputs.cache-hit/"\"\$CACHE_HIT\""}
  expr=${expr//steps.pr.outputs.cache-hit/"\"\$PR_CACHE_HIT\""}
  expr=${expr//inputs.restore-only/"\"\$RESTORE_ONLY\""}
  expr=${expr//inputs.cache-save/"\"\$CACHE_SAVE\""}
  if [[ $expr == *github.* || $expr == *inputs.* || $expr == *steps.* || $expr == *runner.* || $expr == *'format('* ]]; then
    printf 'Unsupported GitHub expression: %s\n' "$expr" >&2
    return 1
  fi
  expr=${expr//\(/ ( }
  expr=${expr//\)/ ) }
  printf '%s' "$expr"
}

# yq parses the actual action YAML rather than matching source formatting.
bazel=$(yq -er '.runs.steps[] | select(.name == "Mark the start of the build") | .env.SAVE' "$script_dir/../.github/actions/setup-bazel/action.yml")
nix=$(yq -o=json '.runs.steps' "$script_dir/../.github/actions/setup-nix/action.yml" | jq -er '.[] | select((.uses // "") | startswith("nix-community/cache-nix-action@")) | .with.save')
cargo=$(yq -er '.jobs.lint-arm64.steps[] | select(.name == "Name the Cargo cache") | .env.SAVE' "$script_dir/../.github/workflows/ci.yml")
bazel=$(translate "$bazel")
nix=$(translate "$nix")
cargo=$(translate "$cargo")

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

export EVENT=pull_request REF=refs/pull/1/merge DEFAULT_REF=refs/heads/main
export REPO=example-org/gang HEAD_REPO=example-org/gang OS=Linux
export CACHE_HIT=false PR_CACHE_HIT=false RESTORE_ONLY=false CACHE_SAVE=true

check 'Bazel same-repository PR saves' "$bazel" true
check 'Nix same-repository PR saves' "$nix" true
check 'Cargo same-repository PR saves' "$cargo" true
HEAD_REPO=example-fork/gang
check 'Bazel fork PR does not save' "$bazel" false
check 'Nix fork PR does not save' "$nix" false
check 'Cargo fork PR does not save' "$cargo" false
HEAD_REPO=
check 'Bazel missing head repository does not save' "$bazel" false
check 'Nix missing head repository does not save' "$nix" false
check 'Cargo missing head repository does not save' "$cargo" false
HEAD_REPO=$REPO EVENT=push REF=$DEFAULT_REF
check 'Bazel default-branch push saves' "$bazel" true
check 'Nix default-branch push saves' "$nix" true
check 'Cargo default-branch push saves' "$cargo" true
REF=refs/heads/other
check 'Bazel non-default push does not save' "$bazel" false
check 'Nix non-default push does not save' "$nix" false
check 'Cargo non-default push does not save' "$cargo" false
EVENT=workflow_dispatch REF=$DEFAULT_REF
check 'Bazel dispatch does not save' "$bazel" false
check 'Nix dispatch does not save' "$nix" false
check 'Cargo dispatch does not save' "$cargo" false
EVENT=push OS=macOS
check 'Bazel macOS does not save' "$bazel" false
check 'Nix macOS does not save' "$nix" false
check 'Cargo save policy is independent of OS' "$cargo" true
OS=Linux RESTORE_ONLY=true
check 'Bazel restore-only does not save' "$bazel" false
check 'Cargo save policy is independent of Bazel restore-only' "$cargo" true
RESTORE_ONLY=false CACHE_SAVE=false
check 'Nix cache-save false does not save' "$nix" false
check 'Cargo save policy is independent of Nix cache-save' "$cargo" true
CACHE_SAVE=true CACHE_HIT=true
check 'Bazel exact restore hit does not save' "$bazel" false
check 'Cargo save policy is independent of Bazel cache hit' "$cargo" true
CACHE_HIT=false PR_CACHE_HIT=true EVENT=pull_request
check 'Bazel exact PR cache hit does not save' "$bazel" false
check 'Cargo save policy is independent of Bazel PR cache hit' "$cargo" true

printf '%s cases, %s failures\n' "$cases" "$failures"
(( failures == 0 ))
