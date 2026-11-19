#!/usr/bin/env bash
# Guard the x86 PR critical path and the push-only release validation graph.
set -euo pipefail

if (( $# > 1 )); then
  echo "Usage: $0 [workflow]" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
workflow=${1-"$script_dir/../.github/workflows/ci.yml"}

yq -o=json '.jobs' "$workflow" | jq -e '
  . as $jobs
  | def needs($id; $expected):
      if ($jobs[$id].needs | if type == "string" then [.] else . end | sort)
         == ($expected | sort) then .
      else error($id + " must depend on exactly: " + ($expected | join(", "))) end;
    def results($id):
      ([$jobs[$id].steps[]?.env[]? | strings
        | scan("needs\\.([A-Za-z0-9_-]+)\\.result") | .[0]] | unique) as $results
      | if $results == ($jobs[$id].needs | unique) then .
        else error($id + " results must cover every dependency") end;
    def status_only($id):
      if ($jobs[$id].steps | type) == "array" and ($jobs[$id].steps | length) == 1
         and $jobs[$id].steps[0].uses == null
         and ($jobs[$id].steps[0].run | type) == "string"
         and ($jobs[$id].steps[0].env.NEEDS_RESULTS | type) == "string" then .
      else error($id + " must contain only its status check") end;
    def push_only($id):
      ($jobs[$id].if // "") as $condition
      | if ($condition | contains("github.event_name == '\''push'\''"))
           and ($condition | contains("refs/heads/main"))
           and ($condition | contains("refs/heads/master"))
           and ($condition | contains("refs/tags/v")) then .
        else error($id + " must run only for main/master pushes and v* tags") end;
  if type != "object" or (.required | type) != "object" then
    error("workflow must contain a required job")
  elif (.changes | type) != "object" then
    error("workflow must contain a changes job")
  else . end
  | needs("required"; ["changes", "nix-vm", "lint", "linux-x86_64-shards", "linux-x86_64", "linux-x86_64-bundle", "e2e-gate-1-shards", "e2e-gate-1", "e2e-gate-2-shards", "e2e-gate-2"])
  | results("required")
  | needs("nix-vm"; ["changes"])
  | needs("linux-x86_64-shards"; ["changes"])
  | needs("lint"; ["changes"])
  | if ([$jobs.lint.steps[]?.run | strings | select(contains("tools/lint.sh --portable"))] | length) != 1 then
      error("lint must execute tools/lint.sh --portable")
    elif ([$jobs.lint.steps[]? | select(.uses == "./.github/actions/setup-bazel")
           | select(.with."cache-job" == "linux-x86_64" and .with."restore-only" == "true")] | length) != 1 then
      error("lint must restore the canonical x86 cache read-only")
    elif ([$jobs.lint.steps[]? | select(.uses == "./.github/actions/save-bazel-cache")
           | select(.with.mode == "export" and .with.shard == "x86-lint")] | length) != 1 then
      error("lint must export x86-lint")
    elif ([$jobs.lint.steps[]? | select(.run? | strings | contains("tools/lint.sh --portable"))
           | select(."timeout-minutes" > 0 and ."timeout-minutes" <= 3)] | length) != 1 then
      error("portable lint must be bounded to three minutes")
    else . end
  | needs("linux-x86_64"; ["changes", "linux-x86_64-shards"])
  | status_only("linux-x86_64")
  | needs("linux-x86_64-bundle"; ["changes"])
  | if ([$jobs["linux-x86_64-bundle"].steps[]? | select(.uses == "./.github/actions/setup-bazel")
         | select(.with."cache-job" == "linux-x86_64" and .with."restore-only" == "true")] | length) != 1
       or ([$jobs["linux-x86_64-bundle"].steps[]? | select(.uses == "./.github/actions/save-bazel-cache")
         | select(.with.mode == "export" and .with.shard == "x86-bundle")] | length) != 1
       or ([$jobs["linux-x86_64-bundle"].steps[]?.run | strings
         | select(contains("bazel build") and contains("//:gang") and contains("//crates/gang-e2e:bundle"))] | length) != 1
       or ([$jobs["linux-x86_64-bundle"].steps[]? | select(.uses == "./.github/actions/upload-gang")
         | select(.with.target == "x86_64-unknown-linux-musl")] | length) != 1
       or ([$jobs["linux-x86_64-bundle"].steps[]?.with.name | select(. == "e2e-harness-x86_64")] | length) != 1 then
      error("x86 bundle must build and export the x86 gang, harness, and working set")
    else . end
  | needs("cache-x86_64"; ["changes", "linux-x86_64-shards", "linux-x86_64-bundle", "lint"])
  | if ([$jobs["cache-x86_64"].steps[]?.with.pattern | select(. == "bazel-working-set-x86-*")] | length) != 1 then
      error("cache-x86_64 must merge every x86 working set")
    else . end
  | reduce ["linux-x86_64-shards", "linux-x86_64-bundle", "lint"][] as $id (.;
      if ($jobs["cache-x86_64"].if | contains("needs." + $id + ".result == '\''success'\''")) then .
      else error("cache-x86_64 must wait for successful " + $id) end)
  | needs("linux-arm64-bundle"; ["changes"])
  | needs("cross"; ["changes"])
  | needs("macos"; ["changes"])
  | needs("linux-arm64"; ["changes", "linux-arm64-bundle", "linux-arm64-checks"])
  | needs("linux-arm64-checks"; ["changes", "linux-arm64-bundle"])
  | needs("release-validation"; ["changes", "required", "linux-arm64", "cross", "macos"])
  | results("release-validation")
  | reduce ["e2e-gate-1", "e2e-gate-2"][] as $id (.;
      needs($id + "-shards"; ["changes", "linux-x86_64-bundle"])
      | needs($id; ["changes", $id + "-shards"]))
  | reduce ["linux-x86_64", "e2e-gate-1", "e2e-gate-2", "required"][] as $id (.;
      results($id)
      | if $jobs[$id].if == "always()" then .
      else error("unsafe aggregator " + $id + ": if must be always()") end)
  | reduce ["linux-arm64-bundle", "linux-arm64-checks", "linux-arm64", "cross", "macos", "release-validation"][] as $id (.;
      push_only($id))
  | if ($jobs["linux-arm64"].if | contains("always()"))
       and ($jobs["release-validation"].if | contains("always()")) then .
    else error("push aggregators must always run after their dependencies") end
  | reduce ["nix-vm", "lint", "linux-x86_64-shards", "linux-x86_64-bundle", "cache-x86_64", "e2e-gate-1-shards", "e2e-gate-2-shards", "linux-arm64-bundle", "linux-arm64-checks", "cross", "macos"][] as $id (.;
      if ($jobs[$id].if | contains("needs.changes.outputs.heavy == '\''true'\''")) then .
      else error($id + " must skip intentionally when heavy is false") end)
  | reduce ["nix-vm", "lint", "linux-x86_64-shards", "linux-x86_64", "linux-x86_64-bundle", "e2e-gate-1-shards", "e2e-gate-1", "e2e-gate-2-shards", "e2e-gate-2", "required"][] as $id (.;
      ($jobs[$id]."runs-on" // "") as $runner
      | if ($runner | type) == "string" and ($runner | contains("ubuntu-24.04"))
           and ($runner | contains("linux-x64")) and ($runner | contains("arm64") | not) then .
        else error($id + " must use the existing x86 runner labels") end)
  | reduce (($jobs | keys) - ["changes"])[] as $id (.;
      ($jobs[$id].needs // []) as $deps
      | if $deps == "changes" or (($deps | type) == "array" and ($deps | index("changes")) != null) then .
        else error("job " + $id + " must directly depend on changes") end)
' >/dev/null
