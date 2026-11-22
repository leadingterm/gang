#!/usr/bin/env bash
# Guard the PR preflight and the merge-queue validation graph.
set -euo pipefail

if (( $# > 2 )); then
  echo "usage: $0 [ci.yml [pr.yml]]" >&2
  exit 2
fi
script_dir=$(cd "$(dirname "$0")" && pwd)
ci=${1-"$script_dir/../.github/workflows/ci.yml"}
pr=${2-"$script_dir/../.github/workflows/pr.yml"}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
yq -o=json '.' "$ci" >"$scratch/ci.json"
yq -o=json '.' "$pr" >"$scratch/pr.json"

jq -en --slurpfile ci "$scratch/ci.json" --slurpfile pr "$scratch/pr.json" '
  def require($ok; $message): if $ok then . else error($message) end;
  def deps: (.needs // [] | if type == "string" then [.] else . end | sort);
  def scope: (.if // "" | gsub("^\\$\\{\\{\\s*|\\s*\\}\\}$"; "") | gsub("^\\s+|\\s+$"; ""));
  def checked($dep):
    . as $job
    | require(($job.steps | type) == "array" and ($job.steps | length) == 1
       and $job.steps[0].uses == null and $job.steps[0].if == null and $job.steps[0].shell == "bash" and ($job.steps[0].run | type) == "string"
       and $job.steps[0].env.RESULT == ("${{ needs." + $dep + ".result }}")
       and ($job.steps[0].run | gsub("\\s+$"; "")) == "test \"$RESULT\" = success"; "status job must check the dependency result in its only step");
  def readiness($dep; $condition):
    require(deps == [$dep]; "readiness must depend only on " + $dep)
    | require(scope == $condition; "readiness condition is unsafe")
    | require(.name == "${{ matrix.check }}" and .strategy.matrix == {"check": ["lint", "cross", "linux-x86_64", "linux-arm64", "e2e-gate-1", "e2e-gate-2"]}; "readiness must expose exactly the six required checks")
    | checked($dep);
  def checkout:
    require((.uses == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1") and (.with.ref == "${{ github.sha }}") and (.with."persist-credentials" == false) and .if == null; "checkout must use pinned action and github.sha without persisted credentials");
  def no_continue:
    require((."continue-on-error" == null or ."continue-on-error" == false) and .permissions == null
      and all(.steps[]?; ."continue-on-error" == null or ."continue-on-error" == false); "critical work must not continue on error or override permissions");
  ["nativelink-memory-tests", "nix-vm", "lint-checks", "linux-x86_64-shards", "linux-x86_64-bundle", "linux-arm64-bundle", "linux-arm64-checks", "cross-build", "e2e-gate-1-shards", "e2e-gate-2-shards", "e2e-v035-upgrade"] as $critical
  | $ci[0] as $c | $pr[0] as $p
  | $c | require((.on | keys) == ["merge_group"] and .on.merge_group.types == ["checks_requested"]; "CI must trigger only for requested merge groups")
  | require(.permissions == {"contents":"read"}; "CI permissions must be read-only")
  | require((.jobs | keys | sort) == (($critical + ["macos", "readiness", "release-validation"]) | sort); "unexpected or missing full CI job")
  | .jobs as $jobs
  | reduce $critical[] as $id (.;
      $jobs[$id] | require(type == "object"; "missing critical job " + $id)
      | require(scope == "github.event_name == '\''merge_group'\''"; "unsafe event scope for " + $id)
      | require([.steps[]? | select((.uses // "") | startswith("actions/checkout@")) | checkout] | length == 1; "critical job must check out the queue SHA")
      | require(all(.steps[]?; .if == null or (.uses == "./.github/actions/save-bazel-cache" and .if == "always() && job.status == '\''success'\''") or (.with.name == "bazel-metrics-x86-bundle" and .if == "always()")); "critical steps must not skip")
      | no_continue)
  | reduce ($critical - ["nix-vm", "linux-arm64-checks", "e2e-gate-1-shards", "e2e-gate-2-shards", "e2e-v035-upgrade"])[] as $id (.; $jobs[$id] | require(deps == []; "unexpected dependency for " + $id))
  | $jobs["nix-vm"] | require(deps == ["linux-arm64-bundle"]; "Nix VM checks must depend only on the ARM bundle")
  | $jobs["linux-arm64-checks"] | require(deps == ["linux-arm64-bundle"]; "ARM checks must depend on the ARM bundle")
  | reduce ["nativelink-memory-tests", "nix-vm", "lint-checks", "linux-x86_64-shards", "linux-x86_64-bundle", "e2e-gate-1-shards", "e2e-gate-2-shards", "e2e-v035-upgrade"][] as $id (.;
      $jobs[$id] | require((."runs-on" | strings | contains("linux-x64") and contains("ubuntu-24.04")); "x86 job must use x86 runners"))
  | $jobs["nativelink-memory-tests"] | require([.steps[]? | select(.name == "Run NativeLink memory tests" and (.run | contains("cargo test -p nativelink-scheduler") and contains("cargo test -p nativelink-worker") and contains("cargo test -p nativelink-service")))] | length == 1; "NativeLink memory tests must run")
  | $jobs["nix-vm"] | require([.steps[]? | select(.run == "tools/lint.sh --nix-vm")] | length == 1; "Nix VM checks must run")
  | $jobs["cross-build"] | require([.steps[]? | select(.run? | strings | contains("bazel build") and contains("--platforms=@zig_sdk//libc_aware/platform:linux_amd64_musl") and contains("//:gang"))] | length == 1; "ARM-to-x86 cross-build must run")
  | $jobs["linux-arm64-checks"] | require([.steps[]? | select(.name == "Test" and (.run | contains("bazel test") and contains("static_binary_test") and contains("hold_signal_test") and contains("postgres_test")))] | length == 1; "native ARM boundary checks must run")
  | $jobs["linux-x86_64-shards"] | require([.steps[]? | select(.name == "Select shard targets" and .run == "tools/ci-targets.sh --query | tools/ci-targets.sh \"$SHARD\" 4 > \"$RUNNER_TEMP/targets\"" and .env.SHARD == "${{ matrix.shard }}")] | length == 1; "x86 shard selection must use the full selector")
  | $jobs["linux-x86_64-shards"] | require([.steps[]? | select(.name == "Test" and (.run | contains("bazel test") and contains("--target_pattern_file=\"$RUNNER_TEMP/targets\"")))] | length == 1; "x86 shard tests must execute selected targets")
  | $jobs["release-validation"]
  | require(.steps[0].env.NEEDS_RESULTS == "${{ join(needs.*.result, '\'' '\'') }}"; "release validation must receive every dependency result")
  | require(deps == ($critical | sort); "release validation must directly depend on all eleven critical jobs")
  | require(scope == "always() && github.event_name == '\''merge_group'\''"; "release validation must always run on merge groups")
  | no_continue
  | require((.steps | length) == 1 and .steps[0].if == null and .steps[0].shell == "bash" and (.steps[0].run | type) == "string"; "release validation must have one unconditional Bash result-checking step")
  | $jobs.readiness | readiness("release-validation"; "always() && github.event_name == '\''merge_group'\''") | no_continue
  | $jobs.macos | require(scope == "github.event_name == '\''merge_group'\'' && !github.event.repository.private"; "macOS must remain advisory and queue-only")
  | [$jobs[].steps[]? | select((.uses // "") | startswith("actions/checkout@")) | checkout] | . as $checkouts
  | require(($checkouts | length) >= 11; "full jobs require checkouts")
  | reduce ["e2e-gate-1-shards", "e2e-gate-2-shards"][] as $id (.;
      $jobs[$id] | require(deps == ["linux-x86_64-bundle"]; "E2E must depend on the x86 bundle")
      | require(.strategy.matrix == {"shard":[1,2,3,4,5,6,7,8]}; "E2E must run all eight shards")
      | (if $id == "e2e-gate-1-shards" then "--gate 1" else "--gate 2 --additions" end) as $selection
      | require([.steps[]? | select(.uses == "./.github/actions/run-e2e") | .with.args] == [$selection + " --shard ${{ matrix.shard }}/${{ strategy.job-total }}"]; "incorrect E2E selection"))
  | $jobs["e2e-v035-upgrade"]
  | require(deps == ["linux-x86_64-bundle"] and .strategy == null; "v035 upgrade must depend on the x86 bundle without a matrix")
  | require([.steps[]? | select(.uses == "./.github/actions/run-e2e") | .with.args] == ["--gate 2 --scenario rolling-upgrade --previous-gang ${{ runner.temp }}/e2e/gang-v035 --previous-fixture ${{ runner.temp }}/e2e/deployment-v035.json"]; "v035 upgrade must run the rolling scenario with the published pair")
  | $jobs["linux-x86_64-shards"]
  | require(.strategy.matrix == {"shard":[1,2,3,4]}; "x86 must run four shards")
  | require([.steps[]? | select(.uses == "./.github/actions/save-bazel-cache") | select(.with.mode == "export" and .with.shard == "x86-${{ matrix.shard }}")] | length == 1; "x86 shards must export working sets")
  | $jobs["lint-checks"]
  | require([.steps[]? | select((.uses // "") | startswith("actions/checkout@")) | select(.with."fetch-depth" == 0)] | length == 1; "portable lint requires full checkout history")
  | require([.steps[]? | select(.run == "tools/lint.sh --portable")] | length == 1; "portable lint must run")
  | require([.steps[]? | select(.uses == "./.github/actions/save-bazel-cache") | select(.with.mode == "export" and .with.shard == "x86-lint")] | length == 1; "lint must export its working set")
  | reduce ["linux-x86_64-bundle", "linux-arm64-bundle"][] as $id (.;
      $jobs[$id] | (if $id == "linux-x86_64-bundle" then "x86_64" else "aarch64" end) as $arch
      | require([.steps[]? | select(.run? | strings | contains("bazel build") and contains("//:gang") and contains("//crates/gang-e2e:bundle"))] | length == 1; "bundle must build gang and harness")
      | require([.steps[]? | select(.uses == "./.github/actions/upload-gang")] | length == 1; "bundle must upload gang")
      | require([.steps[]? | select((.uses // "") | startswith("actions/upload-artifact@")) | select(.with.name == ("e2e-harness-" + $arch) and .with."if-no-files-found" == "error")] | length == 1; "bundle must export its harness"))
  | $jobs["linux-x86_64-bundle"]
  | require([.steps[]? | select(.with.name == "bazel-metrics-x86-bundle" and .uses == "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9" and .if == "always()" and .with."retention-days" == 7 and (.with.path | contains("bazel-build.bep.json") and contains("bazel-build.profile.gz")))] | length == 1; "bundle metrics must always be exported")
  | (.steps | to_entries) as $steps
  | require(([$steps[] | select(.value.name == "Build gang and e2e harness") | .key] | first) < ([$steps[] | select(.value.uses == "./.github/actions/upload-gang") | .key] | first); "bundle must be built before upload")
  | $p | require((.on | keys) == ["pull_request"]; "PR workflow must trigger only on pull_request")
  | require(.permissions == {"contents":"read"}; "PR permissions must be read-only")
  | require((.jobs | keys | sort) == ["preflight", "readiness"]; "PR workflow must contain only preflight and readiness")
  | .jobs.preflight | no_continue
  | require((.needs // []) == [] and ."timeout-minutes" == 30 and scope == "github.event_name == '\''pull_request'\''"; "preflight must run for every PR with a 30 minute timeout")

  | require([.steps[]? | select((.uses // "") | startswith("actions/checkout@")) | checkout | select(.with."fetch-depth" == 0)] | length == 1; "preflight must check out the test merge with full history")
  | require([.steps[]? | select(.uses == "./.github/actions/setup-nix")] | length == 1; "preflight must set up Nix")
  | require([.steps[]? | select(.uses == "./.github/actions/setup-bazel" and .with."cache-job" == "linux-x86_64")] | length == 1; "preflight must use the x86 cache")
  | require([.steps[]? | select((.run // "" | startswith("tools/ci-pr.sh \"$BASE\" \"$HEAD\"\n")) and .env.BASE == "${{ github.event.pull_request.base.sha }}" and .env.HEAD == "${{ github.sha }}" and .if == null)] | length == 1; "preflight must run the real command on the PR base and test merge")
  | require([.steps[]? | select(.uses == "./.github/actions/save-bazel-cache" and .if == "success() && github.event.pull_request.head.repo.full_name == github.repository" and .with.mode == "save" and .with.prune == "true")] | length == 1; "preflight cache writes must be restricted to same-repo PRs")
  | $p.jobs.readiness | readiness("preflight"; "always() && github.event_name == '\''pull_request'\''") | no_continue
  | true
' >/dev/null
# Exercise the workflow's actual release result check, not a copy of its logic.
jq -r '.jobs["release-validation"].steps[0].run' "$scratch/ci.json" >"$scratch/release.sh"
good="success success success success success success success success success success success"
if ! NEEDS_RESULTS="$good" bash --noprofile --norc -e -u -o pipefail "$scratch/release.sh"; then
  echo "release validation rejects successful jobs" >&2
  exit 1
fi
for ((i = 0; i < 11; i++)); do
  for bad in failure cancelled skipped missing; do
    results=(success success success success success success success success success success success)
    if [[ $bad == missing ]]; then
      unset 'results[i]'
    else
      results[i]=$bad
    fi
    if NEEDS_RESULTS="${results[*]}" bash --noprofile --norc -e -u -o pipefail "$scratch/release.sh"; then
      echo "release validation accepted $bad at dependency $i" >&2
      exit 1
    fi
  done
done
