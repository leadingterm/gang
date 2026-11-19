#!/usr/bin/env bash
# Ensure the final required check covers blocking CI jobs and aggregators always run.
set -euo pipefail

if (( $# > 1 )); then
  echo "Usage: $0 [workflow]" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
workflow=${1-"$script_dir/../.github/workflows/ci.yml"}

yq -o=json '.jobs' "$workflow" | jq -e '
  if type != "object" or (.required | type) != "object" then
    error("workflow must contain a required job")
  else
    . as $jobs
    | ($jobs.required.needs // []) as $needs
    | if ($needs | type) != "array" or any($needs[]; type != "string") then
        error("required.needs must be an array of job IDs")
      else
        (($jobs | keys - ["required", "macos", "cache-x86_64"]) - $needs) as $missing
        | if any(["macos", "cache-x86_64"][]; . as $id | ($needs | index($id)) != null) then
            error("required must not depend on informational jobs")
          elif ($missing | length) > 0 then
            error("required job is missing dependencies: " + ($missing | join(", ")))
          else
            if ($jobs.changes | type) != "object" then
              error("workflow must contain a changes job")
            else
              ([$jobs.required.steps[]? | .env.NEEDS_RESULTS? | select(type == "string")]) as $result_expressions
              | if ($result_expressions | length) != 1 then
                  error("required must have exactly one NEEDS_RESULTS expression")
                elif ([$result_expressions[0] | scan("needs\\.([A-Za-z0-9_-]+)\\.result") | .[0]] | unique)
                     != ($needs | unique) then
                  error("required results must cover every dependency")
                else
                  .
                end
              |
              reduce ["lint", "linux-x86_64", "linux-arm64", "e2e-gate-1", "e2e-gate-2", "required"][] as $job (true;
                if ($jobs[$job] | if type == "object" then .if else null end) == "always()" then
                  .
                else
                  error("unsafe aggregator " + $job + ": if must be always()")
                end)
              | if ($jobs.lint.needs | if type == "array" then sort else . end)
                   != ["changes", "lint-arm64", "nix-vm"] then
                  error("lint must depend on changes, lint-arm64, and nix-vm")
                elif ($jobs["linux-x86_64"].needs | if type == "array" then sort else . end)
                     != ["changes", "linux-x86_64-shards"] then
                  error("linux-x86_64 must depend on changes and all shards")
                elif ($jobs["linux-x86_64"].steps | type) != "array"
                     or ($jobs["linux-x86_64"].steps | length) != 1
                     or ($jobs["linux-x86_64"].steps[0].uses != null)
                     or ($jobs["linux-x86_64"].steps[0].run | type) != "string"
                     or ($jobs["linux-x86_64"].steps[0].env.NEEDS_RESULTS | type) != "string" then
                  error("linux-x86_64 must contain only its status check")
                elif ($jobs["cache-x86_64"].needs | if type == "array" then sort else . end)
                     != ["changes", "linux-x86_64-shards"] then
                  error("cache-x86_64 must depend on changes and all shards")
                elif ($jobs["linux-arm64"].needs | if type == "array" then sort else . end)
                     != ["changes", "linux-arm64-bundle", "linux-arm64-checks"] then
                  error("linux-arm64 must depend on changes, bundle, and checks")
                elif ($jobs["linux-arm64-checks"].needs | if type == "array" then sort else . end)
                     != ["changes", "linux-arm64-bundle"] then
                  error("linux-arm64-checks must depend on changes and bundle")
                elif ($jobs["linux-arm64-bundle"] | type) != "object" then
                  error("workflow must contain linux-arm64-bundle")
                elif any(["e2e-gate-1-shards", "e2e-gate-2-shards"][];
                         ($jobs[.].needs | if type == "array" then sort else . end)
                         != ["changes", "linux-arm64-bundle"]) then
                  error("e2e shards must depend on changes and linux-arm64-bundle")
                elif ($jobs["e2e-gate-1"].needs | if type == "array" then sort else . end)
                     != ["changes", "e2e-gate-1-shards"] then
                  error("e2e-gate-1 must depend on changes and all shards")
                elif ($jobs["e2e-gate-2"].needs | if type == "array" then sort else . end)
                     != ["changes", "e2e-gate-2-shards"] then
                  error("e2e-gate-2 must depend on changes and all shards")
                else
                  .
                end
              | reduce (($jobs | keys) - ["changes", "lint", "linux-x86_64", "linux-arm64", "e2e-gate-1", "e2e-gate-2", "required"])[] as $job (.;
                  ($jobs[$job].needs // []) as $deps
                  | if ($jobs[$job] | type) == "object"
                       and ($deps == "changes" or (($deps | type) == "array" and ($deps | index("changes")) != null)) then
                      .
                    else
                      error("job " + $job + " must directly depend on changes")
                    end)
            end
          end
      end
  end
' >/dev/null
