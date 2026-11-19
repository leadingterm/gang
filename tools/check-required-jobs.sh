#!/usr/bin/env bash
# Ensure the final required check covers every CI job and aggregators always run.
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
        (($jobs | keys - ["required"]) - $needs) as $missing
        | if ($missing | length) > 0 then
            error("required job is missing dependencies: " + ($missing | join(", ")))
          else
            if ($jobs.changes | type) != "object" then
              error("workflow must contain a changes job")
            else
              ([$jobs.required.steps[]? | .env.NEEDS_RESULTS? | select(type == "string")]) as $result_expressions
              | if ($result_expressions | length) != 1 then
                  error("required must have exactly one NEEDS_RESULTS expression")
                elif ([$result_expressions[0] | scan("needs\\.([A-Za-z0-9_-]+)\\.result") | .[0]] | unique)
                     != (($needs - ["macos"]) | unique) then
                  error("required private results must cover every dependency except macos")
                else
                  .
                end
              |
              reduce ["lint", "e2e-gate-2", "required"][] as $job (true;
                if ($jobs[$job] | if type == "object" then .if else null end) == "always()" then
                  .
                else
                  error("unsafe aggregator " + $job + ": if must be always()")
                end)
              | if ($jobs.lint.needs | if type == "array" then sort else . end)
                   != ["changes", "lint-arm64", "nix-vm"] then
                  error("lint must depend on changes, lint-arm64, and nix-vm")
                elif ($jobs["e2e-gate-2"].needs | if type == "array" then sort else . end)
                     != ["changes", "e2e-gate-2-shard-1", "e2e-gate-2-shard-2"] then
                  error("e2e-gate-2 must depend on changes and both shards")
                else
                  .
                end
              | reduce (($jobs | keys) - ["changes", "lint", "e2e-gate-2", "required"])[] as $job (.;
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
