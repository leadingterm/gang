#!/usr/bin/env bash
# Ensure the final required check depends on every other CI job.
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
            true
          end
      end
  end
' >/dev/null
