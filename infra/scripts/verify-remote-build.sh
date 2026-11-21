#!/usr/bin/env bash
set -euo pipefail

REMOTE_BUILD_WORKFLOW=${REMOTE_BUILD_WORKFLOW:-remote-build.yml}
if [[ ! $REMOTE_BUILD_WORKFLOW =~ ^[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml$ ]]; then
  echo '::error::invalid remote-build workflow filename' >&2
  exit 1
fi
if ! run_id=$(gh api --method POST "repos/$GITHUB_REPOSITORY/actions/workflows/$REMOTE_BUILD_WORKFLOW/dispatches" \
  -H 'X-GitHub-Api-Version: 2026-03-10' -f ref="$GITHUB_REF_NAME" \
  -f "inputs[salt]=deploy-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT" \
  -F return_run_details=true --jq .workflow_run_id); then
  echo '::error::cannot dispatch remote build: workflow dispatch request failed' >&2
  exit 1
fi
if [[ ! $run_id =~ ^[1-9][0-9]*$ ]]; then
  echo '::error::cannot dispatch remote build: response did not contain a valid workflow run ID' >&2
  exit 1
fi
url="https://github.com/$GITHUB_REPOSITORY/actions/runs/$run_id"
if ! gh run watch "$run_id" --repo "$GITHUB_REPOSITORY" --exit-status; then
  echo "::error::remote build failed: $url" >&2
  exit 1
fi
if ! gh run view "$run_id" --repo "$GITHUB_REPOSITORY" --json headSha,event,status,conclusion |
  jq -e --arg sha "$GITHUB_SHA" '.headSha==$sha and .event=="workflow_dispatch" and .status=="completed" and .conclusion=="success"' >/dev/null; then
  echo "::error::remote build does not match deployed commit or did not succeed: $url" >&2
  exit 1
fi
