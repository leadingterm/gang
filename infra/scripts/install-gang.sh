#!/usr/bin/env bash
# Install step of the deploy workflow (.github/workflows/deploy.yml): downloads the gang binary for
# this machine's architecture from the Gang release of the tag the workflow is pinned to, checks it
# against the caller's pin, and names it in GANG for the later steps.
#
# Environment: GANG_WORKFLOW_REF (job.workflow_ref, `<owner>/<repo>/<path>@refs/tags/<tag>`),
# GANG_REPOSITORY (job.workflow_repository), GANG_SHA256, GH_TOKEN, RUNNER_TEMP, GITHUB_ENV.
set -euo pipefail

ref="${GANG_WORKFLOW_REF#*@}"
tag="${ref#refs/tags/}"
if [[ "$tag" == "$ref" ]]; then
  echo "::error::pin the deploy workflow to a Gang release tag, not ${ref}" >&2
  exit 1
fi

asset="gang-$(uname -m)-unknown-linux-musl"
dir="${RUNNER_TEMP}/gang-bin"
gh release download "$tag" --repo "$GANG_REPOSITORY" --pattern "$asset" --dir "$dir"

# A release's assets can be replaced, and a checksum published beside them with them, so the
# caller's pin decides, never the release's SHA256SUMS.
sha256sum --check --strict <<<"${GANG_SHA256}  ${dir}/${asset}"

install -m 0755 "${dir}/${asset}" "${dir}/gang"
echo "GANG=${dir}/gang" >>"$GITHUB_ENV"
