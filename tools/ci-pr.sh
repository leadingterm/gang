#!/usr/bin/env bash
# Execute the real PR preflight against the selected synthetic merge tree.
set -euo pipefail
cd "$(dirname "$0")/.."
if (( $# != 2 )); then
  echo 'Usage: tools/ci-pr.sh BASE HEAD' >&2
  exit 2
fi

scratch=$(mktemp -d "${TMPDIR:-/tmp}/gang-ci-pr.XXXXXXXX")
trap 'rm -rf "$scratch"' EXIT

tools/lint.sh --portable
python3 tools/ci_pr_targets.py "$1" "$2" > "$scratch/manifest.json"
# Parse data, never shell code; validate the entire manifest before invoking Bazel.
python3 - "$scratch/manifest.json" "$scratch/lint" "$scratch/unit" <<'PY'
import json
import re
import sys
from pathlib import Path


def object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


with open(sys.argv[1], encoding="utf-8") as stream:
    manifest = json.load(stream, object_pairs_hook=object_without_duplicates)
if not isinstance(manifest, dict) or set(manifest) != {"lint", "unit"}:
    raise ValueError("manifest must contain exactly lint and unit")
label_pattern = re.compile(r"(?:@@?[A-Za-z0-9._+~-]+)?//([A-Za-z0-9._+/-]*):([A-Za-z0-9._+@=-]+(?:/[A-Za-z0-9._+@=-]+)*)")
for key in ("lint", "unit"):
    labels = manifest[key]
    if not isinstance(labels, list) or (key == "lint" and not labels):
        raise ValueError(f"invalid {key} list")
    for label in labels:
        match = label_pattern.fullmatch(label) if isinstance(label, str) else None
        if not match or any(part in ("", ".", "..") for part in match.group(1).split("/") if match.group(1)) or any(part in (".", "..") for part in match.group(2).split("/")):
            raise ValueError(f"invalid {key} label")
    Path(sys.argv[2 if key == "lint" else 3]).write_text(
        "".join(label + "\n" for label in labels), encoding="utf-8"
    )
PY

bazel=${BAZEL:-bazel}
"$bazel" build //:gang
"$bazel" test --skip_incompatible_explicit_targets --target_pattern_file="$scratch/lint"
if [[ -s $scratch/unit ]]; then
  "$bazel" test --skip_incompatible_explicit_targets --target_pattern_file="$scratch/unit"
else
  echo 'No affected Rust unit tests.'
fi
