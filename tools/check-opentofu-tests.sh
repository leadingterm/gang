#!/usr/bin/env bash
# Every native test file must be selected by a Bazel contract test, not merely in its runfiles.
set -euo pipefail
cd "$(dirname "$0")/.."
bazel=${BAZEL:-bazel}

while IFS= read -r -d '' file; do
  if [[ $file == */tests/* ]]; then
    package=${file%/tests/*}
  else
    package=${file%/*}
  fi
  test_file=${file#"$package"/}
  pattern=
  for ((i = 0; i < ${#test_file}; i++)); do
    char=${test_file:i:1}
    case $char in
      [a-zA-Z0-9_/]) pattern+=$char ;;
      *) pattern+="\\$char" ;;
    esac
  done
  targets=$("$bazel" query "kind('_opentofu_contract_test rule', attr(test_file, '^${pattern}$', //${package}:*))" --output=label)
  if [[ -z $targets ]]; then
    printf 'No opentofu_contract_test runs %s\n' "$file" >&2
    exit 1
  fi
done < <(find infra -type f -name '*.tftest.hcl' -print0)
