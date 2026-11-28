#!/usr/bin/env bash
# Exercise the PR driver with a fake selector and Bazel, without a build.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gang-ci-pr-test.XXXXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/repo/tools" "$scratch/tmp"
cp "$script_dir/ci-pr.sh" "$scratch/repo/tools/ci-pr.sh"
cat > "$scratch/repo/tools/lint.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'lint %s\n' "$*" >> "$LOG"
[[ ${FAIL_LINT:-0} != 1 ]]
SH
cat > "$scratch/repo/tools/ci_pr_targets.py" <<'PY'
import os
import sys

with open(os.environ["LOG"], "a", encoding="utf-8") as stream:
    stream.write("selector " + " ".join(sys.argv[1:]) + "\n")
if os.environ.get("FAIL_SELECTOR") == "1":
    sys.exit(1)
print(os.environ["MANIFEST"])
PY
cat > "$scratch/bazel" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'bazel %s\n' "$1" >> "$LOG"
if [[ $1 == test ]]; then
  for arg in "$@"; do
    case $arg in
      --target_pattern_file=*) cat "${arg#*=}" >> "$LOG" ;;
    esac
  done
fi
[[ ${FAIL_BAZEL:-0} != 1 ]]
SH
chmod +x "$scratch/repo/tools/lint.sh" "$scratch/bazel"
export LOG="$scratch/log" BAZEL="$scratch/bazel" TMPDIR="$scratch/tmp"
script="$scratch/repo/tools/ci-pr.sh"
export MANIFEST='{"lint":["//crate:crate_clippy_test","//crate:crate_rustfmt_test"],"unit":["//crate:crate_unit_test"],"build_binary":true}'
"$script" base head
cat > "$scratch/expected" <<'EOF'
lint --portable
selector base head
bazel build
bazel test
//crate:crate_clippy_test
//crate:crate_rustfmt_test
bazel test
//crate:crate_unit_test
EOF
cmp "$scratch/expected" "$LOG"
: > "$LOG"
export MANIFEST='{"lint":["//:root_clippy_test"],"unit":[],"build_binary":false}'
"$script" base head
cat > "$scratch/expected" <<'EOF'
lint --portable
selector base head
bazel test
//:root_clippy_test
EOF
cmp "$scratch/expected" "$LOG"

while IFS= read -r manifest; do
  : > "$LOG"
  if env MANIFEST="$manifest" "$script" base head; then
    echo 'Invalid manifest was accepted' >&2
    exit 1
  fi
  printf 'lint --portable\nselector base head\n' > "$scratch/expected"
  cmp "$scratch/expected" "$LOG"
done <<'EOF'
{}
{"lint":[],"unit":[],"build_binary":false}
{"lint":["//pkg:ok"],"unit":[],"extra":[]}
{"lint":["//pkg:ok"],"lint":["//pkg:ok"],"unit":[],"build_binary":false}
{"lint":["//pkg:ok"],"unit":["//pkg:bad\n//:injected"],"build_binary":true}
{"lint":["//pkg:../bad"],"unit":[],"build_binary":false}
{"lint":[1],"unit":[],"build_binary":false}
{"lint":["//pkg:ok"],"unit":[],"build_binary":"false"}
{"lint":["//pkg:ok"],"unit":["//pkg:ok_unit_test"],"build_binary":false}
not json
EOF
export MANIFEST='{"lint":["//:root_clippy_test"],"unit":[],"build_binary":true}'
for failure in FAIL_LINT FAIL_SELECTOR FAIL_BAZEL; do
  : > "$LOG"
  if env "$failure=1" "$script" base head; then
    echo "$failure was ignored" >&2
    exit 1
  fi
done
if "$script" base; then
  echo 'Missing argument was accepted' >&2
  exit 1
fi
if [[ -n $(find "$TMPDIR" -mindepth 1 -print -quit) ]]; then
  echo 'Temporary files were not cleaned up' >&2
  exit 1
fi
