#!/usr/bin/env bash
# Exercise deterministic target partitioning through the helper's public interface.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
helper="$script_dir/ci-targets.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export LC_ALL=C
failures=0
cases=0

expect_output() {
  local description=$1 input=$2 expected=$3
  shift 3
  cases=$((cases + 1))
  if ! "$helper" "$@" <"$input" >"$scratch/output" 2>"$scratch/error" ||
    ! cmp -s "$expected" "$scratch/output"; then
    echo "FAIL: $description" >&2
    failures=$((failures + 1))
  fi
}

expect_failure() {
  local description=$1 input=$2
  shift 2
  cases=$((cases + 1))
  if "$helper" "$@" <"$input" >"$scratch/output" 2>"$scratch/error"; then
    echo "FAIL: $description" >&2
    failures=$((failures + 1))
  fi
}

printf '%s\n' '//gamma:g' '@other//pkg:f' '//beta:b' '//alpha:a' \
  '@ext//pkg:e' '//delta:d' '//beta:b' '//alpha:a' >"$scratch/input"
printf '%s\n' '//alpha:a' '//beta:b' '//delta:d' '//gamma:g' \
  '@ext//pkg:e' '@other//pkg:f' >"$scratch/all"
printf '%s\n' '//alpha:a' '//gamma:g' >"$scratch/expected1"
printf '%s\n' '//beta:b' '@ext//pkg:e' >"$scratch/expected2"
printf '%s\n' '//delta:d' '@other//pkg:f' >"$scratch/expected3"
: >"$scratch/empty"

expect_output 'single shard sorts and deduplicates' "$scratch/input" "$scratch/all" 1 1
for index in 1 2 3; do
  expect_output "shard $index follows sorted-row modulo" "$scratch/input" "$scratch/expected$index" "$index" 3
  # Capture real output separately for repeatability and union checks.
  if ! "$helper" "$index" 3 <"$scratch/input" >"$scratch/shard$index" 2>"$scratch/error"; then
    echo "FAIL: collecting shard $index" >&2
    failures=$((failures + 1))
  fi
  cases=$((cases + 1))
  expect_output "shard $index repeats identically" "$scratch/input" "$scratch/shard$index" "$index" 3
done
cat "$scratch/shard1" "$scratch/shard2" "$scratch/shard3" | sort >"$scratch/union"
cases=$((cases + 1))
if ! cmp -s "$scratch/all" "$scratch/union"; then
  echo 'FAIL: shard union must contain each label exactly once' >&2
  failures=$((failures + 1))
fi

expect_output 'empty input' "$scratch/empty" "$scratch/empty" 1 3
expect_output 'more shards than labels yields empty shard' "$scratch/input" "$scratch/empty" 7 8
expect_output 'last excess shard is empty' "$scratch/input" "$scratch/empty" 8 8
printf '%s\n' '@other//pkg:f' >"$scratch/last-label"
expect_output 'last populated shard retains its label' "$scratch/input" "$scratch/last-label" 6 8

expect_failure 'missing both arguments' "$scratch/input"
expect_failure 'missing count' "$scratch/input" 1
expect_failure 'extra argument' "$scratch/input" 1 3 extra
expect_failure 'nonnumeric index' "$scratch/input" x 3
expect_failure 'nonnumeric count' "$scratch/input" 1 x
expect_failure 'zero index' "$scratch/input" 0 3
expect_failure 'zero count' "$scratch/input" 1 0
expect_failure 'negative index' "$scratch/input" -1 3
expect_failure 'negative count' "$scratch/input" 1 -3
expect_failure 'index exceeds count' "$scratch/input" 4 3

printf '\n' >"$scratch/invalid"
expect_failure 'blank label' "$scratch/invalid" 1 1
printf '  \t\n' >"$scratch/invalid"
expect_failure 'whitespace-only label' "$scratch/invalid" 1 1
printf '//alpha:a\n\n//beta:b\n' >"$scratch/invalid"
expect_failure 'blank line among labels' "$scratch/invalid" 1 1
printf '//alpha:a\nmalformed\n' >"$scratch/invalid"
expect_failure 'newline splitting a malformed label' "$scratch/invalid" 1 1
printf '//alpha:with space\n' >"$scratch/invalid"
expect_failure 'whitespace within label' "$scratch/invalid" 1 1
printf ' //alpha:a\n' >"$scratch/invalid"
expect_failure 'leading whitespace' "$scratch/invalid" 1 1
printf '//alpha:a\t\n' >"$scratch/invalid"
expect_failure 'trailing whitespace' "$scratch/invalid" 1 1
printf '//alpha:a\r\n' >"$scratch/invalid"
expect_failure 'carriage return in label' "$scratch/invalid" 1 1
printf 'alpha:target\n' >"$scratch/invalid"
expect_failure 'label without required prefix' "$scratch/invalid" 1 1
printf '//alpha\n' >"$scratch/invalid"
expect_failure 'workspace label without colon' "$scratch/invalid" 1 1
printf '@repo//alpha\n' >"$scratch/invalid"
expect_failure 'external label without colon' "$scratch/invalid" 1 1

mkdir -p "$scratch/bin" "$scratch/runner"
cat >"$scratch/bin/bazel" <<'WRAPPER'
#!/usr/bin/env bash
exec "$CI_QUERY_BAZEL" "$@"
WRAPPER
chmod +x "$scratch/bin/bazel"
cases=$((cases + 1))
# Real Bazel validates manual and suite wildcard semantics, not just workflow text.
if ! (
  cd "$script_dir/.." || exit 1
  CI_QUERY_BAZEL=$(command -v "${BAZEL:-bazel}") || exit 1
  export CI_QUERY_BAZEL
  yq -er '.jobs.linux-x86_64-shards.steps[] | select(.name == "Select shard targets") | .run' \
    .github/workflows/ci.yml >"$scratch/workflow-step" || exit 1
  PATH="$scratch/bin:$PATH" SHARD=1 RUNNER_TEMP="$scratch/runner" \
    bash -euo pipefail "$scratch/workflow-step" || exit 1
  [[ -s "$scratch/runner/targets" ]]
); then
  echo 'FAIL: actual workflow selects nonempty shard targets' >&2
  failures=$((failures + 1))
fi

mkdir -p "$scratch/fixture"
cat >"$scratch/fixture/MODULE.bazel" <<'EOF'
module(name = "ci_targets_fixture")
EOF
printf '%s\n' 'common --incompatible_autoload_externally=' >"$scratch/fixture/.bazelrc"
cat >"$scratch/fixture/test_rule.bzl" <<'EOF'
def _impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(executable, "#!/bin/sh\nexit 0\n", is_executable = True)
    return [DefaultInfo(executable = executable)]

fixture_test = rule(implementation = _impl, test = True)
EOF
cat >"$scratch/fixture/BUILD.bazel" <<'EOF'
load(":test_rule.bzl", "fixture_test")

fixture_test(name = "normal_test")
fixture_test(name = "manual_standalone", tags = ["manual"])
fixture_test(name = "visible_suite_child", tags = ["manual"])
fixture_test(name = "manual_suite_child", tags = ["manual"])
test_suite(name = "visible_suite", tests = [":visible_suite_child"])
test_suite(name = "manual_suite", tags = ["manual"], tests = [":manual_suite_child"])
genrule(name = "build_only", outs = ["built.txt"], cmd = "touch $@")
EOF
printf '%s\n' '//:build_only' '//:normal_test' '//:visible_suite_child' >"$scratch/fixture-expected"
cat >"$scratch/bin/fixture-bazel" <<'WRAPPER'
#!/usr/bin/env bash
exec "$CI_QUERY_BAZEL" --batch --output_base="$CI_FIXTURE_OUTPUT" --nosystem_rc --nohome_rc "$@"
WRAPPER
chmod +x "$scratch/bin/fixture-bazel"
cases=$((cases + 1))
if ! (
  cd "$script_dir/.." || exit 1
  CI_QUERY_BAZEL=$(command -v "${BAZEL:-bazel}") || exit 1
  CI_FIXTURE_OUTPUT="$scratch/fixture-output"
  export CI_QUERY_BAZEL CI_FIXTURE_OUTPUT
  cd "$scratch/fixture" || exit 1
  BAZEL="$scratch/bin/fixture-bazel" "$helper" --query >"$scratch/fixture-actual" || exit 1
  sort "$scratch/fixture-actual" >"$scratch/fixture-sorted"
  cmp -s "$scratch/fixture-expected" "$scratch/fixture-sorted"
); then
  echo 'FAIL: query selects visible tests and build targets without manual-only targets or suites' >&2
  failures=$((failures + 1))
fi

# Use pinned Bazel to exercise the production aspect's action selection.
mkdir -p "$scratch/compilation-fixture"
cp "$script_dir/../.bazelversion" "$scratch/compilation-fixture/.bazelversion"
cp "$script_dir/../bazel/rust/unit_compilation.bzl" "$scratch/compilation-fixture/unit_compilation.bzl"
cat >"$scratch/compilation-fixture/MODULE.bazel" <<'EOF'
module(name = "unit_compilation_fixture")
EOF
printf '%s\n' 'common --incompatible_autoload_externally=' >"$scratch/compilation-fixture/.bazelrc"
cat >"$scratch/compilation-fixture/test_rule.bzl" <<'EOF'
def _impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    if ctx.attr.fail_compilation:
        ctx.actions.run_shell(
            outputs = [executable],
            command = "echo unexpected-root-compilation >&2; exit 1",
        )
    else:
        ctx.actions.write(
            executable,
            "#!/bin/sh\nprintf '%s\\n' unit-compilation-fixture\n",
            is_executable = True,
        )
    return [DefaultInfo(
        files = depset([executable]),
        executable = executable,
        runfiles = ctx.runfiles(transitive_files = depset(transitive = [
            target[DefaultInfo].files
            for target in ctx.attr.data
        ])),
    )]

_attrs = {
    "crate": attr.label(),
    "data": attr.label_list(),
    "fail_compilation": attr.bool(),
}

rust_test = rule(implementation = _impl, attrs = _attrs, test = True)
fixture_binary = rule(implementation = _impl, attrs = _attrs, executable = True)
EOF
cat >"$scratch/compilation-fixture/BUILD.bazel" <<'EOF'
load(":test_rule.bzl", "fixture_binary", "rust_test")

filegroup(name = "crate")
genrule(
    name = "runtime_data",
    outs = ["runtime_data.txt"],
    cmd = "echo unit-runtime-data-required >&2; exit 1",
)
rust_test(name = "unit", crate = ":crate", data = [":runtime_data"])
rust_test(name = "integration", fail_compilation = True)
fixture_binary(name = "nonunit", crate = ":crate", fail_compilation = True)
EOF

unit_compilation_bazel() (
  cd "$script_dir/.." || exit 1
  CI_QUERY_BAZEL=$(command -v "${BAZEL:-bazel}") || exit 1
  CI_FIXTURE_OUTPUT="$scratch/compilation-output"
  export CI_QUERY_BAZEL CI_FIXTURE_OUTPUT
  cd "$scratch/compilation-fixture" || exit 1
  "$scratch/bin/fixture-bazel" "$@"
)

cases=$((cases + 1))
if ! unit_compilation_bazel build \
  --aspects=//:unit_compilation.bzl%unit_compilation --output_groups=unit_compilation \
  //:unit //:integration //:nonunit >"$scratch/compilation-log" 2>&1 ||
  [[ ! -x "$scratch/compilation-fixture/bazel-bin/unit.sh" ]] ||
  [[ $("$scratch/compilation-fixture/bazel-bin/unit.sh") != unit-compilation-fixture ]]; then
  echo 'FAIL: mixed precompile builds the unit binary without runtime data or other roots' >&2
  cat "$scratch/compilation-log" >&2
  failures=$((failures + 1))
fi

for command in build test; do
  cases=$((cases + 1))
  if unit_compilation_bazel "$command" //:unit >"$scratch/compilation-log" 2>&1 ||
    ! grep -Fq 'unit-runtime-data-required' "$scratch/compilation-log"; then
    echo "FAIL: ordinary $command still requires unit runtime data" >&2
    cat "$scratch/compilation-log" >&2
    failures=$((failures + 1))
  fi
done

printf '%d cases, %d failures\n' "$cases" "$failures"
exit "$((failures > 0))"
