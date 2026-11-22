"""OpenTofu contract tests backed by the rules_tf toolchain.

The test runs the Rust `opentofu-runner` binary from //bazel/opentofu/runner, which stages the
infra/ tree and runs fmt, init, validate and test for the root.
"""

def _opentofu_toolchain_file_impl(ctx):
    runtime = ctx.toolchains["@rules_tf//:tf_toolchain_type"].runtime
    selected = getattr(runtime, ctx.attr.file)
    return [DefaultInfo(
        files = depset([selected]),
        runfiles = ctx.runfiles(files = [selected] + (runtime.deps if ctx.attr.file == "tf" else [])),
    )]

opentofu_toolchain_file = rule(
    implementation = _opentofu_toolchain_file_impl,
    attrs = {
        "file": attr.string(
            mandatory = True,
            values = ["tf", "mirror"],
            doc = "The toolchain file to expose: the tofu binary (`tf`) or the provider mirror (`mirror`).",
        ),
    },
    toolchains = ["@rules_tf//:tf_toolchain_type"],
    doc = "Exposes one file of the rules_tf toolchain to the runner.",
)

def _upgrade_fixture_impl(ctx):
    runtime = ctx.toolchains["@rules_tf//:tf_toolchain_type"].runtime
    output = ctx.actions.declare_file(ctx.label.name + ".json")
    args = ctx.actions.args()
    args.add("--upgrade-fixture")
    args.add("--source-root", ctx.file.source_root.dirname or ".")
    args.add("--test-file", ctx.file.test_file.path)
    args.add("--tofu", runtime.tf.path)
    args.add("--provider-mirror", runtime.mirror.path)
    args.add("--output", output.path)
    ctx.actions.run(
        executable = ctx.executable._runner,
        arguments = [args],
        inputs = depset(ctx.files.srcs + [ctx.file.source_root, ctx.file.test_file, runtime.tf, runtime.mirror] + runtime.deps),
        tools = [ctx.attr._runner[DefaultInfo].files_to_run],
        outputs = [output],
        mnemonic = "UpgradeFixture",
    )
    return [DefaultInfo(files = depset([output]))]

upgrade_fixture = rule(
    implementation = _upgrade_fixture_impl,
    attrs = {
        "source_root": attr.label(mandatory = True, allow_single_file = True),
        "srcs": attr.label_list(allow_files = True),
        "test_file": attr.label(
            default = "//bazel/opentofu/runner:upgrade_test",
            allow_single_file = True,
        ),
        "_runner": attr.label(
            default = "//bazel/opentofu/runner:opentofu-runner",
            executable = True,
            cfg = "exec",
        ),
    },
    toolchains = ["@rules_tf//:tf_toolchain_type"],
)

def _opentofu_contract_test_impl(ctx):
    runtime = ctx.toolchains["@rules_tf//:tf_toolchain_type"].runtime

    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(
        output = executable,
        target_file = ctx.executable._runner,
        is_executable = True,
    )

    files = ctx.files.srcs + runtime.deps + [runtime.tf, runtime.mirror]
    environment = {
        "OPENTOFU_TOFU": runtime.tf.short_path,
        "OPENTOFU_PROVIDER_MIRROR": runtime.mirror.short_path,
        "OPENTOFU_ROOT": ctx.attr.root,
        "OPENTOFU_TEST_FILE": ctx.attr.test_file,
    }
    if ctx.file.test_expectations:
        files.append(ctx.file.test_expectations)
        environment["OPENTOFU_TEST_EXPECTATIONS"] = ctx.file.test_expectations.short_path
    files.extend(ctx.files.root_overlays)
    if ctx.files.root_overlays:
        environment["OPENTOFU_ROOT_OVERLAYS"] = json.encode([file.short_path for file in ctx.files.root_overlays])
    runfiles = ctx.runfiles(files = files)
    runfiles = runfiles.merge(ctx.attr._runner[DefaultInfo].default_runfiles)
    return [
        DefaultInfo(executable = executable, runfiles = runfiles),
        RunEnvironmentInfo(environment = environment),
    ]

_opentofu_contract_test = rule(
    implementation = _opentofu_contract_test_impl,
    test = True,
    attrs = {
        "root": attr.string(
            mandatory = True,
            doc = "Module or root directory, relative to the sources directory.",
        ),
        "test_file": attr.string(
            mandatory = True,
            doc = "Test file, relative to root.",
        ),
        "test_expectations": attr.label(
            allow_single_file = [".json"],
            doc = "Explicit JSON file specifying plan, state and retention expectations.",
        ),
        "root_overlays": attr.label_list(
            allow_files = [".tf"],
            doc = "New test-only *_test.tf and *_override.tf files copied into the disposable root.",
        ),
        "srcs": attr.label_list(
            allow_files = True,
            doc = "Runtime files from infra/, configs/, or //:Cargo.toml that the test needs.",
        ),
        "_runner": attr.label(
            default = "//bazel/opentofu/runner:opentofu-runner",
            executable = True,
            cfg = "target",
        ),
    },
    toolchains = ["@rules_tf//:tf_toolchain_type"],
)

def opentofu_contract_test(name, test_file = None, deps = [], test_expectations = None, root_overlays = []):
    """Runs `tofu fmt`, `init`, `validate` and `test` for the OpenTofu root in the current package.

    The package must live under infra/. The macro declares the public `sources` filegroup of the
    package once, so a package may hold several contract tests; other roots list it in their `deps`.

    Args:
        name: Test target name.
        test_file: Test file relative to the package; defaults to tests/<package name>.tftest.hcl.
        deps: Further files the root needs, such as module `sources`, files in configs/, or //:Cargo.toml.
        test_expectations: Optional JSON label containing test expectations.
        root_overlays: New test-only configuration runfiles installed only in the staged root.
    """
    if not native.existing_rule("sources"):
        native.filegroup(
            name = "sources",
            srcs = native.glob(["*.tf"]) + native.glob(["*.tftpl", "*.json"], allow_empty = True),
            visibility = ["//visibility:public"],
        )
    _opentofu_contract_test(
        name = name,
        root = native.package_name(),
        test_file = test_file if test_file != None else "tests/{}.tftest.hcl".format(native.package_name().split("/")[-1]),
        test_expectations = test_expectations,
        root_overlays = root_overlays,
        srcs = [":sources"] + deps + native.glob(["tests/*.tftest.hcl"]),
        # The watchdog only, never a verdict: long, so load does not kill the run.
        timeout = "long",
        size = "small",
        tags = ["opentofu"],
    )
