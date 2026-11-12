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

def _opentofu_contract_test_impl(ctx):
    runtime = ctx.toolchains["@rules_tf//:tf_toolchain_type"].runtime

    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(
        output = executable,
        target_file = ctx.executable._runner,
        is_executable = True,
    )

    runfiles = ctx.runfiles(files = ctx.files.srcs + runtime.deps + [runtime.tf, runtime.mirror])
    runfiles = runfiles.merge(ctx.attr._runner[DefaultInfo].default_runfiles)
    return [
        DefaultInfo(executable = executable, runfiles = runfiles),
        RunEnvironmentInfo(environment = {
            "OPENTOFU_TOFU": runtime.tf.short_path,
            "OPENTOFU_PROVIDER_MIRROR": runtime.mirror.short_path,
            "OPENTOFU_ROOT": ctx.attr.root,
            "OPENTOFU_TEST_FILE": ctx.attr.test_file,
        }),
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
        "srcs": attr.label_list(
            allow_files = True,
            doc = "Files of the infra/ tree that the test needs at runtime.",
        ),
        "_runner": attr.label(
            default = "//bazel/opentofu/runner:opentofu-runner",
            executable = True,
            cfg = "target",
        ),
    },
    toolchains = ["@rules_tf//:tf_toolchain_type"],
)

def opentofu_contract_test(name, test_file = None, deps = []):
    """Runs `tofu fmt`, `init`, `validate` and `test` for the OpenTofu root in the current package.

    The package must live under infra/. The macro declares the public `sources` filegroup of the
    package once, so a package may hold several contract tests; other roots list it in their `deps`.

    Args:
        name: Test target name.
        test_file: Test file relative to the package; defaults to tests/<package name>.tftest.hcl.
        deps: Further files of the infra/ tree the root needs, such as the `sources` of the modules it calls.
    """
    if not native.existing_rule("sources"):
        native.filegroup(
            name = "sources",
            srcs = native.glob(["*.tf"]) + native.glob(["*.tftpl"], allow_empty = True),
            visibility = ["//visibility:public"],
        )
    _opentofu_contract_test(
        name = name,
        root = native.package_name(),
        test_file = test_file if test_file != None else "tests/{}.tftest.hcl".format(native.package_name().split("/")[-1]),
        srcs = [":sources"] + deps + native.glob(["tests/*.tftest.hcl"]),
        # The watchdog only, never a verdict: long, so load does not kill the run.
        timeout = "long",
        size = "small",
        tags = ["opentofu"],
    )
