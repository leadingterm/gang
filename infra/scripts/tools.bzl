"""Expose the devShell's pinned import-test tools to Bazel tests."""

def _devshell_import_tools_impl(rctx):
    tools = rctx.getenv("GANG_IMPORT_TEST_TOOLS")
    if not tools:
        fail("GANG_IMPORT_TEST_TOOLS is unset: run Bazel inside `nix develop`")
    directory = rctx.path(tools)
    if not directory.exists or not directory.is_dir:
        fail("GANG_IMPORT_TEST_TOOLS is not a directory: " + tools + "; run Bazel inside `nix develop`")
    for name in ["jq", "zstd"]:
        program = directory.get_child(name)
        if not program.exists:
            fail("GANG_IMPORT_TEST_TOOLS is missing " + name + ": " + str(program) + "; run Bazel inside `nix develop`")
        if program.is_dir:
            fail("GANG_IMPORT_TEST_TOOLS names a directory for " + name + ": " + str(program) + "; run Bazel inside `nix develop`")
        rctx.symlink(program, name)
    rctx.file("BUILD.bazel", """exports_files(["jq", "zstd"], visibility = ["//visibility:public"])
""")

devshell_import_tools = repository_rule(
    implementation = _devshell_import_tools_impl,
    doc = "Exposes jq and zstd from GANG_IMPORT_TEST_TOOLS in the devShell as public files (rctx.getenv makes the variable a fetch input).",
)
