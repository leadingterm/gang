"""The external repository the input root probe reads from beside its working directory."""

def _shared_input_impl(rctx):
    rctx.file("BUILD.bazel", 'exports_files(["input"])\n')
    rctx.file("input", "shared input\n")

shared_input = repository_rule(
    implementation = _shared_input_impl,
    doc = "A repository holding one file, `input`.",
)
