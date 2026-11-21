"""Compatibility repository exporting empty execution properties."""

def _action_environment_impl(ctx):
    ctx.file("defs.bzl", "EXEC_PROPERTIES = {}\n")
    ctx.file("BUILD.bazel", """load(\":defs.bzl\", \"EXEC_PROPERTIES\")
platform(
    name = \"environment\",
    exec_properties = EXEC_PROPERTIES,
    visibility = [\"//visibility:public\"],
)
""")

action_environment_repository = repository_rule(
    implementation = _action_environment_impl,
)
