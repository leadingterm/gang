"""Repository exposing the action environment selected by Gang's Bazel rc."""

def _action_environment_impl(ctx):
    identity = ctx.os.environ.get("GANG_ACTION_ENVIRONMENT", "")
    properties = {"gang_action_environment": identity} if identity else {}
    ctx.file("defs.bzl", "EXEC_PROPERTIES = %s\n" % repr(properties))
    ctx.file("BUILD.bazel", """load(\":defs.bzl\", \"EXEC_PROPERTIES\")
platform(
    name = \"environment\",
    exec_properties = EXEC_PROPERTIES,
    visibility = [\"//visibility:public\"],
)
""")

action_environment_repository = repository_rule(
    implementation = _action_environment_impl,
    environ = ["GANG_ACTION_ENVIRONMENT"],
)
