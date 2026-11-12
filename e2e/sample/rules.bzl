"""The probe rule: one shell action that writes a `key=value` report."""

load(":config.bzl", "ACTION_PATH", "ATTEMPTS", "SALT", "SIGNAL")

def _probe_impl(ctx):
    report = ctx.actions.declare_file(ctx.label.name + ".report")
    ctx.actions.run_shell(
        outputs = [report],
        inputs = ctx.files.srcs,
        # The script's standard output becomes the report; the report exists only if the script succeeded.
        command = "{\n%s\n} > \"$E2E_OUT.partial\" && mv \"$E2E_OUT.partial\" \"$E2E_OUT\"" % ctx.attr.script,
        # The action sees exactly this environment; the harness stages the values per deployment.
        env = {
            "E2E_OUT": report.path,
            # The salt makes every run's actions distinct, so a run never hits an earlier run's cache.
            "E2E_SALT": SALT,
            "PATH": ACTION_PATH,
            "E2E_SIGNAL": SIGNAL,
            "E2E_ATTEMPTS": ATTEMPTS,
        },
        mnemonic = "E2eProbe",
        progress_message = "Probing %{label}",
    )
    return [DefaultInfo(files = depset([report]))]

probe = rule(
    implementation = _probe_impl,
    attrs = {
        "script": attr.string(mandatory = True, doc = "Shell script; its standard output is the report."),
        "srcs": attr.label_list(allow_files = True, doc = "Inputs of the action."),
    },
)
