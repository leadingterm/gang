"""The probe rules: one shell action that writes a `key=value` report."""

load(":config.bzl", "ACTION_PATH", "ATTEMPTS", "SALT", "SIGNAL")

def _report(ctx, script, inputs, env):
    report = ctx.actions.declare_file(ctx.label.name + ".report")
    # The action sees exactly this environment; the harness stages the values per deployment.
    env_all = {
        "E2E_OUT": report.path,
        # The salt makes every run's actions distinct, so a run never hits an earlier run's cache.
        "E2E_SALT": SALT,
        # A held build sets its run with `--define=e2e_run=<run>`: its actions differ from every other
        # build's, and its journal lines start with it. Other builds leave it empty.
        "E2E_RUN": ctx.var.get("e2e_run", ""),
        "PATH": ACTION_PATH,
        "E2E_SIGNAL": SIGNAL,
        "E2E_ATTEMPTS": ATTEMPTS,
    }
    env_all.update(env)
    ctx.actions.run_shell(
        outputs = [report],
        inputs = inputs,
        # The script's standard output becomes the report; the report exists only if the script succeeded.
        command = "{\n%s\n} > \"$E2E_OUT.partial\" && mv \"$E2E_OUT.partial\" \"$E2E_OUT\"" % script,
        env = env_all,
        mnemonic = "E2eProbe",
        progress_message = "Probing %{label}",
    )
    return [DefaultInfo(files = depset([report]))]

def _probe_impl(ctx):
    return _report(ctx, ctx.attr.script, ctx.files.srcs, {})

def _input_root_probe_impl(ctx):
    shared = ctx.file.shared
    link = ctx.actions.declare_symlink(ctx.label.name + ".link")

    # Relative: up from the link's directory to the working directory, then along the input's
    # own path, which leaves the working directory under the sibling repository layout.
    ctx.actions.symlink(
        output = link,
        target_path = "../" * len(link.dirname.split("/")) + shared.path,
    )
    return _report(ctx, ctx.attr.script, [shared, link], {
        "E2E_LINK": link.path,
        "E2E_SHARED": shared.path,
    })

input_root_probe = rule(
    implementation = _input_root_probe_impl,
    doc = "A probe that also receives `E2E_SHARED`, the path of `shared`, and `E2E_LINK`, a relative symlink to it.",
    attrs = {
        "script": attr.string(mandatory = True, doc = "Shell script; its standard output is the report."),
        "shared": attr.label(mandatory = True, allow_single_file = True, doc = "The input read from beside the working directory."),
    },
)

probe = rule(
    implementation = _probe_impl,
    attrs = {
        "script": attr.string(mandatory = True, doc = "Shell script; its standard output is the report."),
        "srcs": attr.label_list(allow_files = True, doc = "Inputs of the action."),
    },
)
