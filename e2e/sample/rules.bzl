"""The probe rules: one shell action that writes a `key=value` report."""

load(":config.bzl", "ACTION_IPC", "ACTION_PATH", "ACTION_SHELL", "ATTEMPTS", "HOST_ACTION_PATH", "HOST_ACTION_SHELL", "SALT", "SIGNAL")

def _report(ctx, script, inputs, env, host = False, controlled = False):
    report = ctx.actions.declare_file(ctx.label.name + ".report")
    # The action sees exactly this environment; the harness stages the values per deployment.
    env_all = {
        "E2E_OUT": report.path,
        "E2E_ACTION": ctx.label.name,
        # The salt makes every run's actions distinct, so a run never hits an earlier run's cache.
        "E2E_SALT": SALT,
        # A held build sets its run with `--define=e2e_run=<run>`: its actions differ from every other
        # build's, and its journal lines start with it. Other builds leave it empty.
        "E2E_RUN": ctx.var.get("e2e_run", ""),
        "PATH": HOST_ACTION_PATH if host else ACTION_PATH,
        "E2E_SIGNAL": SIGNAL,
        "E2E_ATTEMPTS": ATTEMPTS,
    }
    env_all.update(env)
    setup = ""
    if controlled and ACTION_IPC and not host:
        setup = """
ipc=".gang-ipc-${E2E_SALT}-${E2E_RUN:-none}-${E2E_ACTION}"
/bin/mkdir "$ipc" || exit 1
/bin/mkfifo "$ipc/ack" || exit 1
IFS= read -r ready < "$ipc/ack" || exit 1
[ "$ready" = ready ] || exit 1
export E2E_SIGNAL="$ipc/started" E2E_ATTEMPTS="$ipc/attempts"
"""
    ctx.actions.run(
        executable = HOST_ACTION_SHELL if host else ACTION_SHELL,
        arguments = ["-c", setup + "{\n%s\n} > \"$E2E_OUT.partial\" && mv \"$E2E_OUT.partial\" \"$E2E_OUT\"" % script],
        outputs = [report],
        inputs = inputs,
        env = env_all,
        mnemonic = "E2eProbe",
        progress_message = "Probing %{label}",
    )
    return [DefaultInfo(files = depset([report]))]

def _probe_impl(ctx):
    path = HOST_ACTION_PATH if ctx.attr.host else ACTION_PATH
    return _report(ctx, ctx.attr.script, ctx.files.srcs, {"PATH": ctx.attr.path or path}, host = ctx.attr.host, controlled = ctx.attr.controlled)

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
        "controlled": attr.bool(default = False, doc = "Requires host lifecycle signal, journal, or attempt control files."),
        "host": attr.bool(doc = "Run on the noop host environment rather than the default sandbox."),
        "path": attr.string(doc = "The action's PATH; empty: the harness's ACTION_PATH."),
        "srcs": attr.label_list(allow_files = True, doc = "Inputs of the action."),
    },
)

def _environment_witness_impl(ctx):
    witness = ctx.actions.declare_file(ctx.label.name + ".witness")
    marker = "declared-" + SALT
    ctx.actions.run(
        executable = ctx.file.probe,
        arguments = ["--gang-e2e-action-environment-probe", "environment", witness.path, marker],
        inputs = [ctx.file.probe],
        outputs = [witness],
        env = {"GANG_E2E_EXACT_ENV": marker},
        use_default_shell_env = False,
        mnemonic = "E2eExactEnvironment",
    )
    return [DefaultInfo(files = depset([witness]))]

environment_witness = rule(
    implementation = _environment_witness_impl,
    attrs = {"probe": attr.label(allow_single_file = True, mandatory = True)},
)
