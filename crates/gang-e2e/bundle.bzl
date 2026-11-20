"""Build a self-contained E2E bundle with upgrade inputs selected by role."""

def _copy(ctx, source, output):
    ctx.actions.run_shell(
        inputs = [source],
        outputs = [output],
        arguments = [source.path, output.path],
        command = "cp \"$1\" \"$2\"",
        mnemonic = "E2eBundleCopy",
        use_default_shell_env = True,
    )

def _e2e_bundle_impl(ctx):
    copies = [
        (ctx.file.harness, ctx.outputs.local),
        (ctx.file.gang, ctx.outputs.out_gang),
        (ctx.file.bazel, ctx.outputs.out_bazel),
        (ctx.file.postgres, ctx.outputs.out_postgres),
        (ctx.file.loaded_probe, ctx.outputs.out_loaded_probe),
        (ctx.file.previous_gang, ctx.outputs.out_previous_gang),
        (ctx.file.previous_fixture, ctx.outputs.out_previous_fixture),
        (ctx.file.historical_gang, ctx.outputs.out_historical_gang),
        (ctx.file.historical_fixture, ctx.outputs.out_historical_fixture),
        (ctx.file.candidate_fixture, ctx.outputs.out_candidate_fixture),
    ]
    calibration_binary = ctx.outputs.out_previous_gang.basename
    if ctx.file.calibration_gang == ctx.file.historical_gang:
        calibration_binary = ctx.outputs.out_historical_gang.basename
    elif ctx.file.calibration_gang != ctx.file.previous_gang:
        output = ctx.actions.declare_file(ctx.label.name + "/gang-calibration")
        copies.append((ctx.file.calibration_gang, output))
        calibration_binary = output.basename

    calibration_fixture = ctx.outputs.out_previous_fixture.basename
    if ctx.file.calibration_fixture == ctx.file.historical_fixture:
        calibration_fixture = ctx.outputs.out_historical_fixture.basename
    elif ctx.file.calibration_fixture != ctx.file.previous_fixture:
        output = ctx.actions.declare_file(ctx.label.name + "/deployment-calibration.json")
        copies.append((ctx.file.calibration_fixture, output))
        calibration_fixture = output.basename

    for source, output in copies:
        _copy(ctx, source, output)
    ctx.actions.write(ctx.outputs.manifest, json.encode({
        "previous": {"binary": ctx.outputs.out_previous_gang.basename, "fixture": ctx.outputs.out_previous_fixture.basename},
        "historical": {"binary": ctx.outputs.out_historical_gang.basename, "fixture": ctx.outputs.out_historical_fixture.basename},
        "calibration": {"binary": calibration_binary, "fixture": calibration_fixture},
        "candidate_fixture": ctx.outputs.out_candidate_fixture.basename,
    }))
    return [DefaultInfo(files = depset([output for _, output in copies] + [ctx.outputs.manifest]))]

e2e_bundle = rule(
    implementation = _e2e_bundle_impl,
    attrs = {
        name: attr.label(mandatory = True, allow_single_file = True)
        for name in [
            "harness",
            "gang",
            "bazel",
            "postgres",
            "loaded_probe",
            "previous_gang",
            "previous_fixture",
            "historical_gang",
            "historical_fixture",
            "calibration_gang",
            "calibration_fixture",
            "candidate_fixture",
        ]
    },
    outputs = {
        "local": "%{name}/local",
        "out_gang": "%{name}/gang",
        "out_bazel": "%{name}/bazel",
        "out_postgres": "%{name}/postgresql.tar.gz",
        "out_loaded_probe": "%{name}/loaded_probe",
        "out_previous_gang": "%{name}/gang-previous",
        "out_previous_fixture": "%{name}/deployment-previous.json",
        "out_historical_gang": "%{name}/gang-historical",
        "out_historical_fixture": "%{name}/deployment-historical.json",
        "out_candidate_fixture": "%{name}/deployment-candidate.json",
        "manifest": "%{name}/upgrade-inputs.json",
    },
)
