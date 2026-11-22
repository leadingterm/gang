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
        (ctx.file.v035_gang, ctx.outputs.out_v035_gang),
        (ctx.file.v035_fixture, ctx.outputs.out_v035_fixture),
        (ctx.file.v038_gang, ctx.outputs.out_v038_gang),
        (ctx.file.v038_fixture, ctx.outputs.out_v038_fixture),
        (ctx.file.candidate_fixture, ctx.outputs.out_candidate_fixture),
    ]
    for source, output in copies:
        _copy(ctx, source, output)
    ctx.actions.write(ctx.outputs.manifest, json.encode({
        "previous": {"binary": ctx.outputs.out_previous_gang.basename, "fixture": ctx.outputs.out_previous_fixture.basename},
        "v035": {"binary": ctx.outputs.out_v035_gang.basename, "fixture": ctx.outputs.out_v035_fixture.basename},
        "history": {"binary": ctx.outputs.out_v038_gang.basename, "fixture": ctx.outputs.out_v038_fixture.basename},
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
            "v035_gang",
            "v035_fixture",
            "v038_gang",
            "v038_fixture",
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
        "out_v035_gang": "%{name}/gang-v035",
        "out_v035_fixture": "%{name}/deployment-v035.json",
        "out_v038_gang": "%{name}/gang-v038",
        "out_v038_fixture": "%{name}/deployment-v038.json",
        "out_candidate_fixture": "%{name}/deployment-candidate.json",
        "manifest": "%{name}/upgrade-inputs.json",
    },
)
