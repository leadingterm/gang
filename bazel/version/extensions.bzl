"""Gang's version for Bazel: Cargo.toml's `[workspace.package] version` is its only owner."""

def _workspace_package_version(manifest):
    """Returns the `version` of the `[workspace.package]` table of a Cargo manifest."""
    in_table = False
    for line in manifest.splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_table = stripped == "[workspace.package]"
        elif in_table and stripped.startswith("version"):
            key, _, value = stripped.partition("=")
            if key.strip() == "version":
                return value.strip().strip('"')
    fail("//:Cargo.toml has no [workspace.package] version")

def _version_repo_impl(rctx):
    rctx.file("BUILD.bazel", "")
    rctx.file("version.bzl", "\"\"\"Cargo.toml's workspace version.\"\"\"\n\nVERSION = {}\n".format(repr(rctx.attr.version)))

_version_repo = repository_rule(
    implementation = _version_repo_impl,
    attrs = {"version": attr.string(mandatory = True)},
)

def _workspace_version_impl(mctx):
    manifest = mctx.read(mctx.path(Label("//:Cargo.toml")), watch = "yes")
    _version_repo(name = "gang_version", version = _workspace_package_version(manifest))

    # Reproducible: the result is a function of a watched source file, so MODULE.bazel.lock records nothing for it.
    return mctx.extension_metadata(reproducible = True)

workspace_version = module_extension(
    implementation = _workspace_version_impl,
    doc = "Exposes Cargo.toml's workspace version as `VERSION` in `@gang_version//:version.bzl`.",
)
