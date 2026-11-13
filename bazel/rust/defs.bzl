"""Shared defaults for first-party Rust packages."""

load("@gang_version//:version.bzl", "VERSION")
load("@rules_rust//cargo:defs.bzl", "extract_cargo_lints")
load(
    "@rules_rust//rust:defs.bzl",
    "rust_clippy_test",
    "rust_doc_test",
    "rust_proc_macro",
    "rust_test",
    "rust_test_suite",
    "rustfmt_test",
    _rust_binary = "rust_binary",
    _rust_library = "rust_library",
)
load("@rust_crates//:defs.bzl", "aliases", "all_crate_deps", "crate_edition")

def _unique(values):
    result = []
    seen = {}
    for value in values:
        if value not in seen:
            seen[value] = True
            result.append(value)
    return result

def _external(normal, dev):
    """Returns the Cargo-derived external crate wiring: `normal` and `dev` select the dependency kinds."""
    return struct(
        aliases = aliases(
            normal = normal,
            normal_dev = dev,
            proc_macro = normal,
            proc_macro_dev = dev,
        ),
        deps = all_crate_deps(normal = normal, normal_dev = dev),
        proc_macro_deps = all_crate_deps(proc_macro = normal, proc_macro_dev = dev),
    )

def _common(rustc_env):
    """Returns the attributes every crate-defining rule takes from the workspace."""
    return {
        "edition": crate_edition(),
        "lint_config": _cargo_lints(),
        "rustc_env": rustc_env,
        "version": VERSION,
    }

def _cargo_lints():
    """Declares the package's lint config, read from its Cargo.toml, once per package; returns its label."""
    if not native.existing_rule("cargo_lints"):
        extract_cargo_lints(
            name = "cargo_lints",
            manifest = "Cargo.toml",
            workspace = "//:Cargo.toml",
        )
    return ":cargo_lints"

def _manifest_env():
    return {"CARGO_MANIFEST_DIR": native.package_name()}

def _rust_library_sources(srcs):
    return srcs or native.glob(
        ["src/**/*.rs"],
        exclude = [
            "src/bin/**/*.rs",
            "src/main.rs",
        ],
    )

def _rust_binary_sources(src):
    """Returns the binary's sources: the crate root's directory tree, or the root alone beside a library, which owns the modules there."""
    if src == "src/main.rs" and native.glob(["src/lib.rs"], allow_empty = True):
        return [src]
    return native.glob([src.rpartition("/")[0] + "/**/*.rs"])

def _rust_test_data(test_data):
    return test_data or native.glob(
        [
            "Cargo.toml",
            "src/**/*.rs",
            "tests/fixtures/**",
        ],
        allow_empty = True,
    )

def _package_sources():
    """Declares the package's `workspace_sources` filegroup, which `workspace_rust_source_set` collects, once per package."""
    if not native.existing_rule("workspace_sources"):
        native.filegroup(
            name = "workspace_sources",
            srcs = native.glob([
                "Cargo.toml",
                "src/**/*.rs",
            ]) + native.glob(["tests/**/*.rs"], allow_empty = True),
            visibility = ["//visibility:public"],
        )

def workspace_rust_source_set(name, packages):
    native.filegroup(
        name = name,
        srcs = ["Cargo.toml"] + [
            "//{}:workspace_sources".format(package)
            for package in packages
        ],
        visibility = ["//visibility:public"],
    )

def _workspace_rust_unit_test(name, compile_data, data, deps, env, rustc_env):
    external = _external(normal = False, dev = True)
    rust_test(
        name = name + "_unit_test",
        size = "small",
        aliases = external.aliases,
        compile_data = compile_data,
        crate = ":" + name,
        data = data,
        deps = deps + external.deps,
        env = env,
        proc_macro_deps = external.proc_macro_deps,
        rustc_env = rustc_env,
        tags = ["rust"],
    )

def _workspace_rust_doc_test(name, deps):
    # Doc tests compile their examples inside the test action, so they are compile-bound.
    external = _external(normal = False, dev = True)
    rust_doc_test(
        name = name + "_doc_test",
        size = "medium",
        crate = ":" + name,
        deps = deps + external.deps,
        proc_macro_deps = external.proc_macro_deps,
        tags = ["rust"],
    )

def _workspace_rust_integration_tests(
        name,
        compile_data,
        data,
        deps,
        proc_macro_deps,
        env,
        rustc_env):
    integration_srcs = native.glob(["tests/*.rs"], allow_empty = True)
    if not integration_srcs:
        return []

    suite_name = name + "_integration_tests"
    external = _external(normal = True, dev = True)
    rust_test_suite(
        name = suite_name,
        size = "small",
        srcs = integration_srcs,
        shared_srcs = native.glob(
            ["tests/**/*.rs"],
            allow_empty = True,
            exclude = ["tests/*.rs"],
        ),
        aliases = external.aliases,
        compile_data = compile_data,
        data = data,
        deps = deps + external.deps,
        env = env,
        proc_macro_deps = proc_macro_deps + external.proc_macro_deps,
        tags = ["rust"],
        **_common(rustc_env)
    )
    return [
        ":{}_{}_test".format(suite_name, src[:-3])
        for src in integration_srcs
    ]

def _workspace_rust_lint_tests(name, targets):
    rust_clippy_test(
        name = name + "_clippy_test",
        size = "small",
        targets = targets,
        tags = ["clippy", "lint", "rust"],
    )
    rustfmt_test(
        name = name + "_rustfmt_test",
        size = "small",
        targets = targets,
        tags = ["lint", "rust", "rustfmt"],
    )

def workspace_rust_library(
        name,
        deps = None,
        dev_deps = None,
        proc_macro_deps = None,
        srcs = None,
        compile_data = None,
        test_data = None,
        test_env = None,
        test_runtime_data = None):
    """Declares a workspace library using Cargo-derived external metadata."""
    _package_sources()
    deps = _unique(deps or [])
    dev_deps = _unique(dev_deps or [])
    rustc_env = _manifest_env()
    test_data = _rust_test_data(test_data)
    runtime_data = test_data + (test_runtime_data or [])
    test_env = test_env or {}

    external = _external(normal = True, dev = False)
    _rust_library(
        name = name,
        srcs = _rust_library_sources(srcs),
        aliases = external.aliases,
        compile_data = compile_data or [],
        crate_name = name,
        proc_macro_deps = _unique(proc_macro_deps or []) + external.proc_macro_deps,
        visibility = ["//visibility:public"],
        deps = deps + external.deps,
        **_common(rustc_env)
    )

    _workspace_rust_unit_test(
        name = name,
        compile_data = test_data,
        data = runtime_data,
        deps = dev_deps,
        env = test_env,
        rustc_env = rustc_env,
    )
    _workspace_rust_doc_test(name = name, deps = dev_deps)
    integration_targets = _workspace_rust_integration_tests(
        name = name,
        compile_data = test_data,
        data = runtime_data,
        deps = _unique([":" + name] + deps + dev_deps),
        proc_macro_deps = _unique(proc_macro_deps or []),
        env = test_env,
        rustc_env = rustc_env,
    )
    _workspace_rust_lint_tests(
        name,
        [":" + name, ":" + name + "_unit_test"] + integration_targets,
    )

def workspace_rust_binary(
        name,
        deps = None,
        data = None,
        env = None,
        proc_macro_deps = None,
        src = "src/main.rs"):
    """Declares a workspace binary using Cargo-derived external metadata."""
    _package_sources()
    rustc_env = _manifest_env()
    test_data = _rust_test_data(None)
    external = _external(normal = True, dev = False)
    _rust_binary(
        name = name,
        srcs = _rust_binary_sources(src),
        crate_root = src,
        aliases = external.aliases,
        crate_name = name.replace("-", "_"),
        data = data or [],
        env = env or {},
        proc_macro_deps = _unique(proc_macro_deps or []) + external.proc_macro_deps,
        visibility = ["//visibility:public"],
        deps = _unique(deps or []) + external.deps,
        **_common(rustc_env)
    )

    _workspace_rust_unit_test(
        name = name,
        compile_data = test_data,
        data = test_data + (data or []),
        deps = [],
        env = env or {},
        rustc_env = rustc_env,
    )
    _workspace_rust_lint_tests(name, [":" + name, ":" + name + "_unit_test"])

def workspace_rust_proc_macro(name, deps = None):
    """Declares a workspace procedural macro using Cargo-derived metadata."""
    _package_sources()
    external = _external(normal = True, dev = False)
    rust_proc_macro(
        name = name,
        srcs = native.glob(["src/**/*.rs"]),
        aliases = external.aliases,
        crate_name = name,
        proc_macro_deps = external.proc_macro_deps,
        visibility = ["//visibility:public"],
        deps = _unique(deps or []) + external.deps,
        **_common(_manifest_env())
    )

    test_data = _rust_test_data(None)
    _workspace_rust_unit_test(
        name = name,
        compile_data = test_data,
        data = test_data,
        deps = [],
        env = {},
        rustc_env = _manifest_env(),
    )

    _workspace_rust_lint_tests(name, [":" + name, ":" + name + "_unit_test"])
