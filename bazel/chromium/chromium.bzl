"""Expose the devShell's pinned Chromium and native DejaVu fonts to Bazel tests."""

def _devshell_chromium_impl(rctx):
    program = rctx.getenv("GANG_E2E_CHROMIUM")
    if program:
        if not rctx.path(program).exists:
            fail("GANG_E2E_CHROMIUM does not exist: " + program)
        if rctx.path(program).is_dir:
            fail("GANG_E2E_CHROMIUM names a directory, not the browser program: " + program)
        browser = rctx.path(program).dirname.get_child("chromium-bazel")
        if not browser.exists or browser.is_dir:
            fail("GANG_E2E_CHROMIUM sibling chromium-bazel does not exist or is a directory: " + str(browser))
        rctx.symlink(browser, "chromium-bin")
        fonts = rctx.path(program).dirname.dirname.get_child("share/fonts")
        for filename in ["DejaVuSans.ttf", "DejaVuSansMono.ttf"]:
            font = fonts.get_child(filename)
            if not font.exists or font.is_dir:
                fail("GANG_E2E_CHROMIUM native font file does not exist or is a directory: " + str(font))
            rctx.symlink(font, "fonts/" + filename)
        rctx.file("BUILD.bazel", """exports_files(["chromium-bin", "fonts/DejaVuSans.ttf", "fonts/DejaVuSansMono.ttf"], visibility = ["//visibility:public"])
filegroup(name = "chromium", srcs = ["chromium-bin"], visibility = ["//visibility:public"])
filegroup(name = "sans_font", srcs = ["fonts/DejaVuSans.ttf"], visibility = ["//visibility:public"])
filegroup(name = "mono_font", srcs = ["fonts/DejaVuSansMono.ttf"], visibility = ["//visibility:public"])
""")
    elif rctx.os.name.lower().startswith("linux"):
        fail("GANG_E2E_CHROMIUM is unset: run Bazel inside `nix develop`, whose Linux devShell pins the browser (flake.nix e2eChromium)")
    else:
        rctx.file("BUILD.bazel", """filegroup(name = "chromium", target_compatible_with = ["@platforms//:incompatible"], visibility = ["//visibility:public"])
filegroup(name = "sans_font", target_compatible_with = ["@platforms//:incompatible"], visibility = ["//visibility:public"])
filegroup(name = "mono_font", target_compatible_with = ["@platforms//:incompatible"], visibility = ["//visibility:public"])
""")

devshell_chromium = repository_rule(
    implementation = _devshell_chromium_impl,
    doc = "Exposes the Linux devShell's chromium-bazel sibling of GANG_E2E_CHROMIUM and its pinned native DejaVu Sans and Sans Mono TTF fonts as public filegroups. Linux requires the wrapper, browser, and font files; on other hosts without a browser, all three filegroups are incompatible so tests are skipped.",
)
