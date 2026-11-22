"""Exercise PR selection against a real Git history and Bazel query graph."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SELECTOR = Path(__file__).with_name("ci_pr_targets.py").resolve()


class SelectorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        self = cls
        git_environment = patch.dict(os.environ, {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": "0",
        })
        git_environment.start()
        self.addClassCleanup(git_environment.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="gang-pr-targets-")
        self.addClassCleanup(self.temp.cleanup)
        temporary = Path(self.temp.name)
        self.root = temporary / "workspace"
        self.root.mkdir()
        bazel = os.environ.get("BAZEL", "bazel")
        self.bazel = str(Path(shutil.which(bazel) or bazel).resolve())
        self.wrapper = temporary / "bazel"
        self.wrapper.write_text(
            '#!/bin/sh\nexec "$CI_PR_FIXTURE_BAZEL" --max_idle_secs=10 '
            '--output_base="$CI_PR_FIXTURE_OUTPUT" --nosystem_rc --nohome_rc "$@"\n',
            encoding="utf-8",
        )
        self.wrapper.chmod(0o755)
        self.output_base = temporary / "bazel-output"
        self.git("init", "-q")
        self.git("config", "user.email", "fixture@example.com")
        self.git("config", "user.name", "Fixture")
        self.put("BUILD.bazel", "")
        self.put(".bazelversion", (SELECTOR.parent.parent / ".bazelversion").read_text(encoding="utf-8"))
        self.put(".bazelrc", "common --incompatible_autoload_externally=\n")
        self.put("MODULE.bazel", 'module(name = "fixture")\n')
        self.addClassCleanup(self.shutdown_bazel)
        self.put("defs.bzl", '''def _impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(executable, "#!/bin/sh\\nexit 0\\n", is_executable = True)
    return [DefaultInfo(executable = executable)]

fixture_test = rule(implementation = _impl, attrs = {"deps": attr.label_list()}, test = True)
''')
        self.put("shared/BUILD.bazel", '''filegroup(name = "lib", srcs = ["src/lib.rs"], visibility = ["//visibility:public"])
''')
        self.put("shared/src/lib.rs", "shared\n")
        self.put("app/BUILD.bazel", '''load("//:defs.bzl", "fixture_test")
filegroup(name = "lib", srcs = ["src/main.rs", "//shared:lib"])
fixture_test(name = "app_unit_test", deps = [":lib"])
fixture_test(name = "manual_unit_test", deps = [":lib"], tags = ["manual"])
fixture_test(name = "hidden_unit_test", deps = [":lib"], tags = ["manual"])
test_suite(name = "visible_suite", tests = [":manual_unit_test"])
fixture_test(name = "app_integration_test", deps = [":lib"])
fixture_test(name = "app_doc_test", deps = [":lib"])
fixture_test(name = "app_clippy_test", deps = [":lib"])
fixture_test(name = "app_rustfmt_test", deps = [":lib"])
''')
        self.put("app/src/main.rs", "app\n")
        self.put("other/BUILD.bazel", '''load("//:defs.bzl", "fixture_test")
fixture_test(name = "other_unit_test", deps = ["data.rs", "src/README.rs", "src/README.md"])
fixture_test(name = "other_clippy_test")
''')
        self.put("other/data.rs", "other\n")
        self.put("other/src/README.rs", "source\n")
        self.put("other/src/README.md", "data\n")
        self.put("ui/BUILD.bazel", 'exports_files(["package.json", "pnpm-lock.yaml"])\n')
        self.put("ui/package.json", "{}\n")
        self.put("ui/pnpm-lock.yaml", "lockfileVersion: '9.0'\n")
        self.put("docs/guide.md", "guide\n")
        self.put("docs/API.md", "api\n")
        self.put("docs/nested/API.md", "ordinary documentation\n")
        self.put("notes/PLAN.md", "ordinary notes\n")
        self.put("README.md", "readme\n")
        self.git("add", "-A")
        self.git("commit", "-qm", "baseline")
        self.base = self.git("rev-parse", "HEAD").strip()

    def setUp(self):
        self.git("checkout", "-q", "-b", f"case-{self._testMethodName}", self.base)

    def tearDown(self):
        self.git("add", "-A")
        self.git("commit", "--allow-empty", "-qm", "fixture cleanup")
        self.git("checkout", "-q", "--detach", self.base)

    @classmethod
    def put(cls, name, content):
        path = cls.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    @classmethod
    def git(cls, *args):
        return subprocess.run(["git", *args], cwd=cls.root, check=True,
                              stdout=subprocess.PIPE, text=True).stdout

    @classmethod
    def fixture_environment(cls):
        return {
            **os.environ,
            "BAZEL": str(cls.wrapper),
            "CI_PR_FIXTURE_BAZEL": cls.bazel,
            "CI_PR_FIXTURE_OUTPUT": str(cls.output_base),
        }

    @classmethod
    def shutdown_bazel(cls):
        subprocess.run(
            [str(cls.wrapper), "shutdown"], cwd=cls.root,
            env=cls.fixture_environment(), check=True, timeout=60,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    def select(self, env=None, base=None, head="HEAD"):
        self.git("add", "-A")
        self.git("commit", "--allow-empty", "-qm", "change")
        selector_env = self.fixture_environment()
        if env is not None:
            selector_env.update(env)
        result = subprocess.run(
            [sys.executable, str(SELECTOR), base or self.base, head],
            cwd=self.root, env=selector_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        return result

    def manifest(self):
        result = self.select()
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def assert_lint(self, manifest):
        self.assertEqual(manifest["lint"], ["//app:app_clippy_test", "//app:app_rustfmt_test", "//other:other_clippy_test"])

    def assert_all_units(self, manifest):
        self.assert_lint(manifest)
        self.assertEqual(manifest["unit"], ["//app:app_unit_test", "//app:manual_unit_test", "//other:other_unit_test"])

    def test_shared_source_reaches_downstream_units_and_visible_manual_child(self):
        self.put("shared/src/lib.rs", "changed\n")
        manifest = self.manifest()
        self.assert_lint(manifest)
        self.assertEqual(manifest["unit"], ["//app:app_unit_test", "//app:manual_unit_test"])

    def test_nearest_package_source_excludes_unrelated_units(self):
        self.put("other/data.rs", "changed\n")
        manifest = self.manifest()
        self.assert_lint(manifest)
        self.assertEqual(manifest["unit"], ["//other:other_unit_test"])

    def test_readme_rust_source_selects_affected_unit(self):
        self.put("other/src/README.rs", "changed\n")
        manifest = self.manifest()
        self.assert_lint(manifest)
        self.assertEqual(manifest["unit"], ["//other:other_unit_test"])

    def test_nested_readme_data_selects_affected_unit(self):
        self.put("other/src/README.md", "changed\n")
        manifest = self.manifest()
        self.assert_lint(manifest)
        self.assertEqual(manifest["unit"], ["//other:other_unit_test"])

    def test_ordinary_docs_select_only_lint(self):
        self.put("docs/guide.md", "changed\n")
        self.put("README.md", "changed\n")
        self.put("docs/nested/API.md", "changed\n")
        self.put("notes/PLAN.md", "changed\n")
        manifest = self.manifest()
        self.assert_lint(manifest)
        self.assertEqual(manifest["unit"], [])

    def test_deleted_source_selects_all_units(self):
        (self.root / "shared/src/lib.rs").unlink()
        self.assert_all_units(self.manifest())

    def test_renamed_source_selects_all_units(self):
        (self.root / "shared/src/lib.rs").rename(self.root / "shared/src/renamed.rs")
        self.assert_all_units(self.manifest())

    def test_graph_input_selects_all_units(self):
        self.put("MODULE.bazel", 'module(name = "fixture", version = "1")\n')
        self.assert_all_units(self.manifest())

    def test_ui_package_manifest_selects_all_units(self):
        self.put("ui/package.json", '{"name": "fixture"}\n')
        self.assert_all_units(self.manifest())

    def test_ui_lockfile_selects_all_units(self):
        self.put("ui/pnpm-lock.yaml", "lockfileVersion: '9.0'\nimporters: {}\n")
        self.assert_all_units(self.manifest())

    def test_unknown_file_selects_all_units(self):
        self.put("app/unowned.rs", "unknown\n")
        self.assert_all_units(self.manifest())

    def test_api_document_is_not_ignored(self):
        self.put("docs/API.md", "changed\n")
        self.assert_all_units(self.manifest())

    def test_hostile_path_selects_all_units(self):
        self.put("app/evil) union //....rs", "unknown\n")
        self.assert_all_units(self.manifest())

    def test_invalid_git_ref_fails_closed(self):
        result = self.select(base="not-a-real-ref")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")

    def test_option_shaped_base_fails_closed(self):
        result = self.select(base="--name-only")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")

    def test_option_shaped_head_fails_closed(self):
        result = self.select(head="--name-only")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")

    def test_failed_query_fails_closed(self):
        self.put("shared/src/lib.rs", "changed\n")
        result = self.select(env={**os.environ, "BAZEL": str(self.root / "missing-bazel")})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
