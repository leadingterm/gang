import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import changelog


class ChangelogTest(unittest.TestCase):
    def setUp(self):
        git_environment = patch.dict(os.environ, {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": "0",
        })
        git_environment.start()
        self.addCleanup(git_environment.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="gang-changelog-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.com")
        self.put("CHANGELOG.md", "# Changelog\n\nArchive pointer.\n\n## Unreleased\n\n## 0.1.0 — 2020-01-01\n\nLegacy $text.\n")
        self.commit("archive")
        self.git("tag", "v0.1.0")

    def git(self, *args, env=None):
        # Real Git is the interface under test; temporary repositories require no network.
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env).stdout.decode().strip()

    def put(self, name, contents):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    def commit(self, message):
        self.git("add", "-A")
        env = dict(os.environ, GIT_AUTHOR_DATE="2024-01-02T23:30:00-02:00", GIT_COMMITTER_DATE="2024-01-02T23:30:00-02:00")
        self.git("commit", "-qm", message, env=env)
        return self.git("rev-parse", "HEAD")

    def call(self, *args):
        with contextlib.redirect_stderr(io.StringIO()):
            return changelog.main(["--repo", str(self.repo), *args])

    def rendered(self, tag, ref="HEAD"):
        output = self.repo / "generated.txt"
        section = self.repo / "section.txt"
        self.assertEqual(self.call("render", "--ref", ref, "--tag", tag, "--output", str(output), "--section-output", str(section)), 0)
        return output.read_text(), section.read_text()

    def test_initial_release_and_literal_markdown(self):
        self.git("tag", "-d", "v0.1.0")
        self.put("changes/z.md", '### Changed\n\n- Keep `$HOME` and "quotes".\n  Continuation with `x`.\n')
        self.put("changes/a.md", "### Added\n\n- First.\n")
        self.put("changes/b.md", "### Added\n\n- Second.\n")
        self.commit("first")
        self.git("tag", "v0.2.0")
        whole, current = self.rendered("v0.2.0")
        self.assertIn("## 0.2.0 — 2024-01-03", current)
        self.assertLess(current.index("### Added"), current.index("### Changed"))
        self.assertLess(current.index("First."), current.index("Second."))
        self.assertIn('- Keep `$HOME` and "quotes".\n  Continuation with `x`.', current)
        self.assertTrue(whole.startswith("# Changelog\n\n## 0.2.0"))
        self.assertEqual(whole.count("# Changelog\n"), 1)
        self.assertNotIn("Archive pointer.", whole)
        self.assertNotIn("## Unreleased", whole)
        self.assertTrue(whole.endswith("## 0.1.0 — 2020-01-01\n\nLegacy $text.\n"))

    def test_historical_releases_and_prerelease_baseline(self):
        self.put("changes/one.md", "### Added\n\n- One.\n")
        self.commit("one")
        self.git("tag", "v0.2.0")
        self.put("changes/two.md", "### Fixed\n\n- Two.\n")
        self.commit("two")
        self.git("tag", "v0.3.0-rc.1")
        _, candidate = self.rendered("v0.3.0-rc.1")
        self.assertIn("Two.", candidate)
        self.assertNotIn("One.", candidate)
        self.git("tag", "v0.3.0")
        self.put("changes/three.md", "### Changed\n\n- Three.\n")
        self.commit("three")
        self.git("tag", "v0.4.0")
        whole, current = self.rendered("v0.4.0")
        self.assertIn("Three.", current)
        self.assertNotIn("Two.", current)
        self.assertLess(whole.index("Three."), whole.index("Two."))
        self.assertLess(whole.index("Two."), whole.index("One."))
        self.assertLess(whole.index("One."), whole.index("Legacy $text."))
        self.assertEqual(whole.count("## 0.3.0 —"), 1)

    def test_empty_release_has_header_only(self):
        self.git("tag", "v0.2.0")
        _, current = self.rendered("v0.2.0")
        self.assertEqual(current, "## 0.2.0 — 2024-01-03\n")

    def test_nonempty_unreleased_is_rejected_without_losing_notes(self):
        archive = "# Changelog\n\n## Unreleased\n\n### Fixed\n\n- Important note.\n\n## 0.1.0\n\nLegacy.\n"
        self.put("CHANGELOG.md", archive)
        self.commit("unreleased notes")
        self.git("tag", "v0.2.0")
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            result = changelog.main(["--repo", str(self.repo), "render", "--ref", "HEAD", "--tag", "v0.2.0", "--output", str(self.repo / "out"), "--section-output", str(self.repo / "section")])
        self.assertEqual(result, 1)
        self.assertIn("nonempty Unreleased", error.getvalue())
        self.assertIn("before publishing", error.getvalue())
        self.assertEqual((self.repo / "CHANGELOG.md").read_text(), archive)
        self.assertFalse((self.repo / "out").exists())

    def test_archived_requested_version_is_reused(self):
        whole, current = self.rendered("v0.1.0")
        self.assertEqual(current, "## 0.1.0 — 2020-01-01\n\nLegacy $text.\n")
        self.assertEqual(whole, "# Changelog\n\n" + current)
        self.assertEqual(whole.count("## 0.1.0"), 1)

    def test_newer_archive_than_requested_tag_is_rejected(self):
        self.git("tag", "v0.0.9")
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            result = changelog.main(["--repo", str(self.repo), "render", "--ref", "HEAD", "--tag", "v0.0.9", "--output", str(self.repo / "out"), "--section-output", str(self.repo / "section")])
        self.assertEqual(result, 1)
        self.assertIn("newer than", error.getvalue())

    def test_prerelease_at_archived_version_is_generated(self):
        self.git("tag", "v0.1.0-rc.1")
        whole, current = self.rendered("v0.1.0-rc.1")
        self.assertEqual(current, "## 0.1.0-rc.1 — 2024-01-03\n")
        self.assertIn("## 0.1.0 — 2020-01-01\n\nLegacy $text.\n", whole)

    def test_released_fragments_are_immutable_but_unreleased_are_editable(self):
        self.put("changes/released.md", "### Added\n\n- Original.\n")
        self.commit("released")
        self.git("tag", "v0.2.0")
        self.put("changes/draft.md", "### Changed\n\n- Draft.\n")
        self.assertEqual(self.call("check"), 0)
        self.put("changes/draft.md", "### Changed\n\n- Revised.\n")
        self.assertEqual(self.call("check"), 0)
        self.put("changes/released.md", "### Added\n\n- Edited.\n")
        self.assertEqual(self.call("check"), 1)
        (self.repo / "changes/released.md").unlink()
        self.assertEqual(self.call("check"), 1)

    def test_malformed_fragments(self):
        for body in ("", "### Added\n", "### Unknown\n- Item\n", "### Added\n- One\n### Added\n- Two\n", "### Added\n- \n", "- no heading\n", "### Fixed\nparagraph\n", "### Fixed\n- yes\n## Bad\n"):
            with self.subTest(body=body):
                self.put("changes/bad.md", body)
                self.assertEqual(self.call("check"), 1)
        self.put("changes/bad.md", "### Added\n- Valid\n### Fixed\n- Also valid\n")
        self.assertEqual(self.call("check"), 0)

    def test_ref_tag_and_shallow_errors(self):
        self.git("tag", "v0.2.0")
        self.put("changes/a.md", "### Added\n- A\n")
        self.commit("new")
        arguments = ("--output", str(self.repo / "out"), "--section-output", str(self.repo / "section"))
        for ref, tag in (("missing", "v0.2.0"), ("HEAD", "v0.2.0"), ("HEAD", "v01.2.3"), ("HEAD", "v9.9.9")):
            with self.subTest(ref=ref, tag=tag):
                self.assertEqual(self.call("render", "--ref", ref, "--tag", tag, *arguments), 1)
        self.assertEqual(self.call("check", "--ref", "missing"), 1)
        with tempfile.TemporaryDirectory(prefix="gang-changelog-shallow-") as clone:
            subprocess.run(["git", "clone", "--quiet", "--depth=1", self.repo.as_uri(), clone], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(changelog.main(["--repo", clone, "check"]), 1)
                self.assertEqual(changelog.main(["--repo", clone, "render", "--ref", "HEAD", "--tag", "v0.2.0", *arguments]), 1)

    def test_render_rejects_released_fragment_edits(self):
        self.put("changes/a.md", "### Added\n- A\n")
        self.commit("a")
        self.git("tag", "v0.2.0")
        self.put("changes/a.md", "### Added\n- Changed\n")
        self.commit("bad")
        self.git("tag", "v0.3.0")
        self.assertEqual(self.call("render", "--ref", "HEAD", "--tag", "v0.3.0", "--output", str(self.repo / "out"), "--section-output", str(self.repo / "section")), 1)


if __name__ == "__main__":
    unittest.main()
