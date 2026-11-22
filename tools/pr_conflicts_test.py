import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import pr_conflicts as bot
from github_api import GitHubApiError


class GitFixtureTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gang-conflicts-")
        self.old_cwd = os.getcwd()
        os.chdir(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(os.chdir, self.old_cwd)
        environment = mock.patch.dict(os.environ, {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": "0",
        })
        environment.start()
        self.addCleanup(environment.stop)
        # Git is the subject's actual merge engine; the repository is isolated and offline.
        self.git("init", "-q")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "commit.gpgsign", "false")
        Path("content").write_text("original\n")
        Path("deleted").write_text("original\n")
        self.git("add", ".")
        self.git("commit", "-qm", "initial")
        self.root = self.git("rev-parse", "HEAD")

    def git(self, *args):
        return subprocess.run(["git", *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode().strip()

    def commit(self, name):
        self.git("add", "-A")
        self.git("commit", "-qm", name)
        return self.git("rev-parse", "HEAD")

    def test_real_merge_conflicts_and_unusual_paths(self):
        unusual = 'strange\n<name>`\\file'
        Path("content").write_text("main\n")
        Path("deleted").unlink()
        Path(unusual).write_text("main\n")
        base = self.commit("main")
        self.git("checkout", "-q", "--detach", self.root)
        Path("content").write_text("head\n")
        Path("deleted").write_text("head\n")
        Path(unusual).write_text("head\n")
        head = self.commit("head")
        self.assertEqual(set(bot.conflict_paths(base, head)), {"content", "deleted", unusual})
        body = bot.comment_body(base, head, [unusual])
        self.assertIn(r"\n&lt;name&gt;", body)
        self.assertNotIn("\n<name>", body)
        self.assertEqual(bot.conflict_paths(self.root, base), [])

    def test_fetch_validates_the_real_pr_ref(self):
        self.git("update-ref", "refs/pull/42/head", self.root)
        self.git("remote", "add", "origin", self.temp.name)
        self.assertTrue(bot.fetch_head(bot.PullRequest(42, self.root)))
        self.assertFalse(bot.fetch_head(bot.PullRequest(42, "0" * 40)))

    def test_invalid_refs_and_git_errors_fail(self):
        with self.assertRaises(bot.ConflictError):
            bot.conflict_paths("-bad", self.root)
        with self.assertRaises(bot.ConflictError):
            bot.conflict_paths("0" * 40, self.root)


BASE = "a" * 40
HEAD = "b" * 40
OTHER = "c" * 40


class FakeApi:
    repository = "example/repo"

    def __init__(self):
        self.base = BASE
        self.prs = {1: HEAD, 2: HEAD}
        self.states = {1: "open", 2: "open"}
        self.bases = {1: "main", 2: "feature-branch"}
        self.labels = {1: [], 2: []}
        self.comments = {1: [], 2: []}
        self.label_exists = False
        self.writes = []
        self.failure = None
        self.fresh_calls = 0
        self.change_on_fresh = None

    def pages(self, route):
        if self.failure == route:
            raise GitHubApiError("offline failure", status=500)
        if route == "/repos/example/repo/pulls?state=open&per_page=100":
            # Two pages, including a draft and a PR targeting a non-main branch.
            yield [{"number": 1, "head": {"sha": self.prs[1]}, "draft": True}]
            yield [{"number": 2, "head": {"sha": self.prs[2]}}]
        elif "/labels?" in route:
            number = int(route.split("/issues/")[1].split("/")[0])
            yield [{"name": value} for value in self.labels[number]]
        elif "/comments?" in route:
            number = int(route.split("/issues/")[1].split("/")[0])
            yield self.comments[number][:1]
            yield self.comments[number][1:]
        else:
            raise AssertionError(route)

    def request(self, method, route, payload=None):
        if self.failure == route:
            raise GitHubApiError("offline failure", status=500)
        if method == "GET" and route.endswith("/commits/main"):
            self.fresh_calls += 1
            if self.change_on_fresh == self.fresh_calls:
                self.base = OTHER
            return {"sha": self.base}
        if method == "GET" and "/pulls/" in route:
            number = int(route.rsplit("/", 1)[1])
            return {"number": number, "head": {"sha": self.prs[number]}, "state": self.states[number], "base": {"ref": self.bases[number]}}
        if method == "GET" and route.endswith("/labels/needs-rebase"):
            if not self.label_exists:
                raise GitHubApiError("missing", status=404)
            return {"name": bot.LABEL}
        self.writes.append((method, route, payload))
        if method == "POST" and route.endswith("/labels") and "/issues/" not in route:
            self.label_exists = True
        elif "/issues/comments/" in route:
            comment_id = int(route.rsplit("/", 1)[1])
            for comments in self.comments.values():
                for comment in list(comments):
                    if comment["id"] == comment_id:
                        if method == "DELETE":
                            comments.remove(comment)
                        else:
                            comment["body"] = payload["body"]
                        return {}
            raise AssertionError(route)
        else:
            number = int(route.split("/issues/")[1].split("/")[0])
            if route.endswith("/comments"):
                self.comments[number].append({"id": 100 + number, "body": payload["body"], "user": {"login": bot.BOT}})
            elif method == "POST" and route.endswith("/labels"):
                self.labels[number].extend(payload["labels"])
            elif method == "DELETE" and route.endswith("/labels/needs-rebase"):
                self.labels[number].remove(bot.LABEL)
            else:
                raise AssertionError((method, route))
        return {}


class ReconcileTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeApi()
        self.pr = bot.PullRequest(1, HEAD)
        self.user_comment = {"id": 1, "body": bot.MARKER + " user text", "user": {"login": "person"}}
        self.api.comments[1].append(self.user_comment)
        self.api.labels[1].append("other")

    def test_create_idempotent_update_and_clean(self):
        bot.reconcile(self.api, BASE, self.pr, ["one"])
        self.assertEqual(self.api.labels[1], ["other", bot.LABEL])
        self.assertEqual(len(self.api.comments[1]), 2)
        writes = len(self.api.writes)
        bot.reconcile(self.api, BASE, self.pr, ["one"])
        self.assertEqual(len(self.api.writes), writes)
        bot.reconcile(self.api, BASE, self.pr, ["two"])
        self.assertIn("two", self.api.comments[1][1]["body"])
        bot.reconcile(self.api, BASE, self.pr, [])
        self.assertEqual(self.api.labels[1], ["other"])
        self.assertEqual(self.api.comments[1], [self.user_comment])

    def test_duplicate_bot_comments_are_removed(self):
        for number in (20, 21):
            self.api.comments[1].append({"id": number, "body": bot.MARKER, "user": {"login": bot.BOT}})
        bot.reconcile(self.api, BASE, self.pr, ["file"])
        self.assertEqual([c["id"] for c in self.api.comments[1]], [1, 20])

    def test_stale_main_and_head_skip_writes(self):
        self.api.base = OTHER
        bot.reconcile(self.api, BASE, self.pr, ["file"])
        self.assertEqual(self.api.writes, [])
        self.api.base = BASE
        self.api.prs[1] = OTHER
        bot.reconcile(self.api, BASE, self.pr, ["file"])
        self.assertEqual(self.api.writes, [])
        self.api.prs[1] = HEAD
        self.api.states[1] = "closed"
        bot.reconcile(self.api, BASE, self.pr, ["file"])
        self.assertEqual(self.api.writes, [])

    def test_snapshot_guard_between_writes(self):
        self.api.label_exists = True
        self.api.change_on_fresh = 3
        bot.reconcile(self.api, BASE, self.pr, ["file"])
        self.assertEqual(self.api.labels[1], ["other", bot.LABEL])
        self.assertEqual(self.api.comments[1], [self.user_comment])

    def test_main_consumes_all_pages_and_skips_moved_fetch(self):
        with mock.patch.object(bot, "fetch_head", side_effect=[True, False]) as fetch, mock.patch.object(bot, "conflict_paths", return_value=["file"]):
            self.assertEqual(bot.main(["--base", BASE], self.api), 0)
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(self.api.labels[1], ["other", bot.LABEL])
        self.assertEqual(self.api.labels[2], [])

    def test_non_main_base_is_processed(self):
        with mock.patch.object(bot, "fetch_head", return_value=True), mock.patch.object(bot, "conflict_paths", return_value=["file"]):
            self.assertEqual(bot.main(["--base", BASE], self.api), 0)
        self.assertEqual(self.api.labels[2], [bot.LABEL])
        self.assertEqual(len(self.api.comments[2]), 1)

    def test_quoted_marker_in_bot_comment_is_preserved(self):
        quoted = {"id": 3, "body": "Quoted marker: " + bot.MARKER, "user": {"login": bot.BOT}}
        self.api.comments[1].append(quoted)
        bot.reconcile(self.api, BASE, self.pr, ["file"])
        self.assertEqual(self.api.comments[1][1], quoted)
        bot.reconcile(self.api, BASE, self.pr, [])
        self.assertEqual(self.api.comments[1], [self.user_comment, quoted])

    def test_forbidden_pr_writes_fail_with_safe_operation_context(self):
        for resource in ("labels", "comments"):
            with self.subTest(resource=resource):
                api = FakeApi()
                route = f"/repos/example/repo/issues/1/{resource}"
                request = api.request

                def deny(method, requested_route, payload=None):
                    if method == "POST" and requested_route == route:
                        raise GitHubApiError("GitHub API HTTP 403", status=403)
                    return request(method, requested_route, payload)

                stderr = io.StringIO()
                with mock.patch.object(api, "request", side_effect=deny), \
                     mock.patch.object(bot, "fetch_head", return_value=True), \
                     mock.patch.object(bot, "conflict_paths", return_value=["private-conflict-path"]), \
                     contextlib.redirect_stderr(stderr):
                    self.assertEqual(bot.main(["--base", BASE], api), 1)
                self.assertIn(f"POST {route}: GitHub API HTTP 403", stderr.getvalue())
                self.assertNotIn("private-conflict-path", stderr.getvalue())
                self.assertEqual(api.comments[1], [])
                self.assertEqual(api.labels[2], [])

    def test_api_errors_and_invalid_data_are_not_success(self):
        self.api.failure = "/repos/example/repo/pulls?state=open&per_page=100"
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(bot.main(["--base", BASE], self.api), 1)
            self.assertEqual(bot.main(["--base", "not-a-sha"], self.api), 1)
        with self.assertRaises(bot.ConflictError):
            bot.parse_pr({"number": "1", "head": {"sha": HEAD}})
        with self.assertRaises(bot.ConflictError):
            bot.parse_pr({"number": 0, "head": {"sha": HEAD}})
        with self.assertRaises(bot.ConflictError):
            bot.parse_pr({"number": 1, "head": {"sha": "--bad"}})


if __name__ == "__main__":
    unittest.main()
