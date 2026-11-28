import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import queue_ci
from github_api import GitHubApiError

SHA = "a" * 40
TREE = "b" * 40


def run(run_id=1):
    return {"id": run_id, "created_at": f"2026-01-{run_id:02}T00:00:00Z", "event": "merge_group",
            "status": "completed", "conclusion": "success",
            "head_sha": SHA, "head_branch": "gh-readonly-queue/main/pr-1-abc",
            "path": ".github/workflows/ci.yml", "repository": {"full_name": "org/repo"},
            "head_repository": {"full_name": "org/repo"},
            "head_commit": {"id": SHA, "tree_id": TREE}}


class FakeApi:
    repository = "org/repo"

    def __init__(self):
        self.status = "ahead"
        self.runs = [run()]
        self.jobs = {1: [{"name": name, "status": "completed", "conclusion": "success"} for name in queue_ci.CHECKS]}
        self.artifacts = {1: [{"name": name, "id": index, "expired": False}
                              for index, name in enumerate(queue_ci.LINUX + queue_ci.WORKING, 1)]}
        self.fail_pages = None

    def request(self, method, route, payload=None):
        if "/compare/" in route:
            return {"status": self.status}
        return {"sha": SHA, "commit": {"tree": {"sha": TREE}}}

    def pages(self, route):
        if "/workflows/" in route:
            key, data = "workflow_runs", self.runs
        else:
            run_id = int(route.split("/actions/runs/")[1].split("/")[0])
            key = "jobs" if "/jobs?" in route else "artifacts"
            data = (self.jobs if key == "jobs" else self.artifacts)[run_id]
        middle = len(data) // 2
        yield {key: data[:middle]}
        if self.fail_pages == key:
            raise GitHubApiError("pagination failed")
        yield {key: data[middle:]}


class QueueCiTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeApi()

    def select(self, working=False):
        return queue_ci.select_queue_run(self.api, SHA, working)

    def rejected(self):
        with self.assertRaises(ValueError):
            self.select(True)

    def test_selection_and_actual_cli_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "output"
            self.assertEqual(queue_ci.main(["--sha", SHA, "--github-output", str(path), "--require-working-sets"], self.api), 0)
            self.assertEqual(path.read_text(), f"run-id=1\ntree-sha={TREE}\nx86-working-set-artifact-ids=3,4,5,6,7\narm-working-set-artifact-ids=8,9,10\n")
        self.assertEqual(self.select().x86_working_set_artifact_ids, ())

    def test_invalid_sha_and_ancestry(self):
        for sha in ("A" * 40, "a" * 39, "a" * 41):
            with self.assertRaises(ValueError):
                queue_ci.select_queue_run(self.api, sha)
        for status in ("behind", "diverged"):
            self.api.status = status
            self.rejected()
        self.api.status = "identical"
        self.assertEqual(self.select().run_id, 1)

    def test_run_trust_and_exact_head(self):
        mutations = {"event": "pull_request", "head_sha": "c" * 40, "head_branch": "main",
                     "path": ".github/workflows/pr.yml", "repository": {"full_name": "evil/repo"},
                     "head_repository": {"full_name": "evil/repo"},
                     "head_commit": {"id": SHA, "tree_id": "c" * 40}}
        for field, value in mutations.items():
            with self.subTest(field=field):
                self.api.runs = [run()]
                self.api.runs[0][field] = value
                self.rejected()
        self.api.runs = [run()]
        self.api.runs[0]["event"] = "push"
        self.rejected()
        self.api.runs = [run()]
        self.api.runs[0]["head_commit"]["id"] = "c" * 40
        self.rejected()

    def test_run_status_and_required_tree_id(self):
        self.api.runs[0]["status"] = "in_progress"
        self.api.runs[0]["conclusion"] = None
        self.assertEqual(self.select().run_id, 1)
        self.api.runs[0]["status"] = "queued"
        self.rejected()
        for field in ("status", "conclusion"):
            for value in ([], 7):
                self.api.runs = [run()]
                self.api.runs[0][field] = value
                self.rejected()
            self.api.runs = [run()]
            del self.api.runs[0][field]
            self.rejected()
        self.api.runs = [run()]
        self.api.runs[0]["head_commit"] = {"id": SHA, "tree": {"sha": TREE}}
        self.rejected()

    def test_critical_jobs_missing_failure_duplicates_and_advisory(self):
        original = copy.deepcopy(self.api.jobs[1])
        for name in queue_ci.CHECKS:
            for conclusion in ("failure", "cancelled", "skipped", None):
                self.api.jobs[1] = copy.deepcopy(original)
                next(job for job in self.api.jobs[1] if job["name"] == name)["conclusion"] = conclusion
                self.rejected()
            self.api.jobs[1] = [job for job in original if job["name"] != name]
            self.rejected()
            self.api.jobs[1] = copy.deepcopy(original) + [{"name": name, "status": "completed", "conclusion": "success"}]
            self.rejected()
            self.api.jobs[1] = copy.deepcopy(original)
            next(job for job in self.api.jobs[1] if job["name"] == name)["status"] = "in_progress"
            self.rejected()
        self.api.jobs[1] = original + [{"name": "macos", "conclusion": "failure"}]
        self.assertEqual(self.select().run_id, 1)

    def test_artifact_validation(self):
        original = copy.deepcopy(self.api.artifacts[1])
        for index in range(len(original)):
            self.api.artifacts[1] = copy.deepcopy(original)
            self.api.artifacts[1][index]["expired"] = True
            self.rejected()
            self.api.artifacts[1] = copy.deepcopy(original) + [copy.deepcopy(original[index])]
            self.rejected()
        self.api.artifacts[1] = copy.deepcopy(original)
        self.api.artifacts[1][1]["id"] = 0
        self.rejected()
        self.api.artifacts[1] = copy.deepcopy(original)
        self.api.artifacts[1][1]["id"] = self.api.artifacts[1][0]["id"]
        self.rejected()
        self.api.artifacts[1] = copy.deepcopy(original) + [{"name": queue_ci.DARWIN, "id": 11, "expired": False}]
        self.assertEqual(self.select().run_id, 1)
        self.api.artifacts[1].append({"name": queue_ci.DARWIN, "id": 12, "expired": False})
        self.rejected()

    def test_arm_working_sets_missing_or_duplicate_fail_closed(self):
        original = copy.deepcopy(self.api.artifacts[1])
        for name in queue_ci.ARM_WORKING:
            with self.subTest(name=name):
                self.api.artifacts[1] = [artifact for artifact in copy.deepcopy(original) if artifact["name"] != name]
                self.rejected()
                artifact = next(artifact for artifact in original if artifact["name"] == name)
                self.api.artifacts[1] = copy.deepcopy(original) + [copy.deepcopy(artifact)]
                self.rejected()

    def test_older_run_fallback_and_latest_jobs(self):
        self.api.runs = [run(1), run(2)]
        self.api.jobs[2] = copy.deepcopy(self.api.jobs[1])
        self.api.jobs[2][0]["conclusion"] = "failure"
        self.api.artifacts[2] = copy.deepcopy(self.api.artifacts[1])
        self.assertEqual(self.select(True).run_id, 1)
        self.api.jobs[2][0]["conclusion"] = "success"
        self.assertEqual(self.select(True).run_id, 2)

    def test_pagination_errors_fail_closed(self):
        for key in ("workflow_runs", "jobs", "artifacts"):
            self.api.fail_pages = key
            with self.subTest(key=key), self.assertRaises(GitHubApiError):
                self.select(True)

    def test_malformed_page_and_from_env(self):
        with patch.object(self.api, "pages", return_value=iter([{"workflow_runs": None}])):
            with self.assertRaises(ValueError):
                self.select()
        with tempfile.TemporaryDirectory() as directory, patch.object(queue_ci.GitHubApi, "from_env", return_value=self.api) as factory:
            self.assertEqual(queue_ci.main(["--sha", SHA, "--github-output", str(Path(directory) / "out")]), 0)
            factory.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
