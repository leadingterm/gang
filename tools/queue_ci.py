#!/usr/bin/env python3
"""Select full merge-queue CI evidence for an exact landed commit."""

import argparse
from dataclasses import dataclass
import re
import sys
from pathlib import Path

from github_api import GitHubApi, GitHubApiError

SHA = re.compile(r"[0-9a-f]{40}\Z")
CHECKS = frozenset(("release-validation", "lint", "cross", "linux-x86_64", "linux-arm64", "e2e-gate-1", "e2e-gate-2"))
LINUX = ("gang-x86_64-unknown-linux-musl", "gang-aarch64-unknown-linux-musl")
DARWIN = "gang-aarch64-apple-darwin"
X86_WORKING = tuple(f"bazel-working-set-x86-{n}" for n in (1, 2, 3, 4, "lint"))

ARM_WORKING = tuple(f"bazel-working-set-arm64-{name}" for name in ("bundle", "checks", "cross"))
WORKING = X86_WORKING + ARM_WORKING


@dataclass(frozen=True)
class QueueRun:
    run_id: int
    tree_sha: str
    x86_working_set_artifact_ids: tuple[int, ...]
    arm_working_set_artifact_ids: tuple[int, ...]


def mapping(value: object) -> dict:
    if not isinstance(value, dict):
        raise ValueError("expected API object")
    return value


def string(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("expected API string")
    return value


def positive_id(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("expected positive API ID")
    return value


def page_items(api: GitHubApi, route: str, key: str) -> list:
    result = []
    for page in api.pages(route):
        items = mapping(page).get(key)
        if not isinstance(items, list):
            raise ValueError(f"invalid {key} page")
        result.extend(items)
    return result


def select_queue_run(api: GitHubApi, sha: str, require_working_sets: bool = False) -> QueueRun:
    if not SHA.fullmatch(sha):
        raise ValueError("SHA must be 40 lowercase hexadecimal characters")
    base = f"/repos/{api.repository}"
    comparison = mapping(api.request("GET", f"{base}/compare/{sha}...main"))
    if string(comparison.get("status")) not in ("identical", "ahead"):
        raise ValueError("commit is not an ancestor of main")
    commit = mapping(api.request("GET", f"{base}/commits/{sha}"))
    if string(commit.get("sha")) != sha:
        raise ValueError("commit SHA mismatch")
    tree = string(mapping(mapping(commit.get("commit")).get("tree")).get("sha"))
    if not SHA.fullmatch(tree):
        raise ValueError("invalid commit tree SHA")
    runs = page_items(api, f"{base}/actions/workflows/ci.yml/runs?head_sha={sha}&event=merge_group&per_page=100", "workflow_runs")
    candidates = []
    for item in runs:
        run = mapping(item)
        run_id = positive_id(run.get("id"))
        created = string(run.get("created_at"))
        event = string(run.get("event"))
        status = string(run.get("status"))
        if "conclusion" not in run:
            raise ValueError("missing run conclusion")
        if run["conclusion"] is not None:
            string(run["conclusion"])
        head = string(run.get("head_sha"))
        branch = string(run.get("head_branch"))
        path = string(run.get("path"))
        repo = string(mapping(run.get("repository")).get("full_name"))
        head_repo = string(mapping(run.get("head_repository")).get("full_name"))
        head_commit = mapping(run.get("head_commit"))
        commit_id = string(head_commit.get("id"))
        tree_id = string(head_commit.get("tree_id"))
        if (status in ("in_progress", "completed") and event == "merge_group"
                and head == sha and branch.startswith("gh-readonly-queue/main/")
                and path == ".github/workflows/ci.yml" and repo == api.repository
                and head_repo == api.repository and commit_id == sha and tree_id == tree):
            candidates.append((created, run_id))
    for _, run_id in sorted(candidates, reverse=True):
        jobs = page_items(api, f"{base}/actions/runs/{run_id}/jobs?filter=latest&per_page=100", "jobs")
        relevant = {name: [] for name in CHECKS}
        for item in jobs:
            job = mapping(item)
            name = string(job.get("name"))
            if name in relevant:
                status = string(job.get("status"))
                conclusion = job.get("conclusion")
                if conclusion is not None:
                    string(conclusion)
                relevant[name].append((status, conclusion))
        if any(values != [("completed", "success")] for values in relevant.values()):
            continue
        artifacts = page_items(api, f"{base}/actions/runs/{run_id}/artifacts?per_page=100", "artifacts")
        wanted = set(LINUX + (DARWIN,) + (WORKING if require_working_sets else ()))
        found = {name: [] for name in wanted}
        for item in artifacts:
            artifact = mapping(item)
            name = string(artifact.get("name"))
            artifact_id = positive_id(artifact.get("id"))
            expired = artifact.get("expired")
            if type(expired) is not bool:
                raise ValueError("invalid artifact expiration")
            if name in found and not expired:
                found[name].append(artifact_id)
        required = LINUX + (WORKING if require_working_sets else ())
        if any(len(found[name]) != 1 for name in required) or len(found[DARWIN]) > 1:
            continue
        ids = [found[name][0] for name in required]
        if found[DARWIN]:
            ids.extend(found[DARWIN])
        if len(ids) != len(set(ids)):
            continue
        return QueueRun(run_id, tree, tuple(found[name][0] for name in X86_WORKING) if require_working_sets else (),
                        tuple(found[name][0] for name in ARM_WORKING) if require_working_sets else ())
    raise ValueError("no qualified merge-queue CI run for commit")


def main(argv: list[str] | None = None, api: GitHubApi | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--github-output", required=True)
    parser.add_argument("--require-working-sets", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = select_queue_run(api if api is not None else GitHubApi.from_env(), args.sha, args.require_working_sets)
        with Path(args.github_output).open("a", encoding="utf-8") as output:
            output.write(f"run-id={result.run_id}\ntree-sha={result.tree_sha}\n")
            output.write("x86-working-set-artifact-ids=" + ",".join(map(str, result.x86_working_set_artifact_ids)) + "\n")
            output.write("arm-working-set-artifact-ids=" + ",".join(map(str, result.arm_working_set_artifact_ids)) + "\n")
    except (GitHubApiError, ValueError, OSError) as error:
        print(f"queue CI selection failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
