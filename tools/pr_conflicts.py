#!/usr/bin/env python3
"""Report conflicts against a trusted main snapshot without checking out PR code."""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from urllib.parse import quote

from github_api import GitHubApi, GitHubApiError

MARKER = "<!-- gang-merge-conflicts -->"
LABEL = "needs-rebase"
BOT = "github-actions[bot]"
SHA = re.compile(r"[0-9a-f]{40}\Z")


class ConflictError(Exception):
    """Invalid Git or GitHub data, or an unsuccessful Git operation."""


def object_value(value: object, description: str) -> dict:
    if not isinstance(value, dict):
        raise ConflictError(f"invalid {description}: expected object")
    return value


def sha_value(value: object) -> str:
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise ConflictError("invalid commit SHA")
    return value


def number_value(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ConflictError("invalid GitHub number")
    return value


def items(api: GitHubApi, route: str):
    for page in api.pages(route):
        if not isinstance(page, list):
            raise ConflictError(f"invalid list response for {route}")
        yield from page


@dataclass(frozen=True)
class PullRequest:
    number: int
    head: str


def parse_pr(value: object) -> PullRequest:
    pr = object_value(value, "pull request")
    return PullRequest(number_value(pr.get("number")), sha_value(object_value(pr.get("head"), "PR head").get("sha")))


def git(*args: str, allowed: tuple[int, ...] = (0,)) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(["git", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if result.returncode not in allowed:
        raise ConflictError(f"git {args[0]} failed (exit {result.returncode}): {result.stderr.decode('utf-8', 'replace').strip()}")
    return result


def fetch_head(pr: PullRequest) -> bool:
    # The helper is fixed and obtains the token from gh's environment, never argv.
    git("-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential", "fetch", "--no-tags", "origin", f"refs/pull/{pr.number}/head")
    fetched = git("rev-parse", "--verify", "FETCH_HEAD^{commit}").stdout.decode("ascii").strip()
    return sha_value(fetched) == pr.head


def conflict_paths(base: str, head: str) -> list[str]:
    sha_value(base)
    sha_value(head)
    result = git("merge-tree", "--write-tree", "--name-only", "--no-messages", "-z", base, head, allowed=(0, 1))
    fields = result.stdout.split(b"\0")
    if len(fields) < 2 or not SHA.fullmatch(fields[0].decode("ascii", "replace")):
        raise ConflictError("invalid git merge-tree output")
    if result.returncode == 0:
        return []
    try:
        end = fields.index(b"", 1)
    except ValueError as error:
        raise ConflictError("unterminated git merge-tree paths") from error
    paths = fields[1:end]
    if not paths:
        raise ConflictError("git merge-tree reported conflicts without paths")
    return [path.decode("utf-8", "surrogateescape") for path in paths]


def comment_body(base: str, head: str, paths: list[str]) -> str:
    # JSON escapes control characters and undecodable bytes; HTML escapes Markdown/HTML.
    lines = [f"- <code>{html.escape(json.dumps(path, ensure_ascii=True)[1:-1]).replace('`', '&#96;')}</code>" for path in paths]
    return "\n".join([MARKER, "This pull request conflicts with main. Resolve the conflicts and update the branch.", "", f"Tested main: `{base}`", f"Tested head: `{head}`", "", *lines])


def fresh(api: GitHubApi, base: str, pr: PullRequest) -> bool:
    prefix = f"/repos/{api.repository}"
    main = object_value(api.request("GET", f"{prefix}/commits/main"), "main commit")
    if sha_value(main.get("sha")) != base:
        return False
    current = object_value(api.request("GET", f"{prefix}/pulls/{pr.number}"), "pull request")
    current_pr = parse_pr(current)
    if not isinstance(current.get("state"), str):
        raise ConflictError("invalid pull request state")
    if "base" in current:
        base_info = object_value(current["base"], "PR base")
        if "ref" in base_info and not isinstance(base_info["ref"], str):
            raise ConflictError("invalid pull request base")
    return current_pr == pr and current["state"] == "open"


def ensure_label(api: GitHubApi, prefix: str) -> None:
    route = f"{prefix}/labels/{quote(LABEL, safe='')}"
    try:
        api.request("GET", route)
    except GitHubApiError as error:
        if error.status != 404:
            raise
        try:
            api.request("POST", f"{prefix}/labels", {"name": LABEL, "color": "d73a4a", "description": "Conflicts with main"})
        except GitHubApiError as create_error:
            if create_error.status != 422:
                raise
            api.request("GET", route)


def reconcile(api: GitHubApi, base: str, pr: PullRequest, paths: list[str]) -> None:
    prefix = f"/repos/{api.repository}"
    issue = f"{prefix}/issues/{pr.number}"
    labels = []
    for value in items(api, f"{issue}/labels?per_page=100"):
        name = object_value(value, "label").get("name")
        if not isinstance(name, str):
            raise ConflictError("invalid label name")
        labels.append(name)
    comments = []
    for value in items(api, f"{issue}/comments?per_page=100"):
        comment = object_value(value, "comment")
        author = object_value(comment.get("user"), "comment author").get("login")
        body = comment.get("body")
        if not isinstance(author, str) or not isinstance(body, str):
            raise ConflictError("invalid comment")
        if author == BOT and body.startswith(MARKER):
            comments.append((number_value(comment.get("id")), body))

    def write(method: str, route: str, payload: object = None) -> bool:
        if not fresh(api, base, pr):
            return False
        try:
            api.request(method, route, payload)
        except GitHubApiError as error:
            raise ConflictError(f"{method} {route}: {error}") from error
        return True

    if paths:
        body = comment_body(base, pr.head, paths)
        if LABEL not in labels:
            if not fresh(api, base, pr):
                return
            ensure_label(api, prefix)
            if not write("POST", f"{issue}/labels", {"labels": [LABEL]}):
                return
        if comments:
            if comments[0][1] != body and not write("PATCH", f"{prefix}/issues/comments/{comments[0][0]}", {"body": body}):
                return
        elif not write("POST", f"{issue}/comments", {"body": body}):
            return
        extra = comments[1:]
    else:
        if LABEL in labels and not write("DELETE", f"{issue}/labels/{quote(LABEL, safe='')}"):
            return
        extra = comments
    for comment_id, _ in extra:
        if not write("DELETE", f"{prefix}/issues/comments/{comment_id}"):
            return


def main(argv: list[str] | None = None, api: GitHubApi | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    args = parser.parse_args(argv)
    try:
        base = sha_value(args.base)
        api = api if api is not None else GitHubApi.from_env()
        prefix = f"/repos/{api.repository}"
        for value in items(api, f"{prefix}/pulls?state=open&per_page=100"):
            pr = parse_pr(value)
            if not fresh(api, base, pr):
                continue
            if not fetch_head(pr):
                continue
            reconcile(api, base, pr, conflict_paths(base, pr.head))
    except (ConflictError, GitHubApiError, OSError, UnicodeError) as error:
        print(f"pr-conflicts: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
