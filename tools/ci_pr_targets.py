#!/usr/bin/env python3
"""Select visible lint tests and affected unit tests for PR preflight."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys


LABEL = re.compile(r"^//[A-Za-z0-9_./+~-]*:[A-Za-z0-9_./+~=-]+$")
PATH = re.compile(r"^[A-Za-z0-9_./+~-]+$")
LINT = re.compile(r"_(?:clippy|rustfmt)_test$")
VISIBLE_TESTS = (
    'let roots = //... except attr("tags", "(^|, |\\[)manual(, |\\]$)", //...) '
    'in tests($roots)'
)


def query(expression: str) -> set[str]:
    result = subprocess.run(
        [os.environ.get("BAZEL", "bazel"), "query", "--output=label", expression],
        check=True, stdout=subprocess.PIPE, text=True,
    )
    labels = set(result.stdout.splitlines())
    if any(not LABEL.fullmatch(label) for label in labels):
        raise ValueError("Bazel returned an unsupported label")
    return labels


def ordinary_doc(path: str) -> bool:
    if path in {"README.md", "CHANGELOG.md", "LICENSE"}:
        return True
    if "/" not in path and path.startswith("LICENSE-"):
        return True
    if path in {"docs/API.md", "docs/PLAN.md"}:
        return False
    return path.startswith(("docs/", "notes/", "changes/")) and path.endswith(".md")


def graph_input(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return (
        name in {"BUILD", "BUILD.bazel", "WORKSPACE", "WORKSPACE.bazel", "MODULE.bazel", "MODULE.bazel.lock", "Cargo.toml", "Cargo.lock", "Cargo.Bazel.lock", "flake.nix", "flake.lock", ".bazelrc", ".bazelversion", "rust-toolchain", "rust-toolchain.toml"}
        or name.endswith((".bzl", ".bazelrc"))
        or "toolchain" in name.lower()
        or name.startswith("Cargo.")
    )


def select(base: str, head: str) -> dict[str, list[str]]:
    diff = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", "-z", base, head, "--"],
        check=True, stdout=subprocess.PIPE,
    ).stdout
    paths = [os.fsdecode(path) for path in diff.split(b"\0") if path]
    visible = query(VISIBLE_TESTS)
    lint = sorted(label for label in visible if LINT.search(label))
    units = {label for label in visible if label.endswith("_unit_test")}

    seeds: list[str] = []
    sources: dict[str, str] | None = None
    for path in paths:
        # Even documentation deletions and unsafe paths must not hide graph changes.
        if not PATH.fullmatch(path) or not Path(path).is_file() or graph_input(path):
            return {"lint": lint, "unit": sorted(units)}
        if ordinary_doc(path):
            continue
        if sources is None:
            sources = {}
            for label in query('kind("source file", //...:*)'):
                package, target = label[2:].split(":", 1)
                filename = f"{package}/{target}" if package else target
                sources[filename] = label
        label = sources.get(path)
        if label is None:
            return {"lint": lint, "unit": sorted(units)}
        seeds.append(label)
    if not seeds:
        return {"lint": lint, "unit": []}
    affected = query(f"rdeps(//..., set({' '.join(sorted(set(seeds)))}))")
    return {"lint": lint, "unit": sorted(units & affected)}


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        print("usage: ci_pr_targets.py BASE HEAD", file=sys.stderr)
        return 2
    try:
        manifest = select(args[0], args[1])
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"ci_pr_targets: {error}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
