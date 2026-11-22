#!/usr/bin/env python3
"""Validate retained release fragments and render a changelog from Git history."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
import subprocess
import sys

CATEGORIES = ("Added", "Changed", "Deprecated", "Removed", "Fixed", "Security", "Upgrade notes")
VERSION = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?\Z")
ARCHIVE_VERSION = re.compile(r"^## v?(\d+)\.(\d+)\.(\d+)(?:\s|$)", re.MULTILINE)


class ChangelogError(Exception):
    pass


def git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(["git", "-C", str(repo), *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if result.returncode:
        raise ChangelogError(f"git {' '.join(args)} failed: {result.stderr.decode('utf-8', 'replace').strip()}")
    return result.stdout


def text(data: bytes, description: str) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ChangelogError(f"{description} is not UTF-8") from error


def version(tag: str) -> tuple[int, int, int, str | None]:
    match = VERSION.fullmatch(tag)
    if not match or (match[4] and any(part.isdigit() and len(part) > 1 and part[0] == "0" for part in match[4].split("."))):
        raise ChangelogError(f"invalid release tag: {tag}")
    return int(match[1]), int(match[2]), int(match[3]), match[4]


def resolve(repo: Path, ref: str) -> str:
    if not ref or ref.startswith("-"):
        raise ChangelogError(f"invalid ref: {ref}")
    return text(git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}"), ref).strip()


def full_history(repo: Path) -> None:
    if git(repo, "rev-parse", "--is-shallow-repository").strip() != b"false":
        raise ChangelogError("full Git history is required (repository is shallow)")


def ancestors(repo: Path, ref: str) -> list[tuple[tuple[int, int, int], str, str]]:
    tags = text(git(repo, "tag", "--merged", ref, "--list", "v*"), "tags").splitlines()
    result = []
    for tag in tags:
        try:
            major, minor, patch, prerelease = version(tag)
        except ChangelogError:
            continue
        if prerelease is None:
            result.append(((major, minor, patch), tag, resolve(repo, f"refs/tags/{tag}")))
    return sorted(result)


def parse_fragment(name: str, content: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current: str | None = None
    has_bullet = False
    for line in content.splitlines(keepends=True):
        stripped = line.rstrip("\r\n")
        if stripped.startswith("### "):
            heading = stripped[4:]
            if heading not in CATEGORIES or heading in sections:
                raise ChangelogError(f"{name}: unknown or duplicate category: {heading}")
            if current is not None and not has_bullet:
                raise ChangelogError(f"{name}: empty category: {current}")
            sections[heading] = []
            current = heading
            has_bullet = False
        elif current is None:
            if stripped.strip():
                raise ChangelogError(f"{name}: expected category heading")
        else:
            if re.match(r"^-\s+\S", stripped):
                has_bullet = True
            elif stripped.strip() and not (has_bullet and (stripped.startswith("  ") or stripped.startswith("\t"))):
                raise ChangelogError(f"{name}: expected a bullet or indented continuation")
            sections[current].append(line)
    if current is None or not has_bullet:
        raise ChangelogError(f"{name}: fragment or category is empty")
    return {category: "".join(lines).strip("\r\n") for category, lines in sections.items()}


def snapshot(repo: Path, ref: str) -> dict[str, bytes]:
    listing = git(repo, "ls-tree", "-r", "-z", "--name-only", ref, "--", "changes")
    files: dict[str, bytes] = {}
    for raw in listing.split(b"\0"):
        if not raw:
            continue
        name = text(raw, "fragment path")
        if not re.fullmatch(r"changes/[^/]+\.md", name):
            raise ChangelogError(f"invalid fragment path: {name}")
        files[name] = git(repo, "show", f"{ref}:{name}")
    return files


def validate(files: dict[str, bytes]) -> dict[str, dict[str, str]]:
    return {name: parse_fragment(name, text(data, name)) for name, data in sorted(files.items())}


def additions(previous: dict[str, bytes], current: dict[str, bytes]) -> dict[str, bytes]:
    for name, data in previous.items():
        if current.get(name) != data:
            raise ChangelogError(f"released fragment modified or deleted: {name}")
    return {name: data for name, data in current.items() if name not in previous}


def section(repo: Path, tag: str, commit: str, baseline: str | None) -> str:
    current = snapshot(repo, commit)
    previous = snapshot(repo, baseline) if baseline else {}
    fragments = validate(additions(previous, current))
    timestamp = text(git(repo, "show", "-s", "--format=%cI", commit), "commit date").strip()
    try:
        date = datetime.fromisoformat(timestamp).astimezone(timezone.utc).date().isoformat()
    except ValueError as error:
        raise ChangelogError(f"invalid commit date: {timestamp}") from error
    result = f"## {tag[1:]} — {date}\n"
    for category in CATEGORIES:
        bodies = [parts[category] for parts in fragments.values() if category in parts]
        if bodies:
            result += f"\n### {category}\n\n" + "\n\n".join(bodies) + "\n"
    return result


def render(repo: Path, ref: str, tag: str) -> tuple[str, str]:
    full_history(repo)
    major, minor, patch, prerelease = version(tag)
    commit = resolve(repo, ref)
    if resolve(repo, f"refs/tags/{tag}") != commit:
        raise ChangelogError("release tag does not resolve to the supplied ref")
    tags = ancestors(repo, commit)
    target_version = (major, minor, patch)
    eligible = [entry for entry in tags if entry[0] < target_version]
    baseline = eligible[-1][2] if eligible else None
    archive = text(git(repo, "show", f"{commit}:CHANGELOG.md"), "CHANGELOG.md")
    version_headings = list(ARCHIVE_VERSION.finditer(archive))
    archive_versions = [tuple(map(int, match.groups())) for match in version_headings]
    if any(number > target_version for number in archive_versions):
        raise ChangelogError(f"historical archive contains a version newer than {tag}")
    archive_start = version_headings[0].start() if version_headings else len(archive)
    preamble = archive[:archive_start]
    unreleased = re.search(r"^## Unreleased[ \t]*\r?$", preamble, re.MULTILINE)
    if unreleased and preamble[unreleased.end():].strip():
        raise ChangelogError(
            "historical CHANGELOG.md has nonempty Unreleased notes; "
            "move them into the appropriate archived release before publishing"
        )
    archived = max(archive_versions, default=(-1, -1, -1))
    if prerelease is None and target_version in archive_versions:
        index = archive_versions.index(target_version)
        start = version_headings[index].start()
        next_heading = re.search(r"^## ", archive[version_headings[index].end():], re.MULTILINE)
        end = version_headings[index].end() + next_heading.start() if next_heading else len(archive)
        return "# Changelog\n\n" + archive[archive_start:], archive[start:end]
    current = section(repo, tag, commit, baseline)
    historical = []
    for index, (number, old_tag, old_commit) in enumerate(eligible):
        if number > archived:
            previous = eligible[index - 1][2] if index else None
            historical.append(section(repo, old_tag, old_commit, previous))
    combined = "# Changelog\n\n" + current.rstrip("\n") + "\n\n"
    for item in reversed(historical):
        combined += item.rstrip("\n") + "\n\n"
    return combined + archive[archive_start:], current


def check(repo: Path, ref: str) -> None:
    full_history(repo)
    commit = resolve(repo, ref)
    directory = repo / "changes"
    files: dict[str, bytes] = {}
    if directory.exists():
        for path in sorted(directory.rglob("*")):
            if path.is_dir() and not path.is_symlink():
                continue
            name = path.relative_to(repo).as_posix()
            if not re.fullmatch(r"changes/[^/]+\.md", name) or path.is_symlink():
                raise ChangelogError(f"invalid fragment path: {name}")
            files[name] = path.read_bytes()
    validate(files)
    tags = ancestors(repo, commit)
    if tags:
        listing = git(repo, "ls-tree", "-r", "-z", tags[-1][2], "--", "changes")
        algorithm = text(git(repo, "rev-parse", "--show-object-format"), "object format").strip()
        for entry in listing.split(b"\0"):
            if not entry:
                continue
            metadata, raw_name = entry.split(b"\t", 1)
            mode, kind, oid = metadata.split(b" ")
            name = text(raw_name, "fragment path")
            data = files.get(name)
            if mode not in (b"100644", b"100755") or kind != b"blob" or data is None:
                raise ChangelogError(f"released fragment modified or deleted: {name}")
            digest = hashlib.new(algorithm, b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest().encode()
            if digest != oid:
                raise ChangelogError(f"released fragment modified or deleted: {name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    check_parser = commands.add_parser("check")
    check_parser.add_argument("--ref", default="HEAD")
    render_parser = commands.add_parser("render")
    render_parser.add_argument("--ref", required=True)
    render_parser.add_argument("--tag", required=True)
    render_parser.add_argument("--output", type=Path, required=True)
    render_parser.add_argument("--section-output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            check(args.repo, args.ref)
        else:
            whole, current = render(args.repo, args.ref, args.tag)
            args.output.write_text(whole, encoding="utf-8")
            args.section_output.write_text(current, encoding="utf-8")
    except (ChangelogError, OSError) as error:
        print(f"changelog: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
