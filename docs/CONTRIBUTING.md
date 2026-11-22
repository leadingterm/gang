# Contributing

## Pull requests

1. Create a branch, make a focused change, and open a pull request. Draft pull
   requests can run CI while the change is in progress.
2. For the pinned development environment and build commands, see
   [Build](../README.md#build).
3. When the pull request is ready, mark it ready for review and enable auto-merge:

   ```sh
   gh pr merge <number> --repo leadingterm/gang --auto
   ```

GitHub waits for the required checks and places the pull request in the merge
queue. Merges are squash merges. You do not need to rebase or update your branch
just because `main` advanced: the queue validates your change against current
`main` and any earlier queued changes.

## Related pull requests

Independent pull requests use ordinary auto-merge as described above. Related
or stacked pull requests carrying earlier unmerged changes must enter the build
window together, before the parent merge group exists. Allow enough build-window
capacity for the whole batch, and submit it in one batch enqueue operation:
one GraphQL mutation with an aliased `enqueuePullRequest` field per PR, or
native-stack **Enqueue stack** when applicable. Sequential `gh pr merge --auto`
calls are not an atomic batch submission.

A [public bug report](https://github.com/github/gh-stack/discussions/223) describes
late children being ejected with `invalid_merge_commit` under squash merging.
Batch submission is a reported workaround, not a GitHub-guaranteed fix. That
removal is not itself a CI failure. Do not blindly retry, edit code, weaken
signed linear history, squash or required checks, or reorder entries already
building. Leave existing builds alone and re-enqueue the unchanged child after
its referenced prefix lands; do not repeat branch updates and primary CI just
because `main` advanced. Do not normally merge temporary `gh-readonly-queue`
refs into source branches as a generic conflict fix: they are ephemeral and
create backend-sensitive ancestry.

The [merge queue settings](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue) for minimum merges and wait time control merging,
not a delay before creating builds.

## Checks and failures

Pull request checks run portable lint, build `//:gang`, run all generated
Clippy and rustfmt tests, and run affected Rust unit tests selected through
reverse dependencies, with a conservative all-unit fallback for uncertain
changes. Even docs-only PRs run the real build and lint. Queue checks run the
full suite on a synthetic `merge_group` commit, including for docs-only changes.
The same six required check names mean all PR preflight work succeeded in a PR
context, and full qualification succeeded in a queue context; PR success does
not mean ARM, cross or E2E ran. Failure, cancellation, skipped or missing
producers cannot pass.

`gh pr checks <number> --required --watch` follows the PR's latest commit, not
its prepared merge-group commit. A successful PR check watch is therefore not
evidence that queue validation has finished. Follow the PR's queue entry and
its linked `merge_group` run for combined-tree results. Advisory macOS results
are not required for merging.

On every push to `main`, a trusted-main conflict diagnostic uses `git
merge-tree` against every open PR, including drafts and PRs with other base
branches. It adds `needs-rebase` and a comment with escaped conflicting
paths when needed, and clears the report when clean. It never checks out or
executes PR code and is not a waiver of the queue gates.

A PR-only push does not rerun this diagnostic. After fixing a conflict, its
label or comment can remain until the next `main` push refreshes the report.
Use current GitHub mergeability and queue results rather than treating a stale
report as a reason to bypass checks or update an otherwise clean branch.

If a queue check fails, inspect the failing merge-group run, for example:

```sh
gh run view <run-id> --repo leadingterm/gang --log-failed
```

Fix the cause, push the change, and enable auto-merge again to re-enqueue the
pull request. Do not bypass or reorder the queue.

## Changelog entries

Add new release notes in a uniquely named `changes/<topic>.md` fragment instead
of appending to the shared Unreleased section. Use one or more headings from
`### Added`, `### Changed`, `### Deprecated`, `### Removed`, `### Fixed`,
`### Security` and `### Upgrade notes`, each followed by nonempty bullets;
indent continuation lines under their bullet. Fragments may be edited before
release, but released fragments are immutable and retained as history. The
root `CHANGELOG.md` is the legacy archive; releases generate the current section
and reconstruct later history from fragments and tags before appending it.

