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

Pull request checks validate the PR checkout. Queue checks run on a
synthetic `merge_group` commit, which may include earlier queued changes; the
combined head is tested before a batch lands. Some checks intentionally skip
for docs-only changes, but `docs/API.md` and `docs/PLAN.md` are live inputs.
A skipped check is not evidence that its underlying tests ran.

If a queue check fails, inspect the failing merge-group run, for example:

```sh
gh run view <run-id> --repo leadingterm/gang --log-failed
```

Fix the cause, push the change, and enable auto-merge again to re-enqueue the
pull request. Do not bypass or reorder the queue.
