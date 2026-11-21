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
