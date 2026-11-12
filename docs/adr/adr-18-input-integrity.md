# ADR-18: Input integrity: hard links are broken before the sandbox runs

Status: Accepted

## Context
- NativeLink hard-links each input from the worker's fast CAS into the action's writable work directory. An action running as the worker's user could `chmod u+w` a linked input, rewrite the shared inode and poison later actions on that worker.

## Decision
- **`[execution] input_links = keep | break`** (default `keep`; production sets `break`). With `break`, before the sandbox runs, `gang exec` gives every regular file in the input tree with a link count above one a private inode: `FICLONE` reflink on Linux, `clonefile` on macOS, otherwise a copy to a temporary file in the same directory renamed over the link with the mode preserved.
- **Sandbox-agnostic and needs no upstream change.**
- **AWS scratch.** The worker's scratch volume is instance-store NVMe RAID0, or an EBS data volume on types without instance store, formatted XFS with reflink, holding the fast CAS and the work directories, so the replacement is a reflink. The fast CAS evicts against a budget derived from the volume's capacity, so the cache cannot fill the volume.

## Consequences
- An action cannot alter a cached input that a later action on the same worker sees.
- The cost is one reflink per linked input, or a copy where reflink is unavailable.
- The step is pointless for single-use workers (ADR-13).

## Rejected alternatives
- **Read-only inputs through a bwrap overlay:** Linux-only, version-dependent, and carries a merge-back cost.
- **Detect and evict poisoned inputs:** detects only after the fact; concurrent actions can read the poisoned blob first.
