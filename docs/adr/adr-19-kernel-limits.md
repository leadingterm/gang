# ADR-19: Kernel limits are a backstop behind NativeLink's soft enforcement

Status: Accepted

## Context
- NativeLink enforces memory only softly: it kills an over-limit action and requeues it with a larger reservation, and `gang-nativelink` renders that enforcement (`[scheduler.nativelink.memory]`). A runaway action can still exhaust the worker before that happens.

## Decision
- **An optional `limits` domain** (zero or one plugin) behind `ActionLimitsPort`. `gang-cgroup` (Linux, cgroup v2) works under the worker's delegated subtree; the worker's systemd unit sets `Delegate=yes`.
- **Lifecycle.** `prepare` (in `gang worker`) moves the worker into a leaf. `enter` (in `gang exec`) creates `actions/<id>` and applies the limits; `gang exec` never enters the scope. The sandbox starts through the `gang-join` trampoline, which joins the scope and execs, so the action and all descendants run under the limits from their first instruction. An out-of-memory kill of the scope cannot end `gang exec`. `leave` records `ActionUsage`, writes `cgroup.kill` for leftovers and removes the scope.
- **Abandoned scopes.** `NativeLink` SIGKILLs `gang exec` by PID after the kill grace, before it can `leave`. With limits, `gang exec` first starts a `gang-guard` child in the worker's process group with an empty environment and no standard streams. The guardian opens a pidfd on its creator and then checks that the creator is still its parent, so the pidfd never names a reused PID; the kernel makes it readable only once the creator has exited, and the guardian then calls `reap_abandoned`, which removes the creator's remaining scopes.
- **Limits sit above the soft path.** `memory.max` is `memory_kb` plus `memory_headroom_percent` (default 100, twice the reservation), above NativeLink's soft kill so its requeue stays primary. Swap is `swap_max` (default 0), so it is a hard ceiling. `memory.oom.group` is 1, `cpu.max` is `cpu_count` times the period, and `pids.max` is `max_pids`.
- **Client-declared inputs.** `memory_kb` and `cpu_count` are client-declared, so the limit bounds only the action's own backstop; the worker's cgroup bounds the whole worker.
- **Bounded environments omit the section** (a pod or job cgroup already bounds the worker), and so does macOS.

## Consequences
- Deployment must provide a delegated cgroup v2 subtree; macOS actions run without kernel limits.
- The kernel limit is a backstop only; soft enforcement stays the normal path.

## Rejected alternatives
- Moving `gang exec` into the scope around the spawn (enter, spawn, detach): an OOM kill between spawn and detach also ended `gang exec`.
- `clone3(CLONE_INTO_CGROUP)` or a pre-exec join: both need unsafe code, which the workspace forbids.
- Waking the guardian on EOF of a pipe from `gang exec`: a killed process closes its descriptors before it becomes a zombie, so the guardian could see the creator still alive and leave its scope behind.
