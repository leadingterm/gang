# ADR-20: Disk is a declared fleet minimum with a soft guard

Status: Accepted

## Context
- NativeLink offers a disk guard and soft kill like its memory limit, but actions with no measurements yet (cold start) are not enforced.
- A worker packed to its declared disk may legitimately hold headroom before any fast writer overshoots.
- Gang's matcher and NativeLink's must agree on what disk an action needs and a worker has.

## Decision
- **Soft enforcement, like memory.** `gang-nativelink` renders NativeLink's disk guard and soft kill (`[scheduler.nativelink.disk]`). Cold start (`[scheduler.nativelink.cold_start]`) gives unmeasured actions a reservation so they are enforced.
- **Disk is a declared minimum.** Each fleet sets `disk_per_instance` (the operator's choice, not derived at runtime), `disk_kb` is a minimum match key, and the scheduler packs against it. An action whose `disk_kb` exceeds every fleet's accepts no fleet (ADR-14).
- **Reserve rule.** The volume must hold a reserve beyond `disk_per_instance`; the rule is in SPECIFICATION §8.1. With `cold_start` enabled, a cold cpu, memory or disk that no fleet accepts is rejected when the scheduler is built.

## Consequences
- Several concurrent hogs can still exhaust the reserve (accepted residual risk).
- Gang's matcher and NativeLink's cannot disagree on disk, because it is set once per fleet.

## Rejected alternatives
- **`RLIMIT_FSIZE` in `gang exec`:** bounds one file; many files bypass it.
- **XFS project quotas:** need root per action.
