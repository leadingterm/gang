# ADR-20: Disk is a declared fleet minimum

Status: Partly superseded by the disk pressure default, which kills only under worker disk pressure and requeues; see [SPECIFICATION §3.2, Disk limits](../SPECIFICATION.md#32-execution-engine--scheduler-gang-engine-gang-scheduler-gang-cas).

## Context
- Actions need a disk reservation for admission and packing, including before measurements exist.
- A worker needs scratch reserve for pressure protection and fast-writer overshoot.
- Gang's matcher and NativeLink's must agree on what disk an action needs and a worker has.

## Decision
- **Pressure enforcement by default.** `[actions.disk]` selects the disk policy; `[actions.cold_start]` gives unmeasured actions a reservation. The current enforcement and requeue rules are in SPECIFICATION §3.2.
- **Disk is a declared minimum.** Each fleet sets `disk_per_instance` (the operator's choice, not derived at runtime), `disk_kb` is a minimum match key, and the scheduler packs against it. An action whose `disk_kb` exceeds every fleet's accepts no fleet (ADR-14).
- **Reserve rule.** The volume must hold a reserve beyond `disk_per_instance`; the rule is in SPECIFICATION §8.1. With `cold_start` enabled, a cold cpu, memory or disk that no fleet accepts is rejected when the scheduler is built.

## Consequences
- Several concurrent hogs can still exhaust the reserve (accepted residual risk).
- Gang's matcher and NativeLink's cannot disagree on disk, because it is set once per fleet.

## Rejected alternatives
- **`RLIMIT_FSIZE` in `gang exec`:** bounds one file; many files bypass it.
- **XFS project quotas:** need root per action.
