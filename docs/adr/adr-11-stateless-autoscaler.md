# ADR-11: The autoscaler persists no fleet state

Status: Accepted

## Context
- The controller restarts, crashes and is redeployed while instances boot, drain or are preempted.
- Persisted fleet state can disagree with what the scheduler and provisioner actually hold, and then needs repair logic of its own.

## Decision
- **The database stores only** the certificate ledger and principal registry and the scaling audit log.
- **Each pass reads the truth.** Queued demand and connected workers (running counts, drain flags) come from `SchedulerPort::snapshot`. Instances and desired capacity come from `ProvisionerPort::supply`: ASG state on AWS, processes under `state_dir` in Local Mode. The provisioner's desired capacity is the source of truth for fleet size.
- **A pass is a pure function.** `gang-reconciler` (Ring 2, no I/O) maps policies, snapshot, supplies and the injected clock to a `Plan` of ordered steps. `gang-server` applies the steps and records audit events only after a step applied. Steady state allocates nothing.
- **Scale-in only drains.** A worker idle past `idle_timeout` beyond the instances needed gets a drain; its instance is terminated once it runs no actions or has drained for `max_drain_wait`. An instance with no worker after `boot_grace` is an orphan and is terminated; a `Pending` instance is never an orphan.
- **Steps are idempotent and orders are remembered** in process: a lost order is reissued after `boot_grace`, and duplicate audit rows are accepted. Timers use the monotonic clock and restart from zero after a restart.

## Consequences
- A restart is an ordinary pass: no migration, no split view. An idle worker may live one extra `idle_timeout`; that is the only cost.
- Decisions are deterministic given inputs and clock; tests use in-memory ports and a fake clock.
- A provisioner must report its truth cheaply and keep no fleet state; pool state is derived from its snapshot (ADR-22), and derived views are never authoritative (ADR-25).
- An end-to-end check kills and restarts `gang server` mid-scale-out and expects the same fleet.

## Rejected alternatives
- **Persisting fleet or timer state in the database:** it makes the database a second owner of facts the scheduler and provisioner already own, with drift and migration cost.
