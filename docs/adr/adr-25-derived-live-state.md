# ADR-25: Live state is derived every pass and never authoritative

Status: Accepted. Builds on ADR-11.

## Context
- Operators need a live view of jobs, fleets, workers, scaling and principals. It must not become a second owner of facts, must not weaken the gate (ADR-05), and must work in `--insecure` Local Mode and behind a TLS-terminating proxy.
- The control pass already holds the data, and the reconciler computes per-fleet numbers privately.
- NativeLink exposes no completed-action counter.

## Decision
- **The wire types are the model.** `gang-wire`'s `state` module defines `State` and `Delta`; the Ring 2 crate `gang-state` fills them from what the pass holds, purely (no I/O, no tasks). There is no domain-to-DTO layer. Rows are bounded by the sum of `max_instances` plus configured list limits.
- **The reconciler exposes what it computes.** Each fleet's `FleetPass` carries a `FleetOutlook` (needed, demand, queued, candidates), written by every pass. Capacity, drains and phases are derived from the supply and the snapshot; nothing is computed twice.
- **Typed cause on every autoscaler step.** `ScalingCause` is a closed enum in `gang-types`, shared by the plan, the audit log and the wire. The audit log gains a nullable `cause` and two kinds, `worker_registered` and `worker_deregistered`, in one ledger with contiguous `seq`.
- **Throughput.** `State.completed` is a monotonic counter incremented by a forwarding decorator around the `WorkerScheduler` when `update_action` succeeds with a `Completed` stage. It is a metric, not state.
- **Skipped passes** keep the last good rows and report staleness. Frames carry timestamps, never durations.
- **No server-side phase history:** the UI accumulates its timeline while open.
- **`[dashboard] enabled = false`** builds no hub or refresher and serves no bundle.

## Consequences
- The state is rebuilt every pass and lost on restart (ADR-11).
- Preemption is not an autoscaler decision, so it has no event or cause.
- Fields and units are owned by `crates/gang-wire/src/state.rs` and `frame.rs`; the stream's behaviour is in SPEC §5.2 and the wire examples in docs/API.md.

## Rejected alternatives
- **Server-side phase history:** second bookkeeping.
- **A pass counter in the stream:** would force a delta per pass (ADR-27).
- **A NativeLink fork** to get counters.
