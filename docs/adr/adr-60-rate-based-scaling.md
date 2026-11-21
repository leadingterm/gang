# ADR-60: Scale on source load and backlog, not queue age

## Status

Accepted for v0.3.4; its payback purchase, nominal slot and scale-out claims below are superseded. Current resource-only scaling, resource wait simulation, retained rate profiles and N−1 scale-in are specified in [SPECIFICATION §§1.2, 5](../SPECIFICATION.md#5-autoscaler-state--restart).

## Context

On 2026-11-19, production x86-64 episodes A, C and D jumped to six instances for short work; B held distributed thin load for roughly two hours, while E sustained work through its window. Queue age did not distinguish a static Bazel batch from refill, and idle-only release could not reclaim partly busy workers.

Corrected evidence is successful ASG launch StartTime through termination EndTime, clipped to whole-fleet UTC windows. It measures lifecycle exposure, not exact billing:

| Episode | Window, 2026-11-19 UTC | Observed instance-minutes |
|---|---|---:|
| A | 12:00–12:45 | 128.6 |
| B | 13:20–15:27 | 738.7 |
| C | 15:28–15:55 | 143.9 |
| D | 15:55–16:25 | 100.0 |
| E | 16:27–19:56 | 1227.8 |

The earlier audit-cohort totals 181/1034/162/167/1837 are retracted: they missed external terminations and window clipping. Sources: the rate-based-scaling proposal §1, its errata E1 and final owner clarifications of 2026-11-20. No historical per-pass action/source/rate trace exists; all after-policy exposure figures are modeled, not production savings. CPU utilization is not action-slot utilization.

## Decision

- Adopt the user's 2026-11-19 choices: default `payback_percent = 50`, source = authenticated principal (CI repository/event class via GitHub `sub`, humans per person), durable source activity profiles without principal strings, and draining partly busy workers.
- Estimate offered resource load from event-time verified completions and sampled in-flight counts. Separate persistent refill from transient queued resource-work. Persistent queued reservations are a raw demand candidate, not a payback exemption: bound purchases componentwise by all duration-weighted queued work and qualified genuine future arrivals, crediting readiness, serving and booting capacity. Require recent completion-backed refill in the current nonempty cohort; historical activity, a growing pre-completion upload or running load alone is not a forecast. Preserve four resource dimensions through summation, reachability and existing bounds; remove candidate trials and the 2B stream trigger.
- Apply the later ruling: keep load distributed, never pack. Cordon one whole physical instance only after N−1 fits the configured waterline continuously for the dwell; choose the instance that empties soonest. Resume owned capacity on returning load before buying. Idle timeout is an upper bound, not a minimum hold.

## Consequences

Short bursts need not reach the ceiling; sustained or learned-heavy sources can justify capacity without waiting for a queue-age override, but source history alone cannot make a cheap static batch pay for maximum capacity. With `B = 45s` and idle timeout `10m`, default `H = 322.5s` and persistence threshold `B + H = 367.5s`; examples using `H = 645s` are non-default 100% payback. Live estimators and dwell restart per term; learned profiles survive compatible fenced flushes. `max_cordons = 0` permits a soak with idle drains intact. After the first `utilization` audit row, v0.3.3 readers reject its cause, so downgrade is unsupported; earlier rollback must also restore the old configuration.

## Rejected alternatives

- Longer stream timer or concurrency-only sizing: still treats long static queues as new arrivals.
- Client-declared repository header or invocation-keyed profiles: adds client surface or discards cross-build source continuity.
- Packing or idle-only scale-in: violates distributed placement or retains thin busy fleets.
- Candidate-count simulation or completion-time-only optimization: unnecessary trial dispatch and an incentive to buy maximum concurrency for short bursts.
