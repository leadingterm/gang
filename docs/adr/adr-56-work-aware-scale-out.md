# ADR-56: Scale out for work that repays readiness

## Status

Accepted, partly superseded by [ADR-60](adr-60-rate-based-scaling.md) for v0.3.4: candidate-count payback simulation, the 2B stream rule, readiness-based unknown durations and the `boot_grace` readiness prior are historical, not current policy. ADR-60 keeps readiness measurement, reachability and one display-only final dispatch. Learned reservation and duration histories are persisted as described in SPECIFICATION §5; the original memory-only history and rejection of database persistence no longer apply. Readiness remains term-local.

## Context

Concurrency-only sizing can repeatedly buy booting instances for a short burst that existing capacity could drain. New capacity takes time to become ready; longer or continuously arriving work can still need full concurrency.

## Decision

Within the existing concurrency ceiling, simulate queued work and buy the largest set of new instances that repays readiness in work served or completion time saved, with a reachability floor. Evaluate every candidate count: payback is not monotone across overlapping fleets. The detailed rules are in SPECIFICATION §5.1.

Use Gang's learned execution slot durations and measured fleet readiness, with readiness as the unknown-duration prior and `boot_grace` as the readiness prior. Learned durations persist through the state database; readiness samples and backlog age remain local to the leadership term.

## Consequences

Short bursts can use fewer instances, while sufficiently long backlogs grow to the concurrency ceiling. A continuously refilling queue grows to that ceiling after 2B waiting on serving capacity; the later ramp for closed-loop builds is accepted. Duration history survives restart or failover subject to expiry, eviction, compatibility and the last successful flush. Without duration history, empty or idle fleets retain concurrency sizing and busy fleets can buy less, never more.

## Rejected alternatives

- Delay or settle every scale-out: delays cold starts without ensuring the resulting capacity repays its boot.
- Forecast arrivals: a growing queue can produce the same overbuying ratchet.
- ~~Patch NativeLink or persist history in the database: the existing API and process-local observations suffice.~~ Superseded for learned reservation and duration history; see SPECIFICATION §5. Readiness and backlog age remain term-local.
- Optimize completion time alone: it favors maximum concurrency even for short bursts.
