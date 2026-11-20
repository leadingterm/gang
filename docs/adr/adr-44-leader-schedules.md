# ADR-44: One leader schedules and autoscales

Status: Accepted. Builds on ADR-43 and amends ADR-11. Partly superseded by ADR-59: voluntary handoff returns the leader process to standby rather than exiting.

## Context
- The scheduler and worker registry are process-local. Multiple control loops would compete to change fleet capacity.
- Controllers should continue scheduling through a transient database outage without allowing an unfenced controller to change fleet capacity indefinitely.

## Decision
- Elect one leader using a database lease with an epoch and a skew allowance. Only the leader serves execution and workers and runs the control loop; every controller serves the public listener.
- A leader that cannot renew keeps scheduling, but a monotonic lease-validity fence stops its provisioner changes once validity lapses. On retirement or a confirmed lost lease it hands off and exits.

## Consequences
- Cache and API serving can scale across controllers; scheduling and worker streams remain on one controller. A leader failure can require in-flight work to be retried.
- During a database partition, a retried action can run on both an isolated old leader and a new leader; fleet changes are fenced.

## Rejected alternatives
- **Multiple independent schedulers:** split worker fleets and competing control loops.
- **Shared scheduler state now:** requires additional coordination and per-action storage; revisit if measurements justify it.
- **Fence scheduling on database unavailability:** a prolonged database failover would also stop builds.
