# ADR-59: Cooperatively hand leadership to a routable newer controller

## Status

Accepted. Builds on ADR-44 and ADR-52: voluntary handoff no longer requires the leader process to exit, and lease rows coordinate successor priority as well as the existing epoch fence.

## Context

A replacement must be reachable before the current leader stops admitting work. Terminating the leader first leaves a gap while the replacement becomes healthy, but allowing a newer controller to seize a live lease risks overlapping authority. Process registration order is not host age: if replacement B registers after host A, then A crashes and restarts, a fresh process counter would rank A above B and reverse the handoff.

## Decision

A prepared newer contender requests a cooperative handoff only after it has become routable. Priority belongs to the host, not the process: AWS uses immutable launch time and instance ID, while other hosts retain their first durable registration order. The holder identity is fresh per process and remains fenced by the lease. The existing leader yields safely, and the same process returns to standby for a later term. The protocol and deployment timing are specified in [the specification](../SPECIFICATION.md#5-autoscaler-state--restart).

## Consequences

Rolling replacement can transfer leadership before the old instance retires. A prepared controller serves real shared services before promotion and through drain and yield; readiness can identify a routable contender that still refuses leader-only work. Leadership refusals retire HTTP/2 connections gracefully, including previously used connections, while admitted streams drain; see [the specification](../SPECIFICATION.md#61-gang-server-listeners). Voluntary yield, including a takeover request during shutdown drain, snapshots learned history before release rather than waiting for stream drain; measurements arriving after release are dropped, so the last moments before handover may be lost. Without such a request, shutdown retains the lease through drain and the final flush. A blocked mutation can delay handoff rather than compromise fencing.

## Rejected alternatives

- **Terminate the leader before promoting the replacement:** creates an avoidable routing gap.
- **Steal a live lease:** risks overlapping provisioner authority.
- **Keep every prepared standby healthy:** routes traffic to passive controllers without a pending takeover.
- **Rank each process by registration order:** a restarted old host would outrank its replacement.
