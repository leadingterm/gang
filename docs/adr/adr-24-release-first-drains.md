# ADR-24: Drains release the provisioner's preferred tier first

Status: Accepted. Builds on ADR-23.

## Context
- With a Spot fleet and an on-demand twin, on-demand workers should drain before Spot ones, but the reconciler must not learn provider concepts. `idle_timeout` alone gates when any worker may drain.

## Decision
- **One generic bit.** `FleetInstance.release_first` is set by the provisioner: on-demand instances of an opted-in fleet when `drain_on_demand_first` is true, always false on Local.
- **Release order.** The surplus is `capacity - needed`. Eligible `release_first` workers (idle at least `idle_timeout`) drain first, up to the surplus. Other eligible workers drain only for surplus the `release_first` instances cannot absorb, counting busy and booting ones. Within a tier: longest idle first, then id.
- **Resume** (`CancelDrain`) orders by running actions descending, then `release_first` ascending (Spot resumes first), then id, so running work is protected first.
- **No proactive migration.** The fleet returns to Spot through ordinary scale-out and scale-in.

## Consequences
- With no `release_first` instance, the order is the plain longest-idle-first.
- `FleetSupply`, `PlanStep` and audit events are unchanged; the dashboard can show the on-demand share as the `release_first` count.
- Restarts recover drains from worker flags and reset idle timers (ADR-11).

## Rejected alternatives
- **A tier or `stalled` concept in the reconciler or `ProvisionerPort`:** pushes provisioner concepts into every provisioner.
- **Proactive on-demand-to-Spot migration:** adds a launch-then-drain cycle.
