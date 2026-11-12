# ADR-23: Spot falls back to on-demand inside the fleet

Status: Accepted

## Context
- A fleet with `capacity = "spot"` gets no on-demand capacity when Spot is short. A mixed-instances ASG does not fall back; its on-demand base and percentage are static.
- Flipping the instance distribution at runtime replaces instances without a drain, breaking drain-before-terminate (ADR-11).
- A failed Spot launch surfaces only as a `Failed` scaling activity; the instance never becomes a member.
- Scale-in protection covers `InService` instances, so a lowered desired capacity then culls only `Pending` ones. Warm pools do not support Spot in mixed groups (ADR-22).

## Decision
1. **Opt-in per fleet** with a launch-only `[fleets.options.fallback]` table (ADR-15), `retry_spot_after` and `drain_on_demand_first`, on a `capacity = "spot"` fleet; otherwise rejected at parse. The `capacity` property is unchanged, so an action pinned to on-demand does not match a fallback fleet.
2. **Two ASGs only when opted in:** the Spot group named `Fleet.name` and an on-demand twin `<name>-on-demand`, both tagged. Both must protect new instances from scale-in; the adapter refuses otherwise. Terraform creates both.
3. **The provisioner owns the split.** `FleetSupply.desired` stays one number; the reconciler learns no concept of spot, on-demand or stalled capacity. One generic per-instance bit, `release_first`, carries the preference (ADR-24).
4. **Stateless detection.** Each pass derives "Spot is down" from AWS: a `Failed` Spot scaling activity within `retry_spot_after`, read with one `DescribeScalingActivities` call only when the Spot group wants more than it has. The cause is never parsed.
5. **Raising.** While Spot is up, raise Spot first. While down, an unmet Spot request is not capacity: withdraw it (desired := live members), then raise the on-demand twin. A crash after the withdraw still converges. Recovery needs no probe: after `retry_spot_after`, the next raise goes to Spot.
6. **`terminate`** finds the instance in either group and decrements that group. `max_instances` caps the total; there is no separate on-demand cap.

## Consequences
- Fallback latency is the ASG's failed-attempt time plus about two pass intervals [UNVERIFIED].
- An unmet Spot request is not withdrawn when demand falls (no raise happens); a plan-level withdraw step is deferred.
- Real-AWS confirmations I6-I9 are in SPECIFICATION §3.1.

## Rejected alternatives
- **A third `capacity` value:** changes the advertised property.
- **Flipping the distribution at runtime:** destructive.
- **EC2 Fleet with Gang-owned instances:** rebuilds ASG health, AZ spread, rebalancing and warm pools.
- **ASG `LaunchInstances`:** needs per-AZ calls, so Gang would reimplement AZ balancing.
- **Cross-fleet fallback:** pinned actions would not follow, and the demand attribution would need to know about stalls.
- **A `stalled` flag in `FleetSupply` or `PlanStep`;** parsing `StatusMessage`; a "pending too long" signal; proactive on-demand-to-Spot migration; an on-demand cap.
