# ADR-22: Warm pools hold stopped standby instances

Status: Accepted. Builds on ADR-11 and ADR-23.

## Context
- A fleet may keep stopped instances in an EC2 Auto Scaling warm pool. Scale-out starts them first; scale-in stops them.
- Only a scale-in by the group returns an instance to the pool. `TerminateInstanceInAutoScalingGroup` terminates and never reuses. The group picks its victim by termination policy and never picks a protected instance.
- Warm instances are outside desired capacity, a warm start counts toward it, pools need an EBS root and on-demand capacity, and instance store is erased on stop. EC2 `LaunchTime` resets on every start.
- IMDS `autoscaling/target-lifecycle-state` announces `Warmed:*`.
- Gang drains a chosen worker, then terminates exactly that instance; the reconciler holds no pool state.

## Decision
1. **Terraform owns the pool.** `worker-fleet` takes `warm_pool_size` per fleet (0 = none). Above 0 it sets a `Stopped` pool with reuse on scale-in, scale-in protection and forced pool deletion on the fleet's on-demand group (the `-on-demand` twin of a fallback fleet, ADR-23). Gang config gains nothing.
2. **Gang reads the regime from AWS.** A group with `reuse_on_scale_in` uses the release path. Reuse without `new_instances_protected_from_scale_in` is `AwsError::Config`, so the group can never pick an arbitrary victim.
3. **Release, not terminate.** For an `InService` member of a reuse group, `terminate` lifts the instance's protection, then lowers desired capacity to `keepers + in_flight` computed from the snapshot, so the group's own scale-in returns that instance to the pool. A repeat writes nothing. `Pending` members and groups without reuse keep the atomic terminate. Release is refused (`Config`) while another `InService` member is unprotected and the group is not already lowering.
4. **The worker parks.** `NodeLifecyclePort::parked()` reads the target lifecycle state every `[worker] lifecycle_poll_interval`; while `Warmed:*` the worker neither enrolls nor registers. Every start is a fresh process with empty scratch and a new enrollment; there is no warm cache.
5. **The reconciler is unchanged.** A warm-starting instance is `Pending` with a fresh `launched_at`, so it is never an orphan within `boot_grace`.

## Consequences
- The release path is the only addition to `terminate` and is a pure function of the AWS snapshot. A real-AWS confirmation list (I1-I5) is in SPEC §3.1.
- Residual: a process death between the two calls, followed by a demand rebound, leaves a serving instance unprotected. The next release counts it as leaving; the cost is one worker's actions, which Bazel retries.

## Rejected alternatives
- Local warm-instance state; `DetachInstances` (never returns to the pool); `[[fleets]]` pool options (two owners for one fact); one release path for every fleet (would rewrite the crash-safe atomic terminate); a persistent warm cache; `Hibernated` (resumes a stale certificate and scheduler connection).
