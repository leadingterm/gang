# ADR-59: Cooperatively hand leadership to a prepared newer controller

## Status

Accepted, amended: restart-stable host priority and cooperative yield remain; routability warmup, healthy contenders, standby shared services and same-process return to standby are superseded. Builds on ADR-44 and ADR-52.

## Context

A replacement must take authority without overlapping provisioner mutations or waiting for a termination notification that can arrive after deregistration. Process registration order is not host age: restarting an older host must not rank it above its replacement.

## Decision

A prepared newer contender requests cooperative yield without waiting for load-balancer routability. Priority belongs to the host, not the process: AWS uses immutable launch time and instance ID, while other hosts retain their first durable registration order. A fresh holder identity and lease epoch fence authority; no contender steals a live lease. The outgoing process drains its own admitted work, then exits. The protocol is specified in [the specification](../SPECIFICATION.md#5-autoscaler-state--restart).

NativeLink transport readiness alone is not takeover readiness. Complete readonly history loading and import, post-import fingerprints, and revocation/dashboard reload precede `Prepared`, which registers this instance in its ASG target groups and observes registration complete while the launch hook is pending and health is 503. Only then may it request yield and acquire. The prepared revocation snapshot may have become stale, so synchronization after acquisition is mandatory before leader readiness or `Serving`, under the existing promotion deadline and lease hold; failure closes promotion. `Serving` completes the launch hook after fenced activation and promotion in the final call budget. Preregistration removes registration propagation from the switch window, which still includes subsequent health detection. The engine starts from that pre-request history snapshot and its own learning; late old-engine learning may wait until the next import. Periodic persistence reconciles canonical metadata without overwriting newer or deleted heads with unchanged prepared values or resetting the live engine.

## Consequences

Only the active lease holder is healthy and admits fresh services. GOAWAY and marked refusals move never-started requests, not executing attempts, to the successor. Old workers and admitted streams remain with the old scheduler until terminal or their dispatch-stamped deadlines; expiring protection prevents the successor from treating old busy workers as free or terminating them. Crashes retain retry and possible re-execution, not scheduler reattachment. systemd restarts a drained process as a fresh passive process on the old host; a retiring host cannot reacquire.

The ASG's `OldestInstance` policy aligns ordinary scale-in with newest-host leadership. Refresh cannot retire the old holder before dispatch freezes, preserving the lifecycle-hook budget for drain. The outgoing listener refuses fresh requests until actual nonrouting in every target group, not merely successor health.

A drained active host with no remaining pending or in-service peer may exit after becoming unhealthy so systemd can recover an in-place restart or abandoned successor. This exception does not claim that NLB fail-open routing stopped; actual retirement or a viable overlapping peer still requires the full nonrouting barrier. A refused lifecycle completion or heartbeat is reconciled against one fresh platform-state read, never assumed successful while its waiting action remains.

AWS keeps established flows alive with both [deregistration](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html#deregistration-delay) and [unhealthy-target](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html#unhealthy-target-connection-termination) connection termination disabled, not by sizing deregistration delay to an action. Lifecycle heartbeats are subject to the [global limit](https://docs.aws.amazon.com/autoscaling/ec2/userguide/lifecycle-hooks.html#lifecycle-hook-considerations), the lesser of 48 hours and 100 times the hook heartbeat timeout.

Configuration-only changes use this same path: changed rendered bytes publish a new immutable generation and update the controller launch template. Qualification therefore includes a same-binary fleet-maximum change under held work, retaining both generated selectors and proving successful original client commands, original attempt ownership and the successor's changed limit. It does not require a separate deploy procedure or a second admission mechanism.

## Rejected alternatives

- **Wait for a healthy contender:** routes fresh traffic to a controller that cannot admit it and delays yield.
- **Steal a live lease:** risks overlapping provisioner authority.
- **Rank each process by registration order:** a restarted old host would outrank its replacement.
