# ADR-59: Cooperatively hand leadership to a prepared newer controller

## Status

Accepted, amended: restart-stable incarnation priority and cooperative yield remain; ECS rolling services replace controller ASG lifecycle hooks and nonrouting observation. Routability warmup, healthy contenders, standby shared services and same-process return to standby are superseded. Builds on ADR-44 and ADR-52.

## Context

A replacement must take authority without overlapping provisioner mutations or waiting for a termination notification that can arrive after deregistration. Process registration order is not host age: restarting an older host must not rank it above its replacement.

## Decision

A prepared newer contender requests cooperative yield without waiting for load-balancer routability. Priority belongs to the incarnation, not the process: AWS uses immutable instance launch time or ECS task creation time with a task-ARN digest, while local controllers retain their configured stable identity and first durable registration order. A fresh holder identity and lease epoch fence authority; no contender steals a live lease. The outgoing process drains its own admitted work, then exits. The protocol is specified in [the specification](../SPECIFICATION.md#5-autoscaler-state--restart).

NativeLink transport readiness alone is not takeover readiness. Complete readonly history loading and import, post-import fingerprints, and revocation/dashboard reload precede any cooperative yield request. The candidate remains unhealthy until fenced activation and promotion. The prepared revocation snapshot may have become stale, so synchronization after acquisition is mandatory before leader readiness, under the existing promotion deadline and lease hold; failure closes promotion. Final promotion has no external AWS wait. The engine starts from that pre-request history snapshot and its own learning; late old-engine learning may wait until the next import. Periodic persistence reconciles canonical metadata without overwriting newer or deleted heads with unchanged prepared values or resetting the live engine.

## Consequences

Only the active lease holder is healthy and admits fresh services. GOAWAY and marked refusals move never-started requests, not executing attempts, to the successor. Old workers and admitted streams remain with the old scheduler until terminal or their dispatch-stamped deadlines; expiring protection prevents the successor from treating old busy workers as free or terminating them. Crashes retain retry and possible re-execution, not scheduler reattachment. A restarted older incarnation cannot outrank its successor.

ECS launches the replacement before stopping the old task and rolls back a deployment whose candidate never becomes healthy. The old holder freezes dispatch, installs original-worker protection and releases authority before the successor promotes. ECS then sends SIGTERM; task, capacity-host agent and runtime share an explicit stop budget exceeding the admitted attempt budget.

AWS keeps established flows alive with both [deregistration](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html#deregistration-delay) and [unhealthy-target](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html#unhealthy-target-connection-termination) connection termination disabled, not by sizing deregistration delay to an action.

Configuration-only changes use this same path: changed rendered bytes publish a new immutable generation and roll the ECS service. Qualification therefore includes a same-binary fleet-maximum change under held work, retaining both generated selectors and proving successful original client commands, original attempt ownership and the successor's changed limit. It does not require a separate deploy procedure or a second admission mechanism.

## Rejected alternatives

- **Wait for a healthy contender:** routes fresh traffic to a controller that cannot admit it and delays yield.
- **Steal a live lease:** risks overlapping provisioner authority.
- **Rank each process by registration order:** a restarted old host would outrank its replacement.
