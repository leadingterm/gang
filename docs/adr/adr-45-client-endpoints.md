# ADR-45: Clients use separate cache and execution endpoints

Status: Accepted. Amends ADR-36 (endpoint routing in the multi-controller shape). Partly superseded by ADR-51: health is an HTTP check of the health listener, not the port accepting.

## Context
- Cache requests may go to any controller, but execution and worker streams must reach the process holding the scheduler.
- A stable endpoint must survive replacement of a controller without requiring the leader to update DNS.

## Decision
- Advertise the public endpoint for cache and the execution endpoint for execution; `gang bazelrc` writes both. Without a dedicated execution listener, both are the same endpoint.
- Execution and worker listeners bind only on the leader, so the load balancers' health checks (ADR-51) route execution and worker traffic to it.

## Consequences
- Clients need access to the execution port as well as the serving port and must regenerate their bazelrc to use the separate endpoint.
- Followers serve cache traffic while execution and worker streams reach the leader.

## Rejected alternatives
- **Leader rewrites DNS:** needs cloud API calls and depends on resolver TTLs.
- **Forward execution through a follower:** adds a hop and complicates principal propagation.
