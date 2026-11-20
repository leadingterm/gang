# ADR-45: Clients use one cache and execution endpoint

Status: Accepted. Amends ADR-36 (endpoint routing in the multi-controller shape). Partly superseded by ADR-51: health is an HTTP check of the health listener, not the port accepting.

## Context
- Cache, execution, API and dashboard traffic must reach the process holding the scheduler.
- A stable endpoint must survive replacement of a controller without requiring the leader to update DNS.

## Decision
- Advertise one public endpoint for cache and execution; `gang bazelrc` writes it as both `remote_cache` and `remote_executor`.
- The leader serves the public and worker listeners. All load-balancer target groups use `/ready` health checks to route client and worker traffic to it.

## Consequences
- Clients need only the public port, 443 in the deployment.
- Before first promotion, a controller returns gRPC `UNAVAILABLE` or HTTP 503 rather than forwarding. After promotion admission remains enabled through a lease lapse even when `/ready` is 503; hand-off drains admitted connections.

## Rejected alternatives
- **Leader rewrites DNS:** needs cloud API calls and depends on resolver TTLs.
- **Forward through a follower:** adds a hop and complicates principal propagation; it is not implemented.
