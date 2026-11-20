# ADR-51: Load balancers check controller health over HTTP; clients keep connections alive

Status: Accepted. Partly supersedes ADR-45 (its TCP health checks).

## Context
- A frozen controller still completes TCP handshakes, so TCP health checks keep it in rotation.
- Bazel's Execute stream has no deadline, and an NLB resets an unhealthy target's flow only when the client sends a packet; connection termination is off for draining anyway. A frozen leader therefore held every build until it resumed.

## Decision
- Every controller serves a plaintext HTTP health listener (`health_listen`) on the runtime that serves its other listeners: `/ready` gives 200 only while the instance is serving and leading with a valid lease and its public and worker listeners serving; otherwise it gives 503, including from the start of hand-off. Every target group checks `/ready`. This readiness is distinct from admission, which remains enabled after first promotion even if lease validity lapses.
- `gang bazelrc` writes `--grpc_keepalive_time=10s` and `--grpc_keepalive_timeout=30s`. Gang's listeners (hyper's HTTP/2 server) enforce no ping policy, so they need no setting for it; a test pings each listener every 10 s through an hour-long pending request.
- `gang_types::health` defines the health check and the keepalive timings. The load-balancer module's `health-check.json` renders them for Terraform, and a test checks that it matches.
- Config parsing enforces `keepalive timeout > ttl + clock_skew + renew_interval + threshold × interval`, so a client's first reconnect after detection finds the frozen controller unhealthy and the new leader healthy.

## Consequences
- A frozen controller loses clients 30–40 s after freezing.
- Followers are outside client and worker load-balancer rotation and are not replaced: the controller Auto Scaling group uses EC2 health checks.
- One more port is open from the load balancers' security groups: `health` in `infra/terraform/modules/network/ports.json` (8080), the single definition read by Terraform and the NixOS firewall, not user configurable; the e2e balancer models the same check.
- Operators who raise lease timings past the invariant get a config error.

## Rejected alternatives
- **HTTPS health checks:** NLB marks TLS-1.3-only targets unhealthy.
- **Keepalive alone:** a reconnect can land on the frozen target again, since it passes TCP checks.
- **Health checks alone:** an idle pinned stream never sends the packet that would be reset.
- **Connection termination on:** would cut draining hand-offs and still needs a client packet.
