# ADR-36: A publicly trusted endpoint re-encrypts to Gang's TLS

Status: Accepted. Amends ADR-09's consequence on public-certificate proxies; transport stays `tls` (ADR-03).

## Context
- Browsers must open the dashboard without a certificate warning or CA installation; Bazel and `gang` should use system trust.
- Workers keep mTLS on the private worker listener with the Gang CA.
- `transport = "plaintext"` turns TLS off on both listeners, and `mtls` requires `tls`.
- `GET /v1/server`'s `caCertificate` served both clients and enrolling workers.

## Decision
- **Re-encrypt to Gang.** A publicly trusted terminator re-encrypts to the public listener. The deployment module's optional `load_balancer` uses an ACM certificate on a Network Load Balancer, TLS 1.3 to Gang, and ALPN `h2` then `http/1.1`. Gang still terminates TLS with its own CA on both listeners.
- **Describe endpoint trust.** `[server] endpoint_trust = "ca" | "system"` says what clients see at `endpoint`. Under `system`, `GET /v1/server` omits `caCertificate`, so `gang login`, the credential helper, `gang bazelrc`, `gang doctor` and Bazel use system roots without a client change.
- **Issue the worker's CA with its certificate.** Workers take the CA from `POST /v1/certificates`'s `caCertificate`, never from `/v1/server`.
- **Keep enrollment private.** The deployment resolves `hostname` to the controller inside the VPC using a Route 53 private hosted zone. Workers reach the controller's own listener, verified by the pinned Gang CA, and enrollment never crosses the load balancer.
- **Reject missing worker trust.** A pinned worker without enrollment (`workers.none` with `ca_pin`) has no CA source under `system`, so parsing rejects that combination with `PinnedWorkersNeedAdvertisedCa`.

## Consequences
- There is one public path. The controller's public port accepts only the load balancer and the workers; its Elastic IP stays its egress address.
- The TLS listener's idle timeout is fixed at 350 s; the 30 s HTTP/2 keepalive pings and 10 s dashboard heartbeat stay under it.
- The certificate validation record is published out of band, and the first apply waits for it.
- Per-address rate limits still see client addresses because instance targets preserve them. Clients of earlier versions work unchanged.

## Rejected alternatives
- **Plaintext behind the load balancer.** It needs per-listener transport and sends workload proofs unencrypted inside the VPC.
- **A second `/v1/server` field telling clients to ignore `caCertificate`.** Earlier clients would keep pinning the CA and fail.
- **Workers trusting system roots to enroll.** This moves their trust anchor from IAM and KMS to the web PKI.
- **HTTPS health checks.** They would spend the open routes' rate-limit buckets.
- **A dedicated CA route for unenrolled pinned workers.** It adds API surface for an edge mode.
