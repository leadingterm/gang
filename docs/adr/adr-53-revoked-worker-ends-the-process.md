# ADR-53: A revoked worker certificate ends the worker; a node is kept out by principal revocation

Status: Accepted. Builds on ADR-07 and ADR-08.

## Context
- Revoking a serial cuts off one certificate, not the node (ADR-08).
- A worker that kept retrying with a revoked certificate hid the revocation from the operator. The controller lingers for `refusal_linger` after refusing so the client can read the `certificate_revoked` alert, and the dialer surfaces it.

## Decision
- The embedded worker stops reconnecting, kills and cleans its actions, and exits 1 with `CertificateRevoked`. An observed revocation takes precedence over a concurrently ready stop or clean worker completion.
- The supervisor starts a replacement worker, which enrolls a new generation: `gang-worker.service` restarts after `RestartSec=5`; Local Mode replaces workers through its provisioner.
- To keep a node out, revoke its principal: enrollment is refused, the worker never registers, and the reconciler terminates the instance as an orphan after `boot_grace`.
- Other handshake refusals keep NativeLink's backoff.

## Consequences
- AWS service restarts are delayed by 5 s. Local Mode backs off early worker exits using `relaunch_backoff` (default 30 s).
- Serial revocation alone does not keep a node out.
- The worker's `fail` record names the alert.

## Rejected alternatives
- **Retrying with the revoked certificate until it expires:** it hid revocation from the operator.
