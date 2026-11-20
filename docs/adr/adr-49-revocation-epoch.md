# ADR-49: Revocations propagate by epoch polling

Status: Accepted. Builds on ADR-43.

## Context
- Revocation through one controller must reach the others. Database-specific notifications differ and can be missed during disconnects.

## Decision
- Each certificate revocation, principal revocation and principal restoration advances a durable epoch atomically with the change.
- Every controller polls that epoch. On a change it reloads the revoked certificates and principals and applies the snapshot; the controller processing a change applies it locally as well.
- Unpromoted controllers check the durable principal record for authenticated shared requests. All proof verification for issuance also checks the durable record, including on promoted leaders; failed lookups fail admission closed. Every request also checks the local set.
- Before leader admission, a controller reloads certificate and principal revocations. Because revocation mutations are leader-only, the promoted leader admits requests using its local set and remains fail-static during a database outage, even after lease expiry.

## Consequences
- Existing streams, certificate cutoffs and restorations propagate to other controllers on a poll and reload; failed reads are retried on later polls, so an outage can delay them longer. Unpromoted shared request admission and issuance observe committed revocations through durable lookup without waiting for a poll.
- A small recurring read per controller avoids backend-specific notification infrastructure.

## Rejected alternatives
- **Database notifications or streams:** backend-specific and still need a polling backstop after a disconnect.
- **An incremental change log:** adds schema for sets expected to change infrequently.
