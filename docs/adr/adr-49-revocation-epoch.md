# ADR-49: Revocations propagate by epoch polling

Status: Accepted. Builds on ADR-43.

## Context
- Revocation through one controller must reach the others. Database-specific notifications differ and can be missed during disconnects.

## Decision
- Each certificate revocation, principal revocation and principal restoration advances a durable epoch atomically with the change.
- Every controller polls that epoch. On a change it reloads the revoked certificates and principals and applies the snapshot; the controller processing a change applies it locally as well.

## Consequences
- With a healthy database, propagation waits for a poll and reload; failed reads are retried on later polls, so an outage can delay it longer.
- A small recurring read per controller avoids backend-specific notification infrastructure.

## Rejected alternatives
- **Database notifications or streams:** backend-specific and still need a polling backstop after a disconnect.
- **An incremental change log:** adds schema for sets expected to change infrequently.
