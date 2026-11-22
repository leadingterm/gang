# ADR-49: Revocations propagate by epoch polling

Status: Accepted. Builds on ADR-43.

## Context
- Revocation through one controller must reach the others. Database-specific notifications differ and can be missed during disconnects.

## Decision
- Each certificate revocation, principal revocation and principal restoration advances a durable epoch atomically with the change.
- Every controller polls that epoch. On a change it reloads the revoked certificates and principals and applies the snapshot; the controller processing a change applies it locally as well.
- Preparing and passive controllers do not admit fresh shared requests. Every authenticated request checks the local set. Proof verification for issuance also checks the durable principal record, including on promoted leaders; failed lookups fail admission closed.
- The prepared snapshot can become stale before acquisition. After acquiring the lease, the controller synchronizes certificate and principal revocations before leader readiness or admission, within the existing promotion deadline and lease hold; failure prevents promotion. Polling alone is not an admission barrier. Once promoted, it uses its local set and remains fail-static during a database outage, even after lease expiry.
- Before a planned lease release, revocation and restoration handlers recheck admission under the same local lock that stops admission. Already-admitted mutations finish before release; delayed handlers cannot commit after the successor's admission snapshot.

## Consequences
- Existing streams, certificate cutoffs and restorations propagate to other controllers on a poll and reload; failed reads are retried on later polls, so an outage can delay them longer. Issuance observes committed revocations through durable lookup without waiting for a poll.
- A small recurring read per controller avoids backend-specific notification infrastructure.

## Rejected alternatives
- **Database notifications or streams:** backend-specific and still need a polling backstop after a disconnect.
- **An incremental change log:** adds schema for sets expected to change infrequently.
