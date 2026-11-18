# ADR-43: Controller state lives in a shared database

Status: Accepted. Amends ADR-11 (the durable state kept by controllers) and ADR-12 (cache sharing).

## Context
- A controller replacement must not lose the certificate ledger, principal registry, audit or in-progress logins. Multiple controllers must see the same durable facts.
- A small deployment still needs a single-controller option without a separate database service.

## Decision
- Store durable controller state behind database ports (ADR-48): the ledger, principals, scaling audit, leader lease and expiring login records.
- SQLite serves one controller; a shared database serves one or more.
- Rate limits, verified-certificate memoization, build observations and the leader's scheduler stay per process.

## Consequences
- An in-progress login can continue on another controller. Controllers can be replaced without moving local state when using a shared database.
- Database availability affects durable-state operations; rate limits apply per controller.
- Several controllers need a shared cache; the filesystem cache serves one.

## Rejected alternatives
- **SQLite replication:** retains a single writer and requires restoration on failover.
- **Only one database engine:** excludes either small local installations or users who need a managed shared database.
