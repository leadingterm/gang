# ADR-52: The PostgreSQL leader lease is a conditionally updated row

Status: Accepted, as amended by ADR-59: the lease row also coordinates restart-stable successor priority for cooperative handoff; it does not transfer execution ownership.

## Context
- `gang-postgres` implements `LeaderLeasePort`: acquire with `stale_before`, renew, release and epoch fencing. It keeps no session state.

## Decision
- Keep one `leases` row with `name`, `holder`, `epoch` and `expires_at`. Acquire uses `INSERT ... ON CONFLICT DO UPDATE ... WHERE holder IS NULL OR expires_at < stale_before`, incrementing the epoch; renew and release use `UPDATE` conditioned on the holder and epoch. Time comes from the caller, as with every backend.

## Consequences
- Shared backends use a clock-based TTL and epoch fence; SQLite instead holds an exclusive file lock. No session state is needed, so a transaction-mode pooler is not excluded (none is exercised at runtime); dropping a connection does not itself end leadership: the TTL does.
- Schema initialization uses only the transaction-scoped `pg_advisory_xact_lock`.
- Every write (lease acquire, renew and release, expiring-record replace, remove and purge, issuance and principal observation records) is one transaction with `SET LOCAL statement_timeout` and `idle_in_transaction_session_timeout`; reads are single statements. A cancelled call discards its connection and the pool aborts its driver at once. A transaction whose `COMMIT` was not yet sent is rolled back by the server when it reads the closed connection. Once `COMMIT` has been sent, cancellation guarantees nothing: the write may still apply, as after a cut link. Residual server work is bounded by `statement_timeout`.

## Rejected alternatives
- **pg_advisory_lock (session advisory lock):** tied to one server session, it breaks transaction-mode poolers, ends or survives with the TCP session rather than the TTL, and carries no epoch for fencing.
- **LISTEN/NOTIFY for hand-off:** session-bound and lossy; ADR-49 already rejects it for revocations.
