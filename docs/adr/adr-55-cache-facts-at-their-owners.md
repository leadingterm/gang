# ADR-55: Cache facts are measured at their owners

Status: Accepted. Amends [ADR-40](adr-40-server-recent-history.md) (farm-wide cache history) and [ADR-42](adr-42-builds-at-the-remote-execution-boundary.md) (cache-call completion and bytes).

## Context
- Operators need farm-wide cache traffic, occupancy and evictions; developers need the traffic attributable to a build.
- The leader serves cache calls, the engine owns bounded-store occupancy, and the provider owns external bucket measurements.

## Decision
- Measure cache-service message bytes as bodies stream at the client-facing observer; record the outcome once at EOF or drop. Execute remains a head observation.
- The observation sink owns coherent, mutex-protected traffic totals, including unattributed calls. Only the leader serves client calls; followers neither receive nor forward observations.
- Read bounded-store occupancy and evictions from the engine. Before the leadership boundary, capture local traffic and cumulative evictions as publisher baselines so setup-time growth is retained. Ask the backend for external-store measurements on the leader, outside the control pass.
- Keep the history and measurements in memory; after restart, the engine rescans its stores and the provider measurement is fetched again.

## Consequences
- Restarts or leadership changes can lose traffic. Dropped response streams count their partial bytes as errors.
- S3 measurements cover the whole bucket and can lag by more than a day; lifecycle expiry is not observed. Per-worker fast-CAS and worker-to-store traffic are deferred because there is no authenticated worker reporting channel; input-fetch time remains a symptom.
- The observer adds a boxed body and a shared request counter per cache call; the bounded observation channel can lose attributed calls.

## Rejected alternatives
- **NativeLink's OpenTelemetry cache wrapper:** requires a process-global meter and does not report evictions.
- **Walking the filesystem or using `statvfs`:** duplicates the engine's accounting or measures the disk rather than the store.
- **Logical blob sizes:** misstate compressed, partial and retried transfers.
- **Independent atomic counters:** snapshots can observe hits and lookups from different updates.
- **Listing S3 objects:** cost and duration grow with object count.
