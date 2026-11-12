# ADR-34: Event history is paged; live events come only from the stream

Status: Accepted. Replaces sequence-windowed scaling-event queries and the long-poll `wait`.

## Context
- Scaling events were read three ways: the state stream, a `since`/`before`/`limit` query with a long-poll `wait`, and a separate `after`/`limit` cursor for principals. The long-poll duplicates the stream, and the e2e tail slept in a loop. Google has no collection-watch convention.

## Decision
- **Every List uses one shape:** request `pageSize` (default 100, maximum 1000) and an opaque keyset `pageToken`; response is the collection key plus `nextPageToken`. Order is fixed and documented, with no `orderBy`. `filter` exists only on principals, and only for revoked ones.
- **Events split by direction.** Live events come only from the state stream (ADR-27), whose snapshot and deltas carry full `Event` resources. History comes from `List events`, newest first.
- **Identity.** Each event is identified by its ledger `seq`, strictly increasing; clients de-duplicate by resource name.
- **Consumers.** The UI's "load earlier" follows the page token. E2E tests follow the stream with an `EventFollower`, awaiting a predicate on frames, never sleeping or polling.

## Consequences
- The UI holds a contiguous suffix of the ledger: its first history request asks for `held + 200` events (at most 1000) and drops events not older than its oldest.
- Roles paginate in memory over the configured list.
- The follower's behaviour is in SPEC §6.5.

## Rejected alternatives
- **Keeping the long-poll as a fallback `wait`.**
- **`since` tailing for live updates:** a reader more than `limit` events behind loses events and must page the gap.
