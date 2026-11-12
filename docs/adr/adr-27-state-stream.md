# ADR-27: State is streamed as snapshot plus deltas over SSE

Status: Accepted

## Context
- The dashboard is one-way. A full frame is hundreds of KB at 1 Hz for large fleets. The stream must pass the per-request gate with the session cookie (ADR-28), work over HTTP/1.1 and h2 (ADR-09), and survive a lagging viewer.

## Decision
- **One snapshot endpoint and one SSE stream.** Every connect starts with a snapshot; then deltas carry only changed parts (keyed upsert and remove), ticks carry the clock for staleness, and a final `closed` event with a reason (`REVOKED`, `EXPIRED`, `VIEWER_CAP`, `SHUTDOWN`) precedes a deliberate close. There is no replay, ring or `Last-Event-ID`; a client reconnects when a delta's `base` differs from its `seq`.
- **Bounded fan-out.** `StateHub` builds each delta once, as shared `Bytes` through a bounded broadcast. With no viewers it only builds. A lagging viewer is resynced with a fresh snapshot, never closed. Memory is bounded by `lag_frames × max delta`. Beyond `max_viewers` the response is a stream that sends `closed`.
- **Unchanged passes send no delta;** `seq` counts changes, not passes.
- **One event mechanism.** The control loop writes the audit row, then appends the event to the hub; the hub's recent events are always a suffix of the audit log, keyed by ledger `seq`.
- **Streams end with the credential.** Each stream selects on the revocation epoch (ADR-08) and sleeps until `valid_until` (ADR-05), then sends `closed`.
- **Streaming bodies.** API response bodies are boxed, and `request_timeout` bounds the response head, not the body.

## Consequences
- Per pass cost is O(workers + instances + queued) once, shared by all viewers.
- `EventSource` hides HTTP statuses, so a browser classifies a failed connect through the session endpoint (ADR-32).
- Dead peers are closed by HTTP/2 keepalive or a failing heartbeat write; the stream is never compressed.

## Rejected alternatives
- **WebSocket:** needs an h1 upgrade or RFC 8441, and escapes per-request gating after the upgrade.
- **Replay with `Last-Event-ID`;** a polling `stream_max_age` instead of authenticated expiry; a pass counter.
