# ADR-42: Builds are observed at the remote-execution boundary

Status: Accepted.

## Context
- Users ask why a build was slow and how often the cache hit. Gang sees every remote call; Bazel's Build Event Service would add client truth.

## Decision
- **A build is (principal, `tool_invocation_id`).** The principal is the gate's authenticated caller; the invocation is client-asserted, so keying by both makes cross-principal injection impossible.
- **Two observers, one channel.** A tower layer inside NativeLink's `OtlpLayer` records `GetActionResult` (OK hit, `NOT_FOUND` miss), `UpdateActionResult` and `Execute`; the completion decorator reads the operation's origin before forwarding each `Completed` update and records its `ExecutionMetadata`. The layer stamps the principal into the request context so NativeLink's operation origin carries it.
- **Loss is counted.** A full channel increments `unrecorded`; nothing blocks a gRPC call.

## Consequences
- Zero client configuration; cache-only clients are covered.
- Local and disk-cache hits, local actions, critical path and build success are not visible; the UI says so.
- A deduplicated execution belongs to its first submitter; later submitters show it as `joined`.

## Rejected alternatives
- **Build Event Service:** per-user flags, ingests command lines and environment, a large schema to parse; revisit only as an allow-listed add-on.
- **AC store decorator:** no caller identity.
- **Polling completed operations:** no "completed after" filter; misses short retention.
