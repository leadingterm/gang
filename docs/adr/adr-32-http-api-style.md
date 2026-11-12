# ADR-32: The HTTP API is resource-oriented and additive within /v1

Status: Accepted. Amends the route, naming and body conventions of the earlier API.

## Context
- The API grew route by route: kebab-case paths with snake_case bodies, unix-ms and `*_ms` times, config-format durations, `null` versus omitted, an RPC-shaped revocation with a second verb path, three paging dialects, and response types that forbid unknown fields and so block additive change.

## Decision
- **Resource-oriented per AIP-121/122/123, versioned `/v1` per AIP-185.** Names are `collection/{id}` or singletons; methods are standard Get and List plus single-verb custom methods (`:revoke`, `:restore`, `:watch`, `:authenticate`). Each resource is one lowerCamelCase noun. State transitions are idempotent custom methods on the resource.
- **JSON conventions,** owned in `gang-wire`: lowerCamelCase fields, RFC 3339 `*Time`, Google `Duration` strings, UPPER_SNAKE enums, oneof unions without tags, unset optionals absent rather than `null`. Requests reject unknown fields; responses never do, and clients ignore unknown fields, enum values and oneof members.
- **The session is a singleton.** `GET session` returns the caller's identity, permissions and expiry, always 200 (anonymous allowed). `POST session:authenticate` exchanges an identity-provider proof for a bearer token (ADR-06). A token is not a resource: it has no name, is not stored and cannot be revoked. A certificate is, so it gets Create.
- **Browser flows stay under `/ui`,** unversioned and not API; `/ui/callback` is the operator-registered redirect URI.
- **Additive within `/v1`:** fields, enum values, oneof members, error reasons and routes may be added; nothing is renamed, repurposed or removed. A break needs `/v2` served alongside.
- **Nothing updatable yet,** so no Update, etag or `request_id`.

## Consequences
- Deviations from Google (64-bit integers as numbers, always-emitted scalars, SSE rather than gRPC streaming, a constant error `domain`) are in docs/API.md.

## Rejected alternatives
- **`POST /v1/tokens`:** a token is not a resource.
- **A stateless `/v1:authenticate`, or verbs named `:exchangeToken` or `:login`.**
- **`providers/{provider}/principals/{principal}`:** a pseudo-parent with no served methods.
- **Fleet as parent of events:** events outlive removed fleets.
- **JSON Patch and 201 for Create.**
