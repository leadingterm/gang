# ADR-33: API errors use one AIP-193 envelope

Status: Accepted

## Context
- The gate answered revoked principals and any authentication error (including an expired bearer token) with plain text, before routing, so the JSON error appeared only for errors the API itself raised.
- The error envelope was hand-written twice, and nine ad-hoc kinds mixed 404, 405 and 503 inconsistently.

## Decision
- **One envelope for every non-2xx on `/v1/**`:** `{error:{code, message, status, details[]}}`. `code` is the HTTP status; `status` is the `google.rpc.Code` name. `details` carries `ErrorInfo` exactly once, with an UPPER_SNAKE `reason`, the constant `domain` `"gang"` and lowerCamelCase `metadata`, plus `BadRequest` for field violations. Metadata keys, once published, are never removed. Messages stay generic and never carry proofs, CSRs or internal causes. Every 401 carries `WWW-Authenticate: Bearer`.
- **Reasons are a closed catalog,** `ErrorReason::ALL` in `gang-wire`, checked against docs/API.md (ADR-35).
- **The API renders gate refusals.** On `/v1` and `/ui` the gate never returns `Err`: it admits the request and records the outcome in `Caller` (an authenticated identity or a refusal). The API checks the refusal first and renders the envelope. The NativeLink gate stays plain text and gRPC for REAPI.
- **Ordering and codes.** The caller is authenticated and authorized before existence is checked. An anonymous unknown path stays 401, so routes are not probed. A wrong method is 404 `ROUTE_NOT_FOUND`, because a route is the `(method, path)` pair. A request timeout is 504.

## Consequences
- The `Allow` header on wrong methods is lost.
- `INTERNAL_ERROR_BODY` is built from the same typed envelope as a constant, so serialization cannot fail.

## Rejected alternatives
- **Keeping the ad-hoc kinds and 405.**
- **Teaching the NativeLink gate to render JSON.**
