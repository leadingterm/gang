# ADR-35: `gang-wire` types are the API schema; docs/API.md is checked against them

Status: Accepted

## Context
- The wire types are used by the server, client, e2e, CLI and the TypeScript UI. A hand-written reference drifts, and a separate OpenAPI or proto file would be a second schema to keep in step.

## Decision
- **The Rust types are the schema** (ADR-25: the wire types are the model). `ui/src/wire.ts` is generated from the `gang-wire` types by the ts-rs derive (`crates/gang-bindings`), and `//ui:wire_ts_test` fails when the checked-in file drifts. `ROUTES` and `ErrorReason::ALL` in `gang-wire` own the route table and the error catalog; the server's matcher, the cookie paths and the client's path building derive from `ROUTES`.
- **`docs/API.md` is the one human reference** (routes, bodies, errors, conventions, deviations). A `gang-server` test checks it: annotated JSON examples round-trip through registered `gang-wire` types, documented routes equal `ROUTES` in both directions, the real `Api` serves each route and answers 404 `ROUTE_NOT_FOUND` for undocumented methods, permission rows match 403 `MISSING_PERMISSION`, the errors table equals `ErrorReason::ALL`, SSE transcripts re-encode byte for byte, and example keys are lowerCamelCase.
- **SPEC §4.4 keeps the gate and authorization rules** and links to docs/API.md; a route or body shape has one owner.

## Consequences
- Every new route needs a doc entry and an example, or the test fails.
- A checked-in JSON Schema is a later, additive step.

## Rejected alternatives
- **OpenAPI or proto as the source of truth:** a second schema beside the Rust types and `wire.ts`.
- **Unchecked hand-maintained documentation.**
