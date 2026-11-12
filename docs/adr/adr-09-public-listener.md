# ADR-09: One public listener serves REAPI, the API and the dashboard

Status: Accepted. Replaces the HTTP/2-only public listener.

## Context
- Operators want one public name and port, and the worker port private.
- Browsers have no h2c, and tunnels default to HTTP/1.1 to the origin, so an HTTP/2-only listener could not serve either.
- A multiplexing proxy shares one origin connection across users, so admission must be per request (ADR-05).

## Decision
- **One public listener.** `listen` serves the Gang API (`/v1/*`), the dashboard (`/ui/*`) and the REAPI services under one name and port. It speaks HTTP/1.1 and HTTP/2, detected per connection (h2 prior knowledge included). The worker listener stays private and serves HTTP/2 only. ALPN is per listener: the public one offers `h2` then `http/1.1`, the worker one `h2`.
- **Routes fail closed.** Only routes named open are open: server description, credential issuance (their proof is the body), the caller's session, and the dashboard shell and sign-in routes under `/ui`. Every other route, unknown paths included, needs an identity, and permission routes also need a role (ADR-10).
- **The gate admits, the service decides.** On `/v1` and `/ui` the gate never rejects: it records the identity or the refusal, and the API renders the answer (ADR-33). `/ui` carries a static shell and sign-in routes and no data; data endpoints answer 401 or 403 themselves.
- **Local Mode** `--insecure` binds the `administrator` role to `none:anonymous`, so the dashboard shows data.

## Consequences
- Public infrastructure is one port (443) plus the private worker port.
- An HTTP/1.1 request head must arrive within `[scheduler.nativelink.http2] header_read_timeout`.
- A browser reaching Gang-terminated TLS must trust the Gang CA; behind a public-certificate proxy Gang runs `transport = "plaintext"` (ADR-03).

## Rejected alternatives
- None were recorded; the HTTP/2-only listener it replaces could not serve browsers or HTTP/1.1 tunnels.
