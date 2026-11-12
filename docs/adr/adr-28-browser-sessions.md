# ADR-28: Browser sessions are read-only cookies of a session-scoped token

Status: Accepted

## Context
- A browser cannot attach an `Authorization` header to `EventSource` or navigation, and a bearer token in script-readable storage is exposed.
- The dashboard is read-only.

## Decision
- **Sessions are a capability of the client plugin,** not a plugin of their own: `token` has `sessions`, `mtls` and `none` do not. After an interactive sign-in (ADR-29) the plugin mints a JWT with `use: "session"` and sets it in a cookie: `HttpOnly; SameSite=Strict; Path=/; Max-Age` `token_ttl`, named `__Host-gang_session` with `Secure` under an `https` endpoint and `gang_session` otherwise.
- **Token scope.** Every token carries `use`: `"client"` is accepted only from `Authorization: Bearer`, `"session"` only from the cookie, and a token without `use` is refused. A stolen cookie cannot drive REAPI, and a CLI token cannot be replayed as a session. Bearer wins if both are present.
- **The cookie is read only for GET and HEAD** on a short list of read-only data routes on the public surface, never on the worker surface. No state-changing route authenticates by cookie, so there is no CSRF surface. `POST /ui/logout` is the one state-changing browser route: it needs no identity, only clears the caller's own cookie, and requires `Sec-Fetch-Site: same-origin` or `Origin` equal to the endpoint's origin.
- **Lifetime.** A session lasts `token_ttl` (default 10 minutes, at most 1 hour). There is no refresh token and no session table; a revoked principal loses it at the next request (ADR-08). On 401 or `closed: EXPIRED` the UI keeps its last frame, marks it stale and returns to sign-in; silent re-approval at the provider is the renewal.
- **On open paths,** a cookie the plugin refuses as an invalid proof is treated as anonymous, so a stale cookie never breaks the sign-in page.
- **Under `none`** every request is `none:anonymous`: no login, no cookie.

## Consequences
- Users re-approve at the identity provider about every 10 minutes by default.
- Mutations would need a per-session CSRF header, `Origin` and `Sec-Fetch-Site` checks, in their own ADR.

## Rejected alternatives
- **A server-side session table or sliding renewal:** keeps a user removed at the provider signed in past one ID-token cycle.
- **Cookie evidence on non-GET methods;** a separate session plugin; one token valid as both bearer and cookie.
