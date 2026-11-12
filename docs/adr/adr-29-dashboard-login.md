# ADR-29: Dashboard sign-in uses a dedicated web client

Status: Accepted

## Context
- The provider's `oidc.login` client is a native-app client whose `client_secret` is copied into the `GET /v1/server` answer for every CLI and treated as "not a secret in the strict sense" (RFC 8252 §8.5). A browser sign-in needs a confidential web client and a server-side code exchange.

## Decision
- **A server-only `[dashboard.login]`** names `provider` (an `[identity.providers]` key whose plugin supports interactive login), `client_id` and `client_secret`. The secret is never sent to clients and is redacted in `Debug`. The operator registers `{endpoint}/ui/callback` on that client.
- **The web client is a per-request override.** `LoginRequest` may carry a `LoginClient`; the provider lists the web client's id in `additional_audiences`. Principals stay `(provider, principal)`, the same as the CLI user, so roles apply uniformly (ADR-10).
- **A login attempt** is a single-use entry in a bounded in-memory map (`max_pending_logins`, TTL `login_ttl`), keyed by `state` and bound to the browser by a `__Host-gang_login` cookie (`SameSite=Lax`, because the callback is a cross-site navigation). It holds the secret and is never sent to the browser.
- **The callback** checks `state` against the cookie, rebuilds its URL from the configured `endpoint` (never from `Host`), completes the login with the web client, issues the session (ADR-28) and redirects to `next`, which must be `/ui` or below. Failures redirect with a generic reason and no detail.
- **Routes are mounted only** when the client plugin has `sessions`, `[dashboard.login]` is set and the provider supports interactive login.

## Consequences
- The controller is a single process (ADR-02), so memory suffices for login attempts.
- Under an `http` endpoint the cookies lose `Secure` and `__Host-`, and a startup warning is logged.

## Rejected alternatives
- **Reusing the provider's `oidc.login` client:** it leaks the secret.
- **A KMS-signed login ticket:** gives an unauthenticated route a signer call.
