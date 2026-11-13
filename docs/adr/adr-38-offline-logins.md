# ADR-38: Offline access is requested only by logins that must yield a refresh token

Status: Accepted

## Context

- `gang-oidc` sent a login's `authorization_params` with every authorization request, so `access_type=offline, prompt=consent` put every dashboard sign-in through Google's consent screen and minted a refresh token that was thrown away.
- Google returns a web client's refresh token only at a consent, and keeps at most 100 per account and client, evicting the oldest.

## Decision

- **`LoginRequest` gains `offline`:** the login must yield a refresh token. `[identity.providers.<name>.oidc.login.offline]` holds `scopes` and `authorization_params` sent only then; the login's own `authorization_params` go with every login. A name or scope may not appear in both.
- **Offline logins:** the CLI loopback login, every device approval (ADR-37), and a dashboard sign-in in a browser without a usable renewal cookie (ADR-39). Renewals never reach the authorization endpoint.
- **No refresh token returned:** a device approval fails and its code stays pending; a browser sign-in gets a session without renewal. Both log a warning naming the offline settings.
- **One token per holder.** Each holder gets its own refresh token; none is shared, so a provider that rotates tokens with reuse detection never sees a reuse.

## Consequences

- Consent appears once per browser and per CLI login, and after a logout or a rejected token; never on renewals or routine re-sign-ins.
- Tokens minted equal consents. Past the cap the provider evicts the oldest holder, which fails its next renewal and signs in again.
- Operators move `access_type` and `prompt` (Google) or the `offline_access` scope into `login.offline`. The login section is sent to clients and older clients reject the unknown key, so clients upgrade first.

## Rejected alternatives

- **Offline access on every login:** consent on every dashboard sign-in, and needless tokens toward the cap.
- **Trying without consent, then retrying with it:** Google gives a returning user no refresh token without consent, so the user sees consent anyway after an extra round trip.
- **Sharing one refresh token between holders:** rotation leaves the other holder's copy stale.
