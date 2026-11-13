# ADR-39: Browser sessions renew silently from a sealed provider refresh token

Status: Accepted. Amends ADR-28 (lifetime).

## Context
- A session lasts `token_ttl` (10 minutes) and is never renewed, so the dashboard returns to sign-in every cycle, through Google's consent screen in production.
- The CLI already keeps its own provider refresh token and renews without the user (ADR-04); the browser has no such holder.
- A refresh token held by the server is a secret store (ADR-06); one readable by script is exposed (ADR-28).

## Decision
- **The browser holds its own login, sealed.** An offline sign-in (ADR-38) sets `__Host-gang_renewal` (`HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=34560000`): AES-256-GCM over `{provider, principal, refreshToken, expireTime}`, with the endpoint origin as associated data, under HKDF-SHA256 of the web client secret. The 400-day `Max-Age` is the browser cap (RFC 6265bis), keeping the token for a consent-free sign-in after the sealed deadline. No new key is stored, and the key never leaves the server.
- **Renewal.** `POST /ui/renew` needs no identity, follows the `/ui/logout` same-origin rule and is rate limited. It opens the cookie, runs the refresh grant with the web client, requires the same principal, issues a fresh session cookie, and reseals the renewal cookie when the token rotated. Renewals of one token that start within `[dashboard] renewal_reuse` (default 1m) share one grant. The UI renews before `expireTime`, and on `closed: EXPIRED` renews and reconnects.
- **Absolute lifetime.** `expireTime` is `[dashboard] session_max_age` (default 12h, from 1m to 30d) after the sign-in. After it, renewal answers 401 and keeps the cookie; the UI signs in at the provider without consent: the callback checks the sealed token with a refresh grant and reseals it, or restarts the sign-in as offline when the check fails.
- **Scope.** Only `GET /ui/login` and `POST /ui/renew` read the renewal cookie; the callback uses the renewal carried by its sign-in attempt. It is never a bearer proof, and `session:authenticate` cannot open it.
- **Logout** clears both cookies and does not revoke at the provider.

## Consequences
- The provider still re-approves every `token_ttl`, invisibly. A suspended account fails its next renewal; a revoked principal is refused at renewal and at the gate (ADR-08).
- There is no session table and no per-browser revocation. A stolen renewal cookie gives read-only dashboard access until `session_max_age`, principal revocation or suspension.
- The renewal cookie outlives `session_max_age` (400-day `Max-Age`) only to skip consent on a real provider sign-in: past `expireTime` `/ui/renew` answers 401, so a stolen cookie cannot extend a session.
- Rotating the web client secret invalidates every renewal cookie; each user consents once more.
- A sealed value over 4000 bytes is not set; that browser keeps the 10-minute session.

## Rejected alternatives
- **Refresh tokens in server state:** a durable secret store (ADR-06, ADR-28).
- **A random in-memory sealing key:** every restart sends every browser through consent.
- **A separately stored sealing key:** a new secret to provision, no gain over deriving from the web secret.
- **Silent `prompt=none` redirects:** a top-level navigation every cycle; frames are blocked by providers and third-party cookie rules.
- **Revoking at the provider on logout:** revocation semantics differ by provider (one token or the whole grant).
