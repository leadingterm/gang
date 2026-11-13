# ADR-37: Headless login is a Gang device authorization approved through the dashboard sign-in

Status: Accepted. Amends ADR-06 (who renews an interactive login) and ADR-29 (what the web client signs in for).

## Context
- `gang login` needs a browser on the same machine: the provider redirects to the CLI's `127.0.0.1` listener. Over SSH, from a phone or a server, the user must copy a failed loopback URL out of an address bar.
- Providers' own device flows are not universal, need a third client per provider whose secret ships to every CLI, and their pages cannot show who asked.
- The dashboard's confidential web client (ADR-29) signs a user in on any device, and its code exchange stays on the server.

## Decision
- **Gang is the device-authorization server (RFC 8628 shape).** `POST /v1/deviceAuthorizations` (open) returns a user code, which is the resource ID, the verification URI `{endpoint}/ui/device`, an expiry, a poll interval and a 256-bit `secret` returned once. The provider is the `[dashboard.login]` provider; the client never names it.
- **Approval is a sign-in.** The user types the code at `/ui/device`, sees the server-observed address and age of the request, and approves with a same-origin `POST /ui/device` that starts the web-client sign-in bound to the code. The callback stores the grant; the first approval wins. No prefilled link.
- **Redemption.** `:redeem` with the secret answers `AUTHORIZATION_PENDING` until approved, then issues a client token through the normal issuance and returns the provider's refresh token; the entry is removed.
- **The CLI holds the web client's refresh token and renews through Gang.** `POST /v1/session:authenticate` accepts `refreshToken` in place of `proof`; the server runs the refresh grant with the web client and returns any rotated token. No session table, no Gang-issued refresh token; the provider still re-approves every ID-token cycle.
- **State is memory only:** a bounded, expiring map, like login attempts.
- **Composition.** `gang login` races the device authorization with the unchanged loopback login when it can open a local browser.

## Consequences
- Revocation is unchanged: a revoked principal is refused at redemption and at every renewal (ADR-08); a suspended account fails its next refresh grant.
- A stolen refresh token works only through Gang; the web secret never leaves the server.
- Device-login renewals need the server to reach the provider: one refresh grant per developer per `token_ttl`.
- Every device approval requests offline access and mints the CLI its own refresh token (ADR-38); each counts toward the provider's per-client token cap.
- Device-code phishing remains possible; `[dashboard.device] enabled = false` removes the routes.

## Rejected alternatives
- **The provider's device flow:** provider-specific, a third client whose secret ships to every CLI, no Gang-side confirmation.
- **Pasting a code back into the CLI:** slightly harder to phish, but cannot complete by itself and needs the same server-side exchange.
- **A Gang-issued refresh token:** a session table and secret store, and the provider stops re-approving (ADR-06).
- **A prefilled verification link:** the link is the phishing vector.
