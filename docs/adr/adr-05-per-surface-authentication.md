# ADR-05: Authentication is per surface, pluggable, and enforced by one per-request gate

Status: Accepted

## Context
- One mechanism for every caller does not fit: operators need Gang behind a tunnel or proxy that authenticates users, and a trusted network with none.
- Bazel can attach headers; NativeLink's worker cannot, so workers keep mutual TLS.
- NativeLink checks nothing itself (ADR-02), so authentication runs in front of it and in front of the Gang API.

## Decision
- **Surfaces.** A surface is an audience: `clients` (people, CI) and `workers`. Each selects exactly one plugin from its own domain, `[authentication.clients.<plugin>]` and `[authentication.workers.<plugin>]`. `compose!` limits the choices at compile time: clients get `token` and `none` (`header` is additive later), workers get `mtls` and `none`.
- **Ports.** `AuthenticationPort` identifies the caller of one request as `Authenticated { identity, valid_until }`, where `valid_until` is the last instant the credential is accepted (absent when unbounded). It reports scheme, capabilities (`certificates`, `tokens`, `sessions`) and the client certificates the transport must request. `CredentialLifecyclePort` issues certificates, tokens and sessions and revokes certificates; an unadvertised operation returns `Unsupported`.
- **Host.** Plugins reach identity providers, the authority, the ledger and the clock only through the `AuthenticationHost` that `gang-server` lends on each call, so plugin crates never depend on each other (ADR-01). `gang-authn` provides `token`, `mtls` and `none`.
- **Identity** is `(provider, principal)` for every plugin; a request with none is anonymous only on open routes (ADR-09).
- **One gate per surface, per request.** `gang-server` authenticates, refuses revoked principals (ADR-08), registers the principal and applies the surface's admission rule. The embedded controller reaches it through a request middleware (`RequestGate`), so NativeLink never sees TLS, tokens or revocation. Connection-level admission is not enough: a multiplexing proxy shares one origin connection across users.

## Consequences
- A new client scheme (`header` for a trusted proxy such as Cloudflare Access) is a plugin, a config section and a client mode. Gang then has no revocation of its own for it; exposure is bounded by the upstream token's expiry.
- One more middleware hop per request, and the controller gains a type parameter for the gate.
- Authenticated expiry lets streams end exactly at credential end (ADR-27).

## Rejected alternatives
- **One plugin per server:** forces workers onto the client mechanism or the reverse.
- **A client plugin registry:** gains nothing over the closed `Authentication` wire tag the client dispatches on.
