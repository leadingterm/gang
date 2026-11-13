# ADR-06: Clients authenticate with short-lived bearer tokens

Status: Accepted. Replaces the earlier model in which every caller, clients included, held a client certificate. Amended by ADR-37 (device login renewal through Gang).

## Context
- Developers and CI jobs gain nothing from client certificates: Bazel needs only server TLS plus a bearer credential. The certificate flow cost a wrapper script, a generated rc file and a renewal command.
- Interactive sessions already have an identity provider that can re-approve the user (ADR-04).

## Decision
- **Credential.** A client presents an ES256 JWT in `Authorization: Bearer`. Gang issues it in exchange for an identity-provider proof. Claims, lifetime bounds (`token_ttl`, 1 minute to 1 hour) and the issuance route are in SPEC §4.1 and docs/API.md.
- **Signing and verification.** The token is signed through `CertificateSignerPort` with the authority key and verified locally against the public key, which the plugin loads from the signer on first verification and caches. Verification never calls the signer. Verified tokens are memoized until `exp` plus `clock_skew`, bounded by `max_cached_tokens`.
- **No server-side session.** An interactive CLI session is the OIDC refresh token the client holds, and the provider re-approves the user at every ID-token cycle. CI supplies a short-lived job token through a token command.
- **No per-token revocation.** A stolen token is usable for at most `token_ttl`; principal revocation (ADR-08) cuts it off earlier.
- **Opaque to clients,** so a dedicated key or format later is not a contract break.
- **Client wiring.** `gang get` is Bazel's credential helper and prints the cached token or mints one; `gang bazelrc` prints the static flags for a committed `.bazelrc`.

## Consequences
- Build availability depends on the identity provider within about one ID-token lifetime instead of a certificate's 7 days; `token_ttl` is the lever.
- Public infrastructure shrinks to one port (ADR-09).

## Rejected alternatives
- **`mtls` for clients as well:** keeps the wrapper and needs a rule for who issues on the shared route.
- **A Gang-issued refresh token and session table:** adds a secret store and ends identity-provider re-approval.
