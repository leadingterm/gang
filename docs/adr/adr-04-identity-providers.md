# ADR-04: Identity providers are plugins, and authorization is configuration

Status: Accepted

## Context
- Developers, CI jobs and build workers each prove identity differently: browser login, a job token, a cloud workload token.
- Static secrets on laptops, CI runners and workers are the usual leak path.
- NativeLink checks no tokens and reads no subject, so access control cannot rest on anything it inspects.

## Decision
- **Pluggable proofs.** Identity providers are plugins under `[identity.providers.<name>.<plugin>]`, any number. Each implements `ProofVerifierPort` (server) and `LoginPort` (client), whose `login()` returns `Interactive` or `Unsupported`.
- **`gang-oidc` is provider-agnostic.** Endpoints come from the issuer's discovery document; tokens are verified against JWKS (`iss`, `aud`, `exp`, `nonce`); the identity is the configured `principal_claim`; `required_claims` must match exactly. No code names a provider, domain or organization.
- **Authorization is configuration.** `required_claims` decides who receives a credential (for example a GitHub organization by immutable numeric id). Tightening to repositories, branches or environments is a configuration change.
- **Identity is `(provider, principal)`** for every proof, whatever the credential that follows.
- **No stored secrets.** Developers hold their own login (refresh token `0600` in the user's config directory). CI jobs and workers use credentials their runner or provisioner mints per job or per instance and never store. The signing key lives in the signer, not on disk in production.

## Consequences
- A new proof source is a plugin or a configuration entry; the credential flows (ADR-06, ADR-07) do not change.
- Issuance and renewal depend on the identity provider being reachable; a suspended account stops renewing.

## Rejected alternatives
- **Long-lived API keys or passwords** checked by NativeLink: it checks no tokens, and stored secrets are the leak path above.
