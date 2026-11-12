# ADR-08: Revocation is live, in-process and ledger-backed

Status: Accepted. Replaces the earlier CRL-oriented and certificate-only revocation decisions.

## Context
- A credential is only as good as the ability to cut it off. NativeLink reads a CRL only at listener start, so file-based revocation means restarting the controller and dropping every running build; a CRL never closes established connections.
- Certificates live up to 7 days, so waiting for expiry is not enough.

## Decision
- **Principal revocation is host-level and applies to every plugin.** A registry flag (`principals.revoked_at`) is the durable record. An in-memory `RevokedPrincipals` set is loaded at start and checked by the gate on every request and by every issuance. A request after a revocation is refused at once. A stream already in flight ends with its action, except the dashboard state stream, which ends at once (ADR-27).
- **Serial revocation is an `mtls` capability.** `gang-tls` holds a live in-memory `RevocationSet` of serials. At start `gang-server` loads it from the certificate ledger behind `StateRepositoryPort`; on each revocation it records the ledger row first, then adds the serial.
- **Revocable connections.** The verifier refuses a revoked serial on its next connection, and an accepted connection carries its peer's serial and closes as soon as it is revoked. Session resumption is disabled so every connection runs the verifier.
- **No revocation list and no restart.** There is no CRL file. `gang-mtls` is the certificate authority only (ADR-03), and revocation never restarts the controller.
- **Per-token revocation is not offered** (ADR-06).

## Consequences
- A revocation takes effect on new and established connections at once, without disturbing other builds.
- It is restart-safe: the sets are rebuilt from the ledger and registry, which are the sole record.
- All connection admission lives in `gang-tls` and the gate, so it is reviewed and tested once.

## Rejected alternatives
- **A CRL file read by NativeLink with a controller restart per revocation:** interrupts every build and leaves established connections open.
- **Short certificate lifetimes alone:** leave up to `certificate_ttl` of exposure after offboarding or compromise.
