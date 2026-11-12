# ADR-07: Workers authenticate with certificates issued from workload credentials

Status: Accepted

## Context
- NativeLink's worker cannot add a header to each call, so workers keep mutual TLS (ADR-05).
- A worker has no human: its credential comes from the environment that launched it (an AWS instance role, a Kubernetes pod, a Slurm job), never from a stored secret (ADR-04).

## Decision
- **Surface plugin.** Workers use `mtls`. A worker enrolls with the workload proof of the configured provider, receives a certificate for `certificate_ttl`, and the worker listener requires a certificate whose ledger record names that provider.
- **One issuance flow.** `gang-server` verifies the proof with that provider's plugin and `gang-mtls` signs the CSR. The private key never leaves the worker. The certificate carries the SPIFFE ID `spiffe://{trust_domain}/{provider}/{principal}` in its SAN, and the principal and serial are recorded in the ledger as `IssuedCertRecord`.
- **Workload credential.** The credential is a property of the provisioner (`WorkloadCredentialPort`): STS `GetWebIdentityToken` on AWS, a mock-IMDS JWT in Local Mode, a projected service-account token on Kubernetes, `munge` on Slurm. Its verifier is an identity provider (ADR-04).
- **Renewal** obtains a fresh platform credential each time and issues again once `certificate_refresh_percent` of the lifetime has elapsed. Nothing is stored.

## Consequences
- Issuance and renewal need the identity provider reachable. A suspended account stops renewing; its last certificate stays valid until expiry unless revoked (ADR-08).
- The embedded worker reads certificate files only when `run_worker` starts, so a worker drains and restarts before its certificate expires unless the instance is replaced first.
- A new platform needs a `WorkloadCredentialPort` and an identity-provider entry; the flow does not change.

## Rejected alternatives
- **A `workload` identity domain:** the credential belongs to the provisioner and its verifier is already the identity-provider domain.
- **Stored long-lived worker secrets.**
