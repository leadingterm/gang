# ADR-12: Provisioner, cache and signer are separate domains

Status: Accepted. Replaces the coarse `platform` domain.

## Context
- One `platform` domain bundled compute, storage and signing. Kubernetes and Slurm need to mix them, for example pods with an S3 cache and a KMS signer.
- Pods and jobs get identity from the platform (projected token, `munge`), not from instance metadata.

## Decision
- **Three exactly-one domains:** provisioner (`ProvisionerPort`, `NodeLifecyclePort`, `ScratchVolumePort`, `WorkloadCredentialPort`), cache (`CacheStorePort`) and signer (`CertificateSignerPort`). Any provisioner combines with any cache and any signer.
- **Node identity, scratch and workload credential stay in the provisioner.** They are properties of the environment that launches workers; their verifier is the identity-provider domain (ADR-07).
- **No controller/node split of the provisioner.** The fleet and node halves share instance ids, preemption and token shape, so mixed combinations are meaningless. The build rule covers the role difference: `Plugin::build` validates configuration only, resources open on first use by the role's ports, and `gang worker` never builds the signer.
- **The filesystem cache is served via the controller.** NativeLink's filesystem store is single-process with an in-memory index, so workers reach it through the controller.
- **Settings follow their owner:** `bucket` is `[cache.s3]`, the key is `[signer.kms]`, and each plugin owns its own tunables.

## Consequences
- A new platform adds crates, one `compose!` line and additive configuration; contracts do not change.
- Single-use workers cost one KMS signature and one ledger row per action; KMS rate limits cap burst size (ADR-13).
- Sites without KMS use `signer.file`, which keeps a key on disk. This contradicts "no stored secrets" until a PKCS#11 or Vault signer exists (accepted gap).

## Rejected alternatives
- **`fleet` and `node` provisioner domains:** halves coupled by instance ids, preemption and token shape.
- **A `workload` identity domain** (ADR-07).
