# ADR-13: Worker lifetime belongs to the provisioner

Status: Accepted

## Context
- Kubernetes pods are cheap per action; Slurm jobs queue. The EC2 model assumes many actions per worker, and a single-use EC2 worker would reuse volumes, which NativeLink forbids.
- A Slurm job queued past `boot_grace` looks like an orphan.

## Decision
- **The plugin reports it.** `ProvisionerPort::worker_lifetime()` returns `LongLived` or `SingleUse`, synchronously and without I/O. It is a setting of the plugin section only where both modes work (`[provisioner.kubernetes]`, `[provisioner.slurm]`); `aws` and `local` always report `LongLived`.
- **One source.** `gang worker` and `gang server` read it from the same plugin; it is never copied into `[fleets]` or `[scheduler.nativelink]`.
- **`SingleUse` sets NativeLink `single_use`,** requires `max_inflight_actions == 1` (else `PortError::Config`), and the worker exits 0 after uploading one action. The instance becomes `Terminating` and drops out of desired capacity.
- **Kubernetes defaults to `SingleUse`, one action per pod.** Pods come only from demand-driven `set_desired_capacity` while queued plus running exceeds live capacity; there is no per-action creation call. `idle_timeout` reaps pods created for demand that has gone. A `Running` pod whose worker does not connect within `boot_grace` is an orphan; `Pending` pods are exempt. Slurm defaults to `LongLived`.

## Consequences
- Each single-use action starts with a cold fast CAS and a fresh certificate: one KMS signature and one ledger row per action; S3 reads stand in for a local cache.
- `input_links = "break"` is pointless for single-use workers.
- Deriving one slot per instance from the lifetime (making `target_actions_per_instance` optional) waits for the first single-use provisioner.

## Rejected alternatives
- **Per-action pod creation or a push model:** NativeLink assigns actions only to registered workers; demand-driven `set_desired_capacity` is the smallest model and keeps restart and audit intact.
