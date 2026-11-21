# Changelog

## Unreleased

### Added

- Expose scheduler-owned action reservations and worker reserved capacity in live state and the dashboard, including pre-fit sizing when clamped.

### Changed

- Scale out to the greater of the fleet minimum and the CPU, memory, or disk reservation need, capped by the fleet maximum. CPU capacity includes `[actions] cpu_oversubscription` (default `2`); rate and payback estimates no longer hold scale-out below resource demand. Expected wait and N−1 scale-in use resource capacity, not nominal action slots.
- The public listener defaults to 4096 connections, with dashboard diagnostics showing current saturation and cumulative refusals since server start.
- Public egress now uses a per-action pasta network namespace and one host nftables policy scoped to the action cgroup subtree. NixOS prepares the policy and cgroup before worker startup; missing or mismatched prerequisites deny granted actions.
- **Server configuration migration:** Remove `slirp4netns`, `nft`, `nsenter` and `ip` from `[sandbox.bwrap.egress]`; these old helper keys are rejected as unknown. The table now accepts only `program`, defaulting to `pasta`, resolved and pinned like `[sandbox.bwrap].program`. Keep the existing `[execution.egress]` policy and fleet grants.

### Removed

- Remove `target_actions_per_instance` from fleet configuration and Terraform fleet inputs, and remove `targetActions` from the fleet policy state API. Operators must delete the setting from existing configuration and Terraform inputs before upgrading; dashboard/API clients must stop reading `targetActions`. There is no replacement action-count limit. Long-lived workers have no fixed in-flight limit; single-use workers retain their lifecycle.
- Replace `[actions.cold_start] cpu_count`, expressed in whole cores, with the integer `cpu_millicores` setting. The new default is 250 millicores; migrate an explicit old value by multiplying it by 1000. The old key is rejected.

### Fixed

- Cache DNS locally on controller and worker images, including for static binaries.
- Expose the public listener connection limit in the deployment module, defaulting to 4096. The module now owns `[server.http2]` for all backends; SQLite callers must remove any appended `[server.http2]` table and move their `max_connections` override to `http2_max_connections`.
- Exclude concurrent child spawns while testing owned pipe-reader cancellation, so fork-inherited readers cannot make the close assertion nondeterministic.

## 0.3.4

### Added

- Canonicalize action work roots so output symlinks work correctly through symlinked work directories.
- Run bwrap actions in a pinned, read-only Debian 13 rootfs with builder UID/GID 1000. Its content digest is operational image metadata, not a client or cache identity.
- Configure per-fleet action capability grants and optional per-action resource ceilings separately from worker capacity. Public network egress is opt-in and denied by default; enforcing sandboxes use a sealed network gate, protected networks and configured public DNS resolvers.
- Rate-based scale-out separates persistent authenticated-source load from transient backlog, using resource-vector payback accounting. N−1 utilization scale-in cordons the physical instance that empties soonest after a waterline dwell, with returning-load rescue before new purchases.
- Persist source activity profiles as fenced learned-history records, and expose persistent load, transient backlog and serving utilization demand estimates.

### Changed

- Reject unknown platform properties, ungranted network requests and over-ceiling requests at admission instead of silently accepting them.
- Namespace the action cache with the server-owned `[actions] environment` version. Image security and patch updates do not change cache keys or require client action.
- Credit pending and reliable requested capacity without repeatedly raising desired size; keep transient credential-refresh gaps separate from serving surplus and preserve failed-request withdrawal.
- Keep controller cordons across reconnects and overlapping worker generations within the live engine; reset N−1 dwell after skipped passes.

### Fixed

- Bound persistent-queue scale-out by payback on queued work and qualified genuine arrivals, avoiding overscaling cheap static batches and closed-loop completion waves.
- Require completion-backed refill in the current cohort before forecasting future arrivals, so finite uploads do not create a sustained-load forecast.
- Reject action-cache environment names without exactly two bounded components, preventing namespace overlap.

### Removed

- Remove obsolete transitional health and configuration aliases, staged PROXY modes, historical database migrations, old upgrade controls, legacy CI cache formats and the in-place configuration restart workflow. See the [specification](docs/SPECIFICATION.md) for compatibility and configuration details.

### Upgrade notes

- The default `default/1` environment uses the existing unprefixed action cache, keeping it warm through the v0.3.3 to v0.3.4 deploy. Running builds continue, and existing client rc files remain usable; no client action is required. Later reviewed environment identity bumps cost one cold action cache.
- See [restart and rollback](docs/SPECIFICATION.md#5-autoscaler-state--restart) for scaling configuration and downgrade constraints, and the [specification](docs/SPECIFICATION.md) for the v0.3.3 upgrade prerequisite and compatibility removals.

## 0.3.3

### Added

- Preserve learned resource reservations and action durations across controller restarts and failover.
- Gate releases on a real mixed-version upgrade that waits for the replacement to be NLB healthy in every target group during warmup before nonblocking pre-stop, with a four-second availability bound and fixed published port-removal and alive-drain negative controls. Expose ordinary instance-refresh qualification separately; this gate does not itself fix published v0.3.2 plain refresh.
- Require a separate configuration-refetch/restart release check: verify deployment-derived parameter lineage, launch selectors and IAM coverage in an offline model, then restart the real published old controller and worker from the exact fetched configuration bytes.
- Require ordinary instance-refresh qualification to prove shared authentication and validate its bearer on the first Capabilities request, followed by a separate cold old-CLI credential mint and first Capabilities validation through the original load balancer, without retries, during the live held-build drain. The harness does not prove fresh leader-only RPC refusal; build-continuity requirements remain unchanged.

### Changed

- Check follower shared responses against the leader and verify marked leader-only refusals in the build scenario.
- Include bounded state, redacted local launch records and build status in launch-failure scenario timeout diagnostics.
- Run cross-backend learned-history and embedded integration tests in the end-to-end crate, keeping lower-layer test dependencies independent of server orchestration.
- Make pull-request CI exclusively x86-64: partition the full test and end-to-end suites, build a self-contained harness bundle in parallel, overlap harness startup with independent waits, and keep portable lint free of Darwin compilation; validate ARM, cross and macOS on default-branch pushes and version tags.
- Balance each E2E gate across eight shards using warm scenario timings, preserving the complete suite and every check. Remove the redundant bundle cache export while retaining source builds, both binary/harness uploads and always-uploaded build metrics; the nonrequired cache fan-in uses the four test shards plus lint.
- Run independent leader-failover and database-outage variants concurrently on fresh clusters without shortening lease/fencing windows. Run fleet drained-start checks alongside a separate fresh cold-pair placement, retaining all 28 checks.
- Accept Gang-issued client-token and local workload-proof TTLs down to 20 seconds without changing production defaults; AWS STS proofs retain their 60-second minimum. Use 25-second tokens and a 60-second absolute browser-session age only in the E2E fixture. The sessions shard weight is an estimate pending measurement; warm-CI and whole-job latency targets are not claimed as measured results.
- Publish primary-region AMIs before independently completing the regional catalog and retirement.
- Require successful release validation from same-repository main push CI for the exact release commit; never reuse PR artifacts or caches. Prioritize one bounded x86 Bazel baseline and Nix store, allowing a 400 MB PR delta for the measured 316 MB working set; disable optional architecture caches.

### Fixed

- Restore macOS validation as advisory, without delaying or blocking releases.
- Give native ARM checks a cold-compilation step budget.
- Train burst scale-out qualification on the actual short-action targets rather than extrapolating one cold worker/CAS sample to the entire queue.
- Run the local E2E gate with the same CI-derived shard partitions and per-shard budget as CI.
- Preserve the exact action environment through nix-ld: an absent `LD_LIBRARY_PATH` remains absent instead of being introduced as an empty value.
- Release held rolling-deploy probes before waiting for leadership transfer, so orderly drain can finish before the final learned-history flush and lease release.
- Pin new instances to immutable configuration generations and freeze the legacy parameter for safe v0.3.2 restarts and rollback. Retain configuration until no instance or launch-template reference remains; deployments require OpenTofu 1.12.4 or newer.
- Move the production apply runtime from OpenTofu 1.13.5 to the 1.13.9 patch release, aligning Bazel tests with the deploy runtime.
- Hand leadership to a routable newer controller during shared-database refresh without terminating the former leader process. Prepared controllers serve shared authentication, API and cache services before promotion and throughout handoff; leader-only refusals safely retire connections while admitted streams drain. Host priority survives restart, and a bounded learned-history snapshot precedes release, though late measurements may be lost. The v0.3.3 health listener retains `/leader` as an exact `/ready` alias for mixed-version rollout.
- Generate `--remote_download_outputs=toplevel` in client rc files so unused remote intermediate outputs are not downloaded.
- Supply sandboxed actions with a synthetic worker identity and public TLS roots, and partition caches by action environment revision.
- Avoid killing actions for ambient memory stalls without a sustained action-pool shortage.
- Align dashboard sections and cache charts using shared responsive layout.
- Preserve client identity with PROXY v2 while avoiding cross-zone NLB backend connection collisions in shared-database deployments. Keep the default `accept` stage during the v0.3.3 release deploy; after every binary has rolled out, apply `proxied` and `require` in separate steps, waiting after each. Roll back one stage per apply in reverse: `require` → `proxied` → `accept` → `off`, waiting after each; reach `off` before downgrading to a parserless binary. See the deployment specification for trust and rollout details.

### Upgrade notes

- The scheduler is built in, not an operator-selected backend. Migrate `gang.toml` as follows (all old paths below start with `scheduler.nativelink`):
  - `worker_endpoint` → top-level `worker_endpoint`; `scheduler_name` is removed and is an internal constant.
  - `max_open_files` → `server.max_open_files` and `worker.max_open_files`.
  - `scheduler.worker_timeout` → `server.worker_timeout`.
  - `scheduler.retain_completed_for`, `scheduler.max_job_retries`, and `scheduler.max_worker_loss_retries` → `actions.retain_completed_for`, `actions.max_job_retries`, and `actions.max_worker_loss_retries`.
  - `scheduler.headroom_percent` → `actions.reservation_headroom_percent`, the margin added to learned peaks.
  - `scheduler.window_days` → `actions.history_window`, a positive whole-day duration (default `7d`); `scheduler.duration_keys` → `actions.history_keys`, a positive duration-history key cap (default 65536). New `actions.history_memory_budget` is a positive resource-history byte budget (default `512MiB`), and `actions.history_flush_interval` is a positive interval (default `30s`).
  - `worker.action_timeout` → `actions.timeout`; `worker.kill_grace` → `actions.kill_grace`.
  - `worker.upload_timeout`, `worker.download_timeout`, `worker.connect_timeout`, `worker.cleanup_wait`, and `worker.keepalive_interval` → the same keys under `worker`. Existing `worker.fast_cas_percent` remains unchanged.
  - `http2.*` → `server.http2.*`; `disk.*` and `cold_start.*` → `actions.disk.*` and `actions.cold_start.*`; remaining `memory.*` keys → `actions.memory.*`, except the escalation settings below.
  - The shared `memory.escalation_percent` → both `actions.memory.escalation_percent` and `actions.disk.escalation_percent`. Set both to retain the prior multiplier for memory and pressure-disk kills.
  - The shared `memory.escalation_max_steps` → `actions.memory.max_steps` and `actions.disk.max_steps`. Memory and disk now have independent, positive finite step counts; the old value `0` for unbounded steps is no longer accepted.
  - Memory enforcement now defaults to `pressure` rather than `soft`; see [resource tunables](docs/SPECIFICATION.md#32-scheduler-gang-nativelink) for the current policies and defaults.
  Old configurations are rejected; no compatibility aliases are provided. HTTP behavior and defaults are unchanged.
- Immutable configuration generations preserve the old v0.3.2 restart and refetch bytes; retain old references until no instance or launch template needs them.
- Upgrading from v0.3.2 to v0.3.3 requires a supervised warm-NLB pre-stop: use `stop --no-block` on the old `gang-server` only after the replacement is healthy in every actual ASG target group while both remain registered and before the actual warmup expires; do not manually deregister targets. Warmup starts before health, not after it. Automatic handoff requires a newer-capable outgoing v0.3.3 controller.
- `/leader` remains a `/ready` alias for compatibility in this release; module migration comes later. The alias does not establish leadership.
- Keep PROXY at `accept` (emission off) until all binaries have rolled; do not switch to `proxied` or `require` during this release deploy.
- Keep the AWS notice feed off until all binaries have rolled.
- The deploy runtime uses OpenTofu 1.13.9; immutable configuration generation retention requires at least 1.12.4.
- The sandbox action-environment revision partitions the cache; expect one cold cache.

## 0.3.2

### Fixed

- Restore remote execution and cache to one client endpoint on port 443; regenerate `gang bazelrc` after upgrading.
- Grant controller lifecycle Describe calls at the scope required by Auto Scaling while retaining group-scoped lifecycle completion.
- Preserve resource-kill outcomes when output upload fails so escalation remains effective.
- Use sustained worker disk pressure instead of learned reservations as the default disk-kill trigger, requeue pressure kills with larger reservations, and floor nonzero disk reservations at 1 GiB by default.

## 0.3.1

### Added

- Show cache transfer bytes by build and across the farm, bounded-store usage and evictions, and the latest available S3 bucket measurement.

### Fixed

- Include follower-served cache lookups and hits in the dashboard's farm-wide history.
