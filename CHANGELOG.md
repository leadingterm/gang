# Changelog

## Unreleased

### Changed

- Make pull-request CI exclusively x86-64: partition the full test and end-to-end suites, build a self-contained harness bundle in parallel, overlap harness startup with independent waits, and keep portable lint free of Darwin compilation; validate ARM, cross and macOS on default-branch pushes and version tags.
- Balance each E2E gate across eight shards using warm scenario timings, preserving all 49 scenarios and every check. Remove the redundant bundle cache export while retaining source builds, both binary/harness uploads and always-uploaded build metrics; the nonrequired cache fan-in uses the four test shards plus lint.
- Run independent leader-failover and database-outage variants concurrently on fresh clusters without shortening lease/fencing windows. Run fleet drained-start checks alongside a separate fresh cold-pair placement, retaining all 28 checks.
- Accept Gang-issued client-token and local workload-proof TTLs down to 20 seconds without changing production defaults; AWS STS proofs retain their 60-second minimum. Use 25-second tokens and a 60-second absolute browser-session age only in the E2E fixture. The sessions shard weight is an estimate pending measurement; warm-CI and whole-job latency targets are not claimed as measured results.
- Publish primary-region AMIs before independently completing the regional catalog and retirement.
- Require successful release validation from same-repository main push CI for the exact release commit; never reuse PR artifacts or caches. Prioritize one bounded x86 Bazel baseline and Nix store with small PR deltas; disable optional architecture caches.

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
