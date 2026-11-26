### Added

- Schedule actions by priority, with out-of-band cgroup `cpu.weight` tiers: priorities >= 200 use 1000, 100 through 199 use 100, and below 100 use 20. `gang setup` and `gang bazelrc` render `--remote_execution_priority=100`.
- Advertise REAPI ZSTD and IDENTITY compression in Capabilities and support both in ByteStream and CAS transfers.
- Cache read-only input trees and bind-mount them under `bwrap`; `[execution] input_trees` defaults to `"bind"`, with `"link"` available as an escape hatch.
- Add `[server]`, `[worker]`, and `[cache.s3.tuning]` tunables.

### Changed

- Replace embedded NativeLink (`gang-nativelink` and nine `nativelink-*` crates) with Gang's native execution engine (`gang-engine`, `gang-scheduler`, `gang-cas`, and `gang-reapi`), eliminating the FSL-1.1-ALv2 dependency and the `nativelink-memory-tests` CI job.
- Harden scheduling and workers with target-learning isolation, capacity-bounded memory history, anti-starvation sticky headroom holds, per-fleet scan budgets, cleanup-held disk reservations, bounded upload file descriptors, and drained shutdown.
- Lower the OpenTofu storage module's default `cache_expiration_days` from 30 to 3 days.

### Removed

- Remove `[worker] cleanup_wait`; it is now rejected as an unknown key.

### Upgrade notes

- Remove `cleanup_wait` from `[worker]` in `gang.toml` before upgrading.
