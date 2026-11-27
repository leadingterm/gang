# ADR-62: Gang's own execution engine replaces embedded NativeLink

## Status

Accepted and implemented. Supersedes [ADR-02](adr-02-embedded-nativelink.md), the NativeLink-specific clauses of [ADR-14](adr-14-fleet-bundles.md) and [ADR-54](adr-54-action-termination.md), and the "NativeLink embedded as a library" decision of PLAN §3 and SPECIFICATION §3.2; keeps the core domain decisions of ADR-14 (catalog fit and fleet preference), ADR-54 (`gang exec` termination relay), ADR-55 (cache facts at their owners), ADR-59 (handoff) and ADR-60 (rate-based scaling). The build contract is [the engine design](../native-engine-design.md).

## Context

Gang links nine `nativelink-*` crates from a fork carrying 46 commits, and `gang-nativelink` spends 22,169 lines translating: rendering typed config into NativeLink JSON, scraping store metrics from `tracing` spans, walking `filter_operations` to rebuild snapshots, decorating schedulers to count completions, and mirroring cordons per connection. Most Gang behaviour (escalation, pressure kills, deadlines, observed-capacity fitting, cache stats, cleanup off the critical path) exists only as fork patches. The cache port leaks NativeLink's schema (`RenderedStoreSpec(serde_json::Value)`). Priority orders the queue but gives running actions no CPU share. Worker staging re-resolves and re-links every input file per action.

## Decision

- Gang implements its scheduler, stores and worker behind the unchanged `SchedulerPort`, in three crates: `gang-scheduler` (`2_application`, pure: no Tokio, I/O or clock reads), `gang-cas` (Ring 3 tier 29: local CAS, single-flight, tree cache, remote client) and `gang-engine` (Ring 3 tier 30: the fixed adapter). `gang-reapi` (Ring 3 tier 27) holds generated protos, checked in with a drift test.
- The controller's scheduling state has one owner: a single actor task holding a `&mut self` state machine. It reads three bounded inboxes in strict order (control, then worker, then client), so client bursts cannot delay snapshots or worker acknowledgements. Learned state outlives the term for the final persistence flush.
- Higher priority still runs first; deduplication promotes a queued operation. Priority and reserved cores also map to a work-conserving cgroup v2 `cpu.weight` (`clamp(tier_per_core * max(1, ceil(reserved_millicores / 1000)), 1, 10_000)` with 1000/100/20 per core, leaving `cpu.max` untouched for `cpu_millicores`). The weight is carried outside the action's platform and environment, so ActionCache keys never change. The worker and actions cgroup siblings get explicit equal weights. `gang bazelrc` defaults clients to priority 100.
- Placement spreads load: the highest-preference fleet, then the lowest reserved CPU fraction within it. Scale-in stays the reconciler's decision (ADR-60).
- Workers speak `leadingterm.gang.worker.v1`, one bidirectional stream per worker generation, with:
  - explicit accept and decline;
  - `Executed` (frees CPU and memory early) before `Completed` (frees disk, publishes the result);
  - dispatch-stamped attempt deadlines;
  - action and command inlined in the assignment only when small.
- The cache domain exposes behaviour, not configuration: `#[port]` traits `ContentStore` (verified writes) and `ActionStore`, as associated types of `CacheStorePort` that `compose!` dispatches. S3 moves into `gang-aws`, and `gang-local` reuses `gang-cas`.
- Repeated input subtrees are cached immutably and bound read-only only where the sandbox can bind. Every attempt gets a fresh directory. On completion that directory is renamed into a graveyard, and a bounded pool unlinks it under capped, exact disk credit.
- Learned history keeps its record keys and v1 body byte for byte, proven by golden fixtures from the fork.
- NativeLink is deleted in one cutover, after end-to-end parity, an A/B benchmark against `v0.3.9`, a CPU-weight contention canary and an ActionCache invariance check. The engine is built beside it with additive contracts through Phase 1, then switched in atomically to run those parity and benchmark gates before NativeLink is deleted.

## Consequences

- No fork to rebase, no translation layer, and no JSON configuration path. Every former patch becomes a domain rule with one owner and a test, or disappears by construction: the cleanup wait, the `operation_id` collision, the completion-count decorator, the snapshot decode and the store-metric scraper.
- Scheduler correctness becomes Gang's to prove. A deterministic simulator over the pure domain, ported reference regressions and the e2e gates carry that burden.
- Each deliberate change is noted in the CHANGELOG:
  - `gang bazelrc` renders `--remote_execution_priority=100`, so unconfigured clients become background;
  - CPU weights by tier;
  - ZSTD advertised, but clients are not opted in;
  - `[worker] cleanup_wait` is removed (a configuration break);
  - read-only input trees under `bwrap`.
- `CacheStorePort`, `ControllerSpec`, `WorkerSpec`, `ActionExecutionSpec` and `ActionLimits` change contract; their consumers migrate in the same commit.
- Removing NativeLink removes its transitive graph (including `mongodb` and `redis`), and nothing then pins the workspace toolchain to NativeLink's `rust-version`.

## Rejected alternatives

- **Keep the fork and upstream the patches:** none of the 46 is upstream (SPECIFICATION §3.2 records the worker fixes as still absent in v1.8.6, with no issue filed), and the translation layer would stay either way.
- **REAPI-style lower-runs-first:** flips live callers that already send 200/100/10 and the dashboard's `↑p`, with no gain.
- **Priority or weight as a platform property or environment variable:** changes ActionCache keys and splits the cache.
- **Pack onto the busiest workers:** couples placement to scale-in and concentrates contention and pressure kills; ADR-60 already owns scale-in.
- **Sharded or lock-based scheduler state:** splits the ledger that preference and placement need globally, with no throughput need at Gang's scale.
- **Keep NativeLink's worker API:** its unary updates cannot express early slot release, decline reasons or out-of-band weights without more fork patches.
- **Directory clones or link-loop fallbacks for trees without bind support:** extra paths whose integrity differs from the bound case. Without binds, files are staged normally.
- **`rules_rust_prost` codegen:** splits the Bazel and darwin Cargo builds.
