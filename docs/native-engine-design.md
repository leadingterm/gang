# Native Execution Engine: Design

Companion to [ADR-62](adr/adr-62-native-execution-engine.md). The ADR records the decision; this
document is the build contract: crate boundaries, SPI changes, data structures, protocols,
configuration, verification, and the work-package DAG. Behaviour that does not change stays
specified where it already is (SPECIFICATION §§3.2, 3.7, 3.8, 5) and is referenced, not copied.

Status: Accepted with revisions (Carbon review, P0–P2 closed; decisions in §12). Baseline:
`origin/main` @ `v0.3.9` (`f0ec0a55`).

---

## 1. Scope

**Replace**, behind `gang_spi::SchedulerPort`: the embedded NativeLink v1.8.3 fork (9 crates at
`leadingterm/nativelink@aee3d6d8`, 46 carried commits) and the 22,169 lines of
`crates/gang-nativelink/src` (inline tests included) that translate between it and Gang.

**Keep unchanged:** every `SchedulerPort` method and its documented semantics, including
*higher priority runs first* (`gang-spi/src/scheduler_port.rs:357`, the dashboard's `↑p` in
`ui/src/ui.tsx:122`); `gang-server`'s leadership, handoff (ADR-59), reconciler and dashboard;
ADR-60 and SPECIFICATION §5.1 scaling; the `gang exec` entrypoint contract (§3.7); every
`[actions]`, `[worker]`, `[server]`, `[server.http2]` key and default except `[worker] cleanup_wait`
(§9); the learned-history record keys and v1 body encoding; ActionCache keys (the action's
platform and environment are never touched by scheduling QoS, §3.2).

**Non-goals:** persistent workers, remote output service, REAPI v2 `Execution` features Bazel does
not use, multi-controller active-active scheduling, scheduler reattachment after crashes (ADR-59
keeps retry semantics), dual-session workers during handoff (§12 Q6).

---

## 2. What NativeLink carries today, and where each piece goes

| Responsibility (today) | Where it lives today | Disposition |
|---|---|---|
| Queue, matching, worker ledger, dispatch, retries | `nativelink-scheduler` (`SimpleScheduler`, memory `AwaitedActionDb`) | **Rewrite** in `gang-scheduler` (pure) |
| Learned CPU/memory/disk reservations (`ResourceSketch`, v1 codec) | fork `resource_sketch`, `history.rs` | **Port byte-exact** to `gang-scheduler::sketch` + `history` |
| Learned durations, completion rates, first-creator memo | `durations.rs`, `source_rates.rs`, `observing.rs` | **Move** into `gang-scheduler` (already Gang code) |
| Fitting, placeability, observed capacity, catalog equivalence | `fitted.rs`, `placeable.rs`, `capacity.rs`, `equivalence.rs` + fork `1771cc2e`, `11314249` | **Fold** into `gang-scheduler` admission/fitting; the NativeLink-vs-catalog equivalence check is deleted (one matcher now) |
| Memory/disk kill escalation, kill floors | fork escalation commits, `c86940ed` | **Port** to `gang-scheduler::escalation` |
| Cordons, `retain_drains`, stale-build replacement | `cordons.rs` + `set_drain_worker` | **Move** to `gang-scheduler::cordons` (single owner; no connection self-drain mirror) |
| Dispatch-stamped attempt deadlines | fork `0e75592a`, `c74086b3`, `14cb9658` | **Native** in `gang-scheduler::attempts` and the worker runner |
| Snapshot / actions listing | `snapshot.rs` (2,018), `actions.rs` (1,051): walks `filter_operations`, decodes metadata | **Replace** by direct projections of domain state |
| Store usage | `store_usage.rs` (538): scrapes `tracing` metric spans | **Delete**; stores report `EngineStore` directly |
| Typed config rendering to NativeLink JSON | `settings.rs` (1,619), `config.rs`, `stores.rs`, `globals.rs`, `RenderedStoreSpec` | **Delete** |
| Serve loop, request gate, connection standing, refusal logging, build observer, handoff stream release | `serve.rs`, `gate.rs`, `standing.rs`, `refusals.rs`, `observer.rs`, `handoff.rs`, `acquisition.rs` | **Move verbatim** (Gang code) to `gang-engine`; only the router they wrap changes |
| REAPI services | `nativelink-service` | **Rewrite** in `gang-engine::reapi_*` on `gang-reapi` stubs |
| Worker API (`ConnectWorker`, unary updates) | `nativelink-service` + `nativelink-worker` | **Replace** by `leadingterm.gang.worker.v1` bidi stream (§8.2) |
| Fast/slow CAS, filesystem, S3 and gRPC stores | `nativelink-store` | **Rewrite**: `gang-cas` (fast store, tiering, remote), S3 in `gang-aws`, filesystem via `gang-cas` in `gang-local` |
| Running-actions manager: staging, entrypoint spawn, upload, cleanup | `nativelink-worker` + fork `aee3d6d8`, symlink rebase, kill outcome, side-channel memory | **Rewrite** in `gang-engine::worker_*` with the fixes as native behaviour |
| Memory PSI / disk pressure enforcement | fork pressure commits | **Port**: window/victim policy to `gang-scheduler::pressure` (pure), sampling I/O to `gang-engine` |
| Clippy/fixture/test-compile fixups (`448f90b9`, `7ba4c52a`, `fb465050`, `7b3bb47c`, `90e4318d`, `7e2b3348`, `9c6235d4`, `9b94041b`, `787df634`, `c5177d8c`) | fork | **Delete**: no fork to maintain |

Workarounds that disappear structurally (each has a regression test in §10.3):

- `wait_for_cleanup_if_needed` / `CleanupInFlightGuard` / `AlreadyExists` on `operation_id` reuse:
  every attempt gets a fresh `AttemptId` directory (§6.5).
- The completion-counting `WorkerScheduler` decorator and its `clippy::missing_trait_methods`
  guard: completions are domain events.
- The `filter_operations` walk (`1 + busy workers + 1` listings per call) and the metadata decode
  that skips undecodable operations: the domain owns typed rows.
- The `enduser.id` baggage override: the gate's `Admitted` principal is a typed `execute`
  argument.
- `capacity_ignored_properties`, `action_ceilings` on `ConnectWorkerRequest`, and the JSON
  `supported_platform_properties` table: matching reads `FleetCatalog` directly.
- `globals::init` (process-wide digest hasher): SHA-256 is a type.

---

## 3. Contract changes

All land atomically with every consumer (production and tests). Phase 0 adds new items
additively; Phase 2 (H1) switches to the new ones and removes the old ones.

### 3.1 Typed stores replace `RenderedStoreSpec`

`RenderedStoreSpec(serde_json::Value)` leaks NativeLink's configuration schema through the cache
domain. It is replaced by two behavioural port traits in `gang-spi` (`1_domain_ports`, tier 15),
declared with `#[port]` and `async fn` like every other port:

```rust
// gang-types (0_kernel): Copy, 40 bytes, Display "<hex>/<size>", FromStr, Ord.
pub struct Digest { hash: [u8; 32], size: u64 }
impl Digest { pub const EMPTY: Digest; /* e3b0c442…b855/0 */ pub fn is_empty(&self) -> bool; }

#[port]
pub trait ContentStore: std::fmt::Debug + Send + Sync + 'static {
    /// Clears `missing`, then pushes each digest of `digests` the store lacks, in input order.
    /// `Digest::EMPTY` is never missing.
    async fn find_missing(&self, digests: &[Digest], missing: &mut Vec<Digest>) -> Result<(), PortError>;
    /// Streams `range` of the blob, or `Ok(None)` when absent or expired (§7.4).
    async fn read(&self, digest: Digest, range: ByteRange) -> Result<Option<BlobStream>, PortError>;
    /// Stores `body`; visible only if its bytes hash to `digest`; a mismatch is
    /// `PortError::Rejected(Rejection::DigestMismatch)` and leaves nothing visible.
    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError>;
    /// Usage of an engine-bounded store; `None` for provider-bounded stores (S3).
    fn usage(&self) -> Option<EngineStore>;
}

#[port]
pub trait ActionStore: std::fmt::Debug + Send + Sync + 'static {
    async fn get(&self, action: Digest) -> Result<Option<bytes::Bytes>, PortError>;
    async fn put(&self, action: Digest, result: bytes::Bytes) -> Result<(), PortError>;
    fn usage(&self) -> Option<EngineStore>;
}

pub struct BlobStream { pub size: u64, pub chunks: BoxStream<'static, Result<bytes::Bytes, PortError>> }
pub struct ByteRange { pub offset: u64, pub limit: Option<NonZeroU64> }
```

`ByteRange` semantics (REAPI ByteStream `Read`): `offset == size` yields an empty stream;
`offset > size` is `PortError::Rejected(Rejection::OutOfRange)`; `limit = None` reads to the end;
`offset + limit > size` is clipped to `size`. The empty digest is answered in memory by every
caller layer (`gang-cas`, the controller services, the worker) and never reaches disk or S3; the
store contract suite (`gang-testkit`) asserts these edges against every implementation.

`CacheStorePort` gains associated types:

```rust
#[port]
pub trait CacheStorePort: Send + Sync {
    type Content: ContentStore;
    type Actions: ActionStore;
    fn content_store(&self) -> Result<Self::Content, PortError>;
    fn action_store(&self, environment: &EnvironmentName) -> Result<Self::Actions, PortError>;
    fn worker_access(&self) -> StoreAccess;                                       // unchanged
    async fn measured_usage(&self) -> Result<Option<MeasuredUsage>, PortError>; // unchanged
}
```

The `std::fmt::Debug` supertrait is required because `compose!` derives `Debug` on the dispatch
enum it generates for each associated type.

`compose!` support, in two steps:

- **WP 0.2 (additive):** add the two-argument delegation arm
  `($enum_ty:path { $($variant:ident),* })` to the `macro_rules!` that `#[port]` emits
  (`crates/gang-macros/src/port.rs`), forwarding `async fn` methods by
  `match self { V(inner) => inner.method(args).await }` and synchronous `usage(&self)` directly.
  Re-export `ContentStore`, `ActionStore`, `BlobStream`, and `ByteRange` from
  `gang-spi/src/lib.rs`, and re-export `pub use ::bytes;` and `pub use gang_types::Digest;` in
  `gang-spi/src/private.rs` so generated `__gang_delegate_*!` macros resolve inside caller crates
  (such as `gang-main`) that do not depend on `bytes` directly. A macro test in an isolated module
  without local `bytes`/`Digest` imports applies `__gang_delegate_ContentStore!` and
  `__gang_delegate_ActionStore!` to a two-variant enum and proves each method reaches the selected
  variant.
- **WP H1 (with the associated types):** register `Associated { Content, Actions }` for the cache
  domain in the `DOMAINS` table of `crates/gang-macros/src/domain.rs` (the mechanism
  `limits_port`'s `Scope` already uses), add `type Content`/`type Actions` to `CacheStorePort`,
  and add the full `compose!` test with two cache plugins.

`ControllerSpec` loses `content_store`/`action_store`; `run_controller` takes
`ControllerStores<C, A> { content: Arc<C>, actions: Arc<A> }`. `WorkerSpec` loses `content_store`,
`action_store` and `store_access`; `run_worker` takes
`WorkerStores<C, A> = Direct { content: Arc<C>, actions: Arc<A> } | ViaController`. Access mode
and handles are one fact; `Arc` sharing means the generated enums need no `Clone`. In `gang-worker`,
branch on `cache.worker_access()` first and call `cache.content_store()` / `cache.action_store(...)`
only when `StoreAccess::Direct` (never opening a `FilesystemCache` `DiskCas` on a `ViaController`
worker).

### 3.2 Priority stays higher-runs-first; CPU weight is out of band

**Queue order.** Gang's documented server semantics stay: *higher `ExecutionPolicy.priority`
runs first*, ties oldest first. REAPI explicitly leaves value interpretation server-dependent.
Queue key: `(Reverse<i32>, u64 seq, OperationKey)`. `GetCapabilities` advertises
`execution_priority_capabilities = [i32::MIN, i32::MAX]`; negative values are accepted.

**Client default.** `gang bazelrc` and `gang setup` render
`build --remote_execution_priority=100` (`crates/gang-client/src/bazelrc.rs`, WP H1), so
developer builds are interactive by default; raw `0` (unconfigured clients) is background.

**Dedup promotion.** When `Execute` deduplicates onto a `Queued` operation with
`incoming.priority > op.priority`, `op.priority` and `op.cpu_weight` are raised and the operation
is re-indexed keeping its original `seq`. On a `Running` operation the promotion is recorded and
applies to any worker-loss or escalation retry. Priority is never lowered by deduplication.

**`CpuWeight`** (`gang-types`, `NonZeroU16` in `1..=10_000`, the cgroup v2 `cpu.weight` range).
One pure core-scaled mapping in `gang-scheduler::qos`:
`cpu.weight = clamp(tier_per_core * max(1, ceil(reserved_millicores / 1000)), 1, 10_000)` (leaving
`cpu.max` untouched for `cpu_millicores` reservations so actions stay work-conserving):

| `priority` | Tier | `tier_per_core` (1-core `cpu.weight`) |
|---|---|---|
| `>= 200` | gate | 1000 |
| `100..=199` | interactive | 100 |
| `< 100` (incl. `0`, negatives) | background | 20 |

The weight travels only in `worker.v1::Assign.cpu_weight` (§8.2) and the per-attempt side
channel. It is **never** added to `Assign.platform` or the action environment, so ActionCache
keys are unchanged (guarded by `gang-e2e/src/action_environment_probe.rs` and the G3 ≥ 99% hit
check). Authorization: every authenticated caller may use `>= 200` initially; the controller logs
`(provider, principal, priority)` for every admitted gate-tier `Execute` so a later policy can be
evidence-based.

### 3.3 Read-only input trees and CPU weight reach the sandbox and limits

- `ActionExecutionSpec` gains `read_only_trees: &'a [ReadOnlyTree<'a>]`
  (`ReadOnlyTree { source: &Path, target: &Path }`, `target` strictly inside
  `action_directory`), and `ActionIsolationPort` gains `fn binds_read_only(&self) -> bool`
  (`gang-bwrap`: `true`, `--ro-bind source target` after the work bind; `gang-noop`: `false`).
- `ActionLimits` (`gang-spi/src/limits_port.rs:102`) gains `pub cpu_weight: Option<CpuWeight>`.
- The worker writes both into `.side_channels/<attempt_id>/` (worker-written, action-unwritable,
  already validated by `gang exec`): `read_only_trees` (at most `MAX_RO_BINDS = 64` entries) and
  `cpu_weight`. `gang exec` walks each bind target **component by component** from the action
  directory descriptor, `openat(parent, component, O_NOFOLLOW | O_DIRECTORY)` per component (a
  single multi-component `openat` would check only the final component), and refuses a symlinked,
  non-directory or missing component; `gang-bwrap` refuses more than `MAX_RO_BINDS` entries.
  Neither crosses the environment.
- `gang_exec::ActionDirectory::try_new` (`action_directory.rs:46`) reserves `.graveyard` alongside
  `.side_channels`, so neither can be an action id.

### 3.4 Hierarchical CPU weights in `gang-cgroup` (WP F3)

In `Controller::prepare` (`gang-cgroup/src/controller.rs`, after
`self.write(subtree, SUBTREE_CONTROL, ENABLE_CONTROLLERS)`): write `S/worker/cpu.weight = 1000`
and `S/actions/cpu.weight = 1000`. Explicit equal sibling weights pin the worker-vs-actions split
at 1:1 under full contention whatever the delegating supervisor set; with unequal siblings
(1000 vs. the default 100) actions would get 9.1%. Worker-side CPU work (upload hashing,
compression) is further bounded by `[worker] hash_concurrency = min(vCPUs, 4)`. In
`Controller::apply` (per action scope) write `S/actions/<scope>/cpu.weight` from
`ActionLimits::cpu_weight` when present, so gate, interactive and background actions share the
action pool 1000:100:20 under contention and run unthrottled when idle.

---

## 4. Crate layout

| Crate | Ring | Tier | Role | Est. LOC |
|---|---|---|---|---|
| `gang-types` | `0_kernel` | 1 | adds `Digest`, `AttemptId`, `OperationName`, `CpuWeight` | +350 |
| `gang-spi` | `1_domain_ports` | 15 | §3 contract changes | +450 |
| `gang-macros` | `0_kernel` | 0 | cache associated types; two-argument delegation arm | +150 |
| **`gang-scheduler`** | `2_application` | 25 | Pure scheduling domain for both roles: queue, admission, QoS mapping, fitting, ledger, placement, learning (sketch, durations, v1 codec), escalation, cordons, attempts, pressure policy, projections. `std` + workspace crates + `serde`/`thiserror`: no Tokio, no I/O, no clock reads | ~8,000 |
| **`gang-reapi`** | `3_driven_adapters` | 27 | Checked-in generated prost/tonic code: REAPI v2 subset, `google.bytestream`, `google.longrunning`, `google.rpc`, `leadingterm.gang.worker.v1`; `Digest` conversions | generated |
| **`gang-protogen`** | `tooling` | - | Regenerates `gang-reapi`'s sources (`tonic-prost-build`); a Bazel test diffs them | ~150 |
| **`gang-cas`** | `3_driven_adapters` | 29 | `DiskCas`, `SingleFlight`, `Tiered` fast/slow, `TreeCache`, zstd codec, `RemoteCas` (ViaController gRPC client). Depends on `gang-os`, `gang-tls` (28), `gang-reapi` (27) | ~4,000 |
| **`gang-engine`** | `3_driven_adapters` | 30 | Fixed adapter `Engine: SchedulerPort`: controller actor shell, REAPI + worker services, transport moved from `gang-nativelink`, worker runtime | ~9,000 + 3,500 moved |
| `gang-aws` | `3_driven_adapters` | 30 | `S3Cache` implements `ContentStore`/`ActionStore` with `aws-sdk-s3`, its client built through `gang-aws-sdk` | +700 |
| `gang-local` | `3_driven_adapters` | 30 | `FilesystemCache` = `gang_cas::DiskCas` + a disk `ActionStore` | −300 net |
| `gang-cgroup` | `3_driven_adapters` | 30 | sibling and per-scope `cpu.weight` (§3.4) | +80 |
| `gang-nativelink` | — | — | **Deleted** | −22,169 |

Placement notes:

- Gang's `1_domain_ports` holds config and ports; pure domain engines (`gang-reconciler`,
  `gang-insight`, `gang-state`) sit at `2_application` tier 25–26 with no I/O, tasks or clock
  reads. `gang-scheduler` joins them at 25 and does not depend on `gang-reconciler`.
- `gang-cas` is storage mechanics, so Ring 3 (PLAN §1). Tier 29 sits beside `gang-aws-sdk` (no
  edge between them) and below the tier-30 plugins and `gang-engine`, so `gang-local` reuses it.
- Codegen: vendored `.proto` files under `third_party/remote-apis/` (pinned upstream commit,
  Apache-2.0, `NOTICE` updated) and `proto/leadingterm/gang/worker/v1/`; `gang-protogen` (pattern:
  `gang-bindings`) writes `crates/gang-reapi/src/*.rs`; a Bazel test regenerates and diffs. One
  codegen path for Bazel and the darwin Cargo check; no `build.rs`, no host protoc.
- `aws-sdk-s3` (already in `Cargo.lock` through NativeLink) becomes a direct workspace dependency
  in root `Cargo.toml` and `gang-aws/Cargo.toml` with one `Cargo.Bazel.lock` repin (WP 0.6).
- `unsafe`: none. Reflink is `rustix::fs::ioctl_ficlone`, `O_TMPFILE` + `linkat` and `futimens`
  are `rustix` safe wrappers, as already used by `gang-exec/src/private_copy.rs`.
  `gang-os/src/sys.rs` stays the only `#![expect(unsafe_code)]` file.
- Lints: every new crate inherits `[workspace.lints]`; state machines take `&mut self`; the
  `clippy.toml` bans on std `Mutex`/`RwLock`/`RefCell`/`Rc` hold (`parking_lot` where a lock is
  genuinely shared).

---

## 5. `gang-scheduler`: the pure domain

### 5.1 Shape

One deterministic state machine per controller term, driven by its caller:

```rust
pub struct Scheduler { /* §5.2 */ }

impl Scheduler {
    pub fn new(policy: &Policy, catalog: &FleetCatalog, learned: Learned) -> Result<Self, PolicyError>;
    pub fn execute(&mut self, now: Now, request: ExecuteRequest<'_>, out: &mut Effects) -> Result<OperationKey, Refusal>;
    pub fn watch(&mut self, now: Now, operation: &OperationName) -> Option<OperationKey>;
    pub fn unwatch(&mut self, now: Now, key: OperationKey, out: &mut Effects);
    pub fn cancel(&mut self, now: Now, key: OperationKey, out: &mut Effects);
    pub fn register(&mut self, now: Now, registration: Registration<'_>, out: &mut Effects) -> Result<WorkerKey, RegistrationError>;
    pub fn on_worker(&mut self, now: Now, worker: WorkerKey, event: WorkerEvent<'_>, out: &mut Effects);
    pub fn disconnect(&mut self, now: Now, worker: WorkerKey, out: &mut Effects);
    pub fn advance(&mut self, now: Now, out: &mut Effects);   // due timers only
    pub fn next_wakeup(&self) -> Option<Tick>;
    pub fn set_draining(&mut self, now: Now, fleet: &FleetName, instance: &InstanceId, draining: bool, out: &mut Effects);
    pub fn retain_drains(&mut self, now: Tick, grace: Duration, census: &Census);
    pub fn hand_off(&mut self, now: Now, into: &mut ControllerDrain, out: &mut Effects);
    pub fn snapshot(&self, into: &mut SchedulerSnapshot);
    pub fn actions(&self, limits: ListingLimits, into: &mut ActionsView);
    pub fn learned(&self) -> &Learned;
    pub fn into_learned(self) -> Learned;
}
pub struct Now { pub tick: Tick, pub wall: Timestamp }
/// Compact provider census: (fleet, instance) pairs, sorted and unique.
pub struct Census(Vec<(FleetName, Arc<InstanceId>)>);
```

`Learned` (sketch, durations, rates, capacity facts) has its own
`snapshot(now, &mut Vec<LearnedHistory>)`, `restore(now, &[LearnedHistory])` and
`byte_limit() -> NonZeroUsize`; it outlives a `Scheduler` (§6.1, P1-3).

`Effects` is a caller-owned, cleared-and-reused `Vec<Effect>`:
`Assign { worker, attempt }`, `Kill { worker, attempt, reason }`,
`SetDraining { worker, draining }`, `Publish { operation, stage }`,
`CloseWorker { worker, reason }`, `Observe(Completion)`, `AuditGate { source, priority }`.
Hot paths allocate nothing in steady state: effects, snapshot and listing buffers are reused;
operation and worker records live in slabs addressed by generation-checked `u32` keys.

### 5.2 State

- `operations: Slab<Operation>` + `HashMap<Digest, OperationKey>` for deduplication on action
  digest (never merging `skip_cache_lookup`): first creator's `SourceId`, `BuildId`, interned
  target/mnemonic, original platform (immutable admission floor), current fitted reservation,
  captured initial learned reservation, candidates (`FleetSet`, preference order), `priority`,
  `cpu_weight`, `seq`, retry counters (generic, worker-loss, memory steps, disk steps), stage,
  watcher count, `client_less_since`.
- `queue`: per-fleet `BTreeSet<(Reverse<i32>, u64, OperationKey)>` for placement, plus **one
  global** `BTreeSet` with the same key for `snapshot`/`actions` (no k-way merge or dedup). An
  operation sits in the index of every candidate fleet; enqueue/dequeue cost
  `O((|candidates| + 1) · log n)`, `|candidates| ≤` fleets.
- `workers: Slab<Worker>` + `BTreeMap<WorkerId, WorkerKey>` (snapshot order for free): fleet
  index, `Arc<InstanceId>`, registered capacity, effective ceilings, `reserved`, attempts,
  `max_inflight`, `current_build`, drain sources (`self`, `cordon`, `controller`),
  `disk_hold_until_keepalive`, `last_assigned_seq`, `gate_running`, `last_seen`, cache stats.
- `instances: BTreeMap<(FleetIndex, Arc<InstanceId>), InstanceState>`: the worker generations of
  one physical instance (overlapping mTLS renewal generations), its cordon/drain intent, and its
  one capacity fact (latest generation's report wins; deduplicated per `InstanceId`).
- `ledger`: per worker `capacity − reserved`, CPU in millicores =
  `physical vCPU × 1000 × cpu_oversubscription` (SPEC §3.2), memory = registered post-reserve pool
  capacity, disk = `disk_per_instance`.
- `attempts: Slab<Attempt>`: `AttemptId` (fresh UUIDv4 per dispatch), operation, worker,
  `dispatched_at`, `ack_deadline`, `attempt_deadline = dispatched_at + attempt_budget`, phase
  (`Offered → Running → Executed → Completed`).
- `timers`: `BinaryHeap<Reverse<(Tick, TimerKind, key, generation)>>`; stale entries are skipped by
  generation; `next_wakeup` is its head.
- `cordons`: keyed by `(fleet, Arc<InstanceId>)` with `{ replacing, absent_since }` (port of
  `cordons.rs` semantics), applied to every generation of the instance.

**`WorkerId` minting:** `format!("{}{uuid}", instance.worker_name())` with
`InstanceId::worker_name` (`gang-types/src/names.rs:319`), preserving the SPI rule that a worker
id starts with its instance's id and the existing round-trip test.

### 5.3 Admission (on `execute`)

One function replaces the three decorators (`PlaceableActions` → `HistoricalResourceScheduler` →
`FittedActions`):

1. Validate original client properties against the catalog (exact selectors, numeric minimums ≤
   effective ceiling, capability grants, harmless-metadata allowlist, unknown keys) → `Refusal`
   (`INVALID_ARGUMENT` naming property and fleets; SPEC §3.2 "Match rule").
2. Candidates = `FleetCatalog::accepting` in preference order; empty → `Refusal`.
3. `cpu_weight = qos::weight(priority)` (§3.2); gate tier emits `AuditGate`.
4. Learned reservation from the sketch (when a target label is present: `target+mnemonic → target`
   without cross-target mnemonic fallback so one outlier test never poisons an entire mnemonic;
   when no target label is present: `mnemonic → command`), else cold start; headroom, configured
   minima, capped at the largest eligible effective fleet ceiling; capture the initial learned
   snapshot.
5. Fit with `FleetCatalog::fit` within the admitted set, then authoritative physical-memory
   fitting against current capacity facts (whole-worker CPU when memory is clamped to full
   physical capacity, bounded by the CPU ceiling). Originals remain floors.
6. Deduplicate (with promotion, §3.2) or insert; enqueue; `Publish(Queued)`; place (§5.4).

### 5.4 Placement: distributed (ADR-60 unchanged)

Eligibility `eligible(w, op) = !draining && !cordoned && !paused && current_build &&
fleet(w) ∈ op.candidates && inflight < max_inflight && fits(op.reservation, w.free) &&
!disk_hold`. Two triggers share it:

- **Operation-driven** (enqueue, promotion): take the highest-preference candidate fleet that has
  an eligible worker; within it choose the worker with the **lowest reserved CPU fraction**
  (`reserved_cpu / capacity_cpu`, compared exactly as integer cross-products), then fewest
  running gate-tier attempts, then least recently assigned (`last_assigned_seq`), then `WorkerId`.
  Spreading keeps contention, pressure kills and cgroup weight competition low; scale-in remains
  the reconciler's N−1 decision (ADR-60, SPECIFICATION §5.1), not a placement side effect.
- **Worker-driven** (capacity freed on `w` in fleet `f`: `Executed`, `Completed`, registration,
  un-drain, cordon removal, disk-hold expiry): walk `queue[f]` in key order and assign each
  operation that fits until `w` is full or `dispatch_scan_limit` non-fitting entries have been
  skipped. Every operation in `queue[f]` is waiting because no eligible worker existed when it was
  last considered, so filling `w` respects preference.
- **Anti-starvation headroom hold:** When the highest-priority queued operation for a fleet fits
  an empty eligible worker (`op.reservation ≤ w.capacity`) but currently cannot fit any worker's
  free capacity, the scheduler pins a single eligible worker in that fleet to drain (refusing
  smaller operations on that held worker across both operation-driven and worker-driven placement)
  until the large operation is placed or leaves the queue.

Each assignment reserves on the ledger, creates an `Attempt` (`Offered`), arms `ack_deadline`,
emits `Assign`, removes the operation from all indices, and stamps `w.last_assigned_seq`.
There is no placement configuration knob.

### 5.5 Attempt lifecycle

| Event | Effect |
|---|---|
| `Accept` before `ack_deadline` | `Running`; `Publish(Executing)`; disarm ack timer |
| `Decline(DiskShortage)` | requeue at original `seq`, no retry spent; `disk_hold` until the worker's next keepalive (SPEC §3.2 disk limits) |
| `Decline(Draining)` | requeue at original `seq`, no retry spent |
| ack timeout | worker-loss for this attempt (worker-loss budget); `Kill` sent best-effort |
| `Executed { usage, kill }` | release CPU and memory (processes are gone); keep disk until `Completed`; record sketch sample; classify kill → escalation (§5.7) |
| `Completed { result }` | release disk; `Publish(Completed)`; completion, duration and rate updates; `Observe` |
| `Completed { status }` non-OK, not a resource kill | generic retry (`max_job_retries`) or terminal |
| `attempt_deadline` passes | `Kill(DeadlineExceeded)`; terminal `DEADLINE_EXCEEDED`, no requeue (fork `0e75592a`); reservation released when the worker reports `Executed` or is lost |
| worker disconnect / `worker_timeout` | every attempt requeued at original `seq` with the worker-loss budget (`max_worker_loss_retries`) and any promoted priority/weight |
| no watcher for `client_action_timeout` | queued: drop; running: `Kill(Cancelled)`, terminal |
| `CancelOperation` | as client-less, immediately |

Invariants: an operation reaches a terminal stage exactly once; a worker's reserved CPU and memory
equal the sum over its `Offered`/`Running` attempts, its reserved disk the sum over its
non-`Completed` attempts.

### 5.6 Learning and the v1 codec

`ResourceSketch` (two half-window log2 histograms per key; CPU percentile from
`cpu_reservation_percentile` over lifetime-average millicores; p95 of memory/disk peaks; killed
floors; seeded entries; `SAMPLER_GENERATION`) is ported from the fork as pure code. The record
layout is kept exactly: `[VERSION=1][window_secs u64 BE][Snapshot::encode()]`, key ids from
`history_id(tag, parts)` with the `gang-learned-history-key-v1\0` domain. Continuity is proven by
golden fixtures from the fork's real collectors (`history_fixture.rs`): decode(fork bytes) →
encode → identical bytes, and identical reservations for a fixed query set. Duration records
move unchanged. `Learned::byte_limit` keeps today's formula (sketch bound + 9 vs. the
header-derived duration bound).

### 5.7 Escalation (port of the fork's escalation commits)

For the killed resource:
`next = min(cap, max(ceil(peak·(100+headroom)/100), ceil(reservation·escalation_percent/100), reservation+1))`,
cap = largest eligible effective ceiling in the current capacity catalog; separate memory and disk
step budgets independent of `max_job_retries`; soft disk kills fail instead of requeueing; a
single terminal failure (naming action, peak, capacity, reason) on peak > cap, a further kill at a
lower policy cap, a further kill while holding physical capacity and whole-worker CPU, or budget
exhaustion. A side-channel `failure: memory` is an enforced memory kill unless a kill was already
classified (`c41a9690`). The formula and rules are SPEC §3.2's; this module is their single owner.

### 5.8 Worker-side pure policy

`gang-scheduler::pressure`: the PSI shortage window (`delta_stall·100 ≥ pct·elapsed` and
available < reserve; baseline reset on counter decrease, malformed sample or non-monotonic time),
the disk free-space window with the graveyard credit rules of §6.5, the soft-ceiling
sample/spike counters, victim selection (greatest positive usage − reservation, else greatest
usage; excluding finished and already-killing attempts) and the post-kill quiescence rule. All
inputs are numbers and `Tick`s; the worker samples the files.

### 5.9 Projections

`snapshot` and `actions` read domain state and fill the caller's reused buffers: workers in
`WorkerId` order; queued in global-index order (`O(limit)` iteration); executing ordered by
operation name; `truncated_*` from counts; `completed` is a domain counter; `source_rates_at` is
the read's `now`. Nothing is decoded, so nothing is skipped.

### 5.10 Concurrency model

**Single-owner actor per controller term.** The `Scheduler` is owned by one Tokio task in
`gang-engine`. Three bounded inboxes, drained with strict bias each iteration:

1. `advance(now)` for every due timer (acks, deadlines, keepalive expiry, client-less timeouts).
2. `control_rx` (`snapshot`, `actions`, `set_draining`, `retain_drains`, `hand_off`, learned
   history): drained completely.
3. `worker_rx` (registrations, accept/decline, `Executed`, `Completed`, keepalives, disconnects):
   up to `scheduler_batch` messages.
4. `client_rx` (`Execute`, watch/unwatch, cancel): up to `scheduler_batch` messages.
5. Apply accumulated `Effects`; sleep until the next message or `next_wakeup`.

An `Execute` burst therefore never delays a reconciler `snapshot` or a worker `Accept`/
`KeepAlive` by more than one batch. Rationale for one owner: every scheduler step is `O(log n)`
CPU with no I/O; at Gang's scale (hundreds of concurrent actions, thousands queued, hundreds of
workers) one core sustains >10⁵ commands/s; sharding would split the ledger that preference and
placement need globally. No lock exists, so none is held across `.await`.

Reads carry the caller's owned buffer (`mem::take`) and return it filled: zero steady-state
allocation and a consistent cut. `control_acquire_timeout` bounds *enqueueing* a control command;
once accepted it runs to completion (unchanged SPI semantics). `retain_drains` builds a compact
`Census` from the borrowed `&[(&FleetName, &FleetSupply)]` before sending; the
`SchedulerPort::retain_drains` doc drops its "asynchronous write lock" wording (H1). Client
streams are `tokio::sync::watch` channels owned by the shell; the actor never awaits a client.

---

## 6. `gang-engine`

### 6.1 Controller shell

```
            ┌──────────── public conns (gate P) ───────────┐   ┌── worker conns (gate W) ──┐
            │ serve / standing / refusals / observer        │   │ serve / standing           │
            │ router: Capabilities, CAS, ByteStream, AC,    │   │ router: worker.v1, CAS,    │
            │         Execution, Operations, fallback → api │   │         ByteStream, AC     │
            └───────────────┬───────────────────────────────┘   └────────────┬───────────────┘
                            │ client_rx                         worker_rx    │
                            ▼          control_rx ← SchedulerPort calls      ▼
                       ┌──────────────── scheduler actor (owns Scheduler) ────────────────────┐
                       │ loop { advance; control*; worker≤B; client≤B; apply Effects; sleep } │
                       └───────┬──────────────────────┬──────────────────────┬────────────────┘
                               │ Assign/Kill/Drain    │ Publish              │ Observe/Audit
                               ▼                      ▼                      ▼
                     per-worker outbound mpsc   per-op watch channel    ObservationSink / log
```

- Outbound worker messages are `try_send` into a bounded per-worker channel
  (`worker_outbound_depth`); a full channel closes that worker rather than blocking the actor.
- Stores arrive as `ControllerStores<C, A>`. Under `Direct` the controller still serves CAS/AC to
  Bazel and streams through (§7.4).
- `ControllerReady` is signalled after policy validation and store open, before the first
  connection is served (unchanged).
- **Learned state survives the term (P1-3).** `Engine` holds
  `learned: parking_lot::Mutex<LearnedSlot>` = `Live(control sender) | Retained(Learned) | None`
  and a `byte_limit: OnceLock<NonZeroUsize>` set at controller readiness. While serving,
  `learned_history`/`restore_learned_history` are control commands; when the actor exits it
  returns `Scheduler::into_learned()` into `Retained`, so the post-drain persistence flushes
  (`gang-server/src/server.rs:2642`, `:3183`) read the final state. `learned_history_byte_limit`
  stays synchronous and reads `byte_limit`. The lock is never held across `.await`: a reader
  clones the `Live` sender, releases the lock, then sends. If the actor exits in between, the
  send or its `oneshot` reply fails; the reader then re-locks and reads `Retained`, which the exit
  path wrote before dropping the receiver.
- `hand_off`: the actor freezes dispatch and registration, sets every worker
  `draining(controller)`, initiates public GOAWAY through the moved `handoff.rs`, and fills
  `ControllerDrain` from instances with non-terminal attempts (latest `attempt_deadline`). When a
  worker's attempts are terminal the controller ends its session with `Rehome`; the worker redials
  and reaches the new leader. `complete` = no non-terminal attempts and all pre-handoff public
  streams finished. Worker-store requests on the old controller keep `RequestScope::WorkerStore`.
- `LeaderUnavailable`, `RequestScope`, `GateDecision::NotLeading`, HTTP/1.1+HTTP/2 detection,
  connection limits, receive windows, idle eviction and refusal logging are unchanged because the
  code moves verbatim.

### 6.2 Worker session

One `worker.v1` bidi stream per worker generation (§8.2). The session task owns the stream; each
attempt runs in its own task in a `JoinSet` owned by the worker future (dropping `run_worker`
aborts them, as documented). Reconnect uses exponential backoff with jitter capped at
`connect_timeout`; a `Rejection::PrincipalRevoked` refusal ends `run_worker` with
`PortError::Backend` and never reconnects (unchanged). The application `KeepAlive` is sent every
`connect_timeout / 2` (the existing worker keepalive rule) carrying `WorkerCacheStats` and the
capacity fact; HTTP/2 PINGs keep `[worker] keepalive_interval`. `Rehome` ends the session after
the worker's attempts are terminal; the worker redials at once.

### 6.3 Input staging

Given `input_root_digest`:

1. **Resolve the Merkle tree.** `ViaController`: one `GetTree` per uncached root. `Direct`:
   breadth-first batched reads from the `ContentStore` per level. Parsed `Directory` messages stay
   in a byte-bounded LRU (`directory_cache_budget`) keyed by digest.
2. **Choose read-only trees** — only when `isolation.binds_read_only()` and
   `[execution] input_trees = "bind"`. A subtree qualifies when its recursive file count ≥
   `tree_cache_min_files` and none of these hold: `tree.starts_with(out) || out.starts_with(tree)`
   for any declared output path `out`; `cwd.starts_with(tree)` (one-way: the working directory
   stays writable and the input root `""` itself is never bound, while read-only subtrees such as
   `external/<toolchain>` under Bazel's default `cwd = ""` remain eligible); any ancestor within
   the input root is a symlink. At most `MAX_RO_BINDS = 64` per action, largest file count first,
   non-overlapping (an ancestor wins over its descendants). Otherwise no tree is bound and every
   file is staged by step 3–4: there is **no** link-loop or directory-clone fallback.
3. **Fetch missing blobs** for files not under a bound tree: `Digest::EMPTY` is created directly;
   local `find_missing` on `DiskCas`, then `Tiered` read-through with `SingleFlight` and bounded
   concurrency (`download_concurrency`); blobs ≤ `batch_blob_max` are batched up to the controller's
   batch limit.
4. **Materialize**: create `<work>/<attempt_id>/work/` and `<work>/<attempt_id>/tmp/` (the
   action's private temporary directory, which `gang-e2e`'s `verdict.rs` `siblings_hidden` and
   `action_environment_probe.rs` expect); create directories; hardlink each file from `DiskCas`
   (`EMLINK` → reflink, else copy), using the per-mode inode variant (§7.1); create symlinks as
   given; for each bound tree create an empty mountpoint directory.
   `TreeCache::get_or_materialize` (single-flight per digest, `admit_after = 1`) returns an RAII
   **`TreeLease`** on the immutable tree to bind, so cold and warm runs have the same read-only
   semantics. If the cache cannot make room (§7.3), it returns no lease and that subtree is staged
   as ordinary files.
5. Pin the blobs of unbound files until `Executed` (they are then only needed by the hardlinks
   already in the directory). The attempt holds its `TreeLease`s until `Executed` (or until the
   attempt is dropped); `DiskCas` `tree_pins` belong to the tree, not the attempt.

Layout is `gang_exec::ActionDirectory`'s: `<work>/<attempt_id>/work/<cwd>`. Mountpoints are empty
on the host, so `input_links = break` never walks bound trees; inside the sandbox they are
read-only (`EROFS` on write or `chmod`), so they need no private copy.

### 6.4 Execution

The runner writes `.side_channels/<attempt_id>/` (mode 0700: `read_only_trees`, `cpu_weight`)
and spawns the `entrypoint` (`gang exec`) in its own process group with `Command::env_clear()`.
It then builds the environment in this order, so the worker's own environment never leaks:

1. defaults `PWD` = canonical working directory and `TMPDIR` = canonical
   `<work>/<attempt_id>/tmp`;
2. the client's `Command.environment_variables`, which may override those defaults (neither is
   reserved);
3. the reserved `GANG_*` values from the admitted platform, removing any that are absent, and
   `GANG_SIDE_CHANNEL_FILE`, which always win. It samples the scope named in `report.cgroup`
(`memory.current`, `memory.peak`) at `sample_interval`, feeds the pressure monitors (§5.8),
enforces `timeout`, `attempt_deadline` and `kill_grace` (`cgroup.kill` with valid metadata, else
process-group SIGKILL, wrapper-PID fallback), and reads the final report. The process is a child
subreaper with one zombie reaper (unchanged). `Executed { usage, kill }` is sent **before**
upload, so outcome and usage reach the scheduler whatever happens to uploads (the fork's
kill-outcome fix, native).

### 6.5 Output upload, publication and teardown

1. Collect outputs (`output_paths` for REAPI ≥ 2.1 clients, `output_files`/`output_directories`
   otherwise). Absolute symlink targets inside the action directory are rebased relative to the
   symlink's parent from a once-canonicalized action root; targets are not read; dangling allowed.
2. Hash on a bounded blocking pool (`hash_concurrency = min(vCPUs, 4)` by default), streaming
   SHA-256; build `Tree` messages for output directories.
3. Insert outputs into `DiskCas` by reflink, else hardlink (§7.1 rules), recording the bytes that
   became shared with `DiskCas` (`cas_shared_bytes`); `find_missing` on the slow store; upload only
   missing (batch small, ByteStream large, zstd when negotiated and above `compress_min_size`).
4. `ActionStore::put` when cacheable (`do_not_cache` false, exit 0); send `Completed { result }`.
5. **Graveyard:** `renameat(<work>/<attempt_id>, <work>/.graveyard/<attempt_id>)` (one syscall,
   same filesystem by construction) and enqueue to the unlink pool. The post-upload gap is the
   rename, not the tree removal. The side-channel directory goes with it after usage is consumed.
6. **Unlink pool:** `graveyard_workers` blocking tasks remove entries with `gang_os`
   descriptor-relative traversal (no symlink following, read-only directory repair; macOS
   `0000`/`0200` directories fail closed as in today's stale-work cleanup).

Disk exactness:

- Each entry's **reclaimable** bytes = last-walk size − `cas_shared_bytes` (unlinking a name whose
  inode `DiskCas` also holds frees nothing). `graveyard_pending_bytes` is their sum and is
  decremented when an entry finishes **or fails**; a failure is counted
  (`graveyard_failures`), logged once per entry, and its remnant is removed by the next start's
  stale-work cleanup (failing closed on macOS as today).
- Disk admission (`free(statvfs) − Σ admitted reservations ≥ reservation`) gets no credit: a
  backlog yields `Decline(DiskShortage)` → requeue without retry (existing semantic).
- The disk pressure monitor evaluates `free + min(graveyard_pending_bytes,
  10% × disk_per_instance)`, except that raw `statvfs` free below `2%` of the volume counts as
  pressure with no credit (emergency floor). After a pressure kill, the quiescence rule waits for
  the victim's graveyard entry to be fully processed before re-reading free space.
- `stop`: decline new assignments, kill every running attempt's group, rename every attempt
  directory into the graveyard, **drain the graveyard**, then return `Ok(())` (the next worker
  gets an empty `work_directory`, per the SPI). Single-use: return after `Completed` is sent and
  the graveyard is drained. Worker start empties `<work>` including `.graveyard`.
- No cleanup wait: a retry of the same operation is a new `AttemptId` and a new directory.

---

## 7. Storage

### 7.1 `DiskCas` (fast NVMe store)

- Layout: `<cas>/<hh>/<hex>-<size>` (256 shard directories) and `<cas>/<hh>/<hex>-<size>.x` for
  the executable variant (separate inode, mode 0555; plain 0444), so a hardlinked input never
  shares an inode across modes. Temporaries in `<cas>/tmp/`.
- Insert: Linux `O_TMPFILE` in the shard directory, streaming write + SHA-256 verify, `futimens`
  both times to `2000-01-01T00:00:00Z` (stable input mtimes for actions that read them), then
  `linkat` (never replaces; `EEXIST` = already present, success); fallback exclusive temp name +
  `renameat`. macOS: exclusive temp + normalize + `renameat`. No fsync (a cache; verify-on-insert
  plus a startup size check detect corruption). `Digest::EMPTY` is never stored.
- Index: `HashMap<(Digest, Mode), SlabKey>` + intrusive doubly linked LRU over a `Slab` (O(1)
  touch/insert/evict), sharded 16 ways with `parking_lot::Mutex` held only for O(1) work, never
  across `.await` or I/O. Global used bytes in an `AtomicU64`; eviction takes the LRU tail of the
  shard with the oldest tail stamp.
- Pins: an entry with `attempt_pins > 0` or `tree_pins > 0` is never evicted. Distinct bytes with
  `tree_pins ≥ 1` (tracked on 0 ↔ 1 transitions) are capped at `tree_pin_percent` (50%) of
  `fast_cas_budget`, and `TreeCache` holds at most `tree_cache_max_trees` (256); materializing a
  tree beyond either cap first evicts unleased LRU trees (§7.3), and if that cannot make room the
  subtree is staged without binding. An insert **bypasses** `DiskCas` whenever evicting every
  unpinned entry still could not fit `blob.size` within `fast_cas_budget` (including
  `blob.size > fast_cas_budget`): the blob is written directly into the action directory
  (verified) and counted as a slow read.
- Startup: parallel shard scan rebuilds the LRU from `atime` (from `ctime` on `noatime` mounts,
  since `mtime` is normalized), removes `tmp/` and malformed names, reports `size_bytes`.
- Stats: exactly `WorkerCacheStats` (local/slow reads and bytes, evictions and evicted bytes
  excluding replacements, size, budget); `EngineStore` for controller-side engine-bounded stores.

### 7.2 `SingleFlight`

`Sharded<HashMap<Key, Arc<Flight>>>`; `Flight` holds a `tokio::sync::watch<FlightState>`. The
first caller spawns the work on the store's own task set, so a cancelled caller never aborts
others; followers await the watch. Completion removes the map entry before publishing. Follower
local reads count as local reads ("concurrent slow fetches deduplicate to one physical fetch,
while follower local reads count separately"). Used for blob fetches, `Directory` resolution and
tree materialization.

### 7.3 `TreeCache`

- Active only when the sandbox binds read-only and `input_trees = "bind"`.
- Key: `Directory` digest. Content: `<scratch>/trees/<hex>-<size>/`, files hardlinked from
  `DiskCas` (same inodes, `tree_pins` held), directories 0555 with normalized `mtime`, published
  by `renameat` of a fully built temporary tree. `<scratch>/trees` is a `WorkerLayout` path
  passed as `WorkerSpec::trees_directory: Option<PathBuf>` (H1), `Some` iff
  `sandbox.binds_read_only() && input_trees == Bind`, beside `fast_cas_directory`.
- Admission on first use (`admit_after = 1`) through `SingleFlight`; caps per §7.1.
- **Leases.** `get_or_materialize` returns an RAII `TreeLease` that increments the entry's
  `leases`; the attempt holds it until `Executed` (or drop), which covers the whole time the tree
  is bind-mounted. Eviction considers **only `leases == 0`** entries, in LRU order. If evicting
  every unleased entry cannot make room, no lease is returned and the caller stages the subtree
  as ordinary files.
- **Eviction.** An evicted tree is renamed to a unique `<work>/.graveyard/tree-<uuid>`, so evicting
  a re-materialized tree with the same digest never collides with an earlier remnant
  (`ENOTEMPTY`). Its graveyard entry has `reclaimable = 0` (`cas_shared_bytes == walk size`:
  every file is a `DiskCas` inode), and its `DiskCas` `tree_pins` are released after the unlink
  completes.
- **Startup.** `WorkerLayout::prepare` empties `<scratch>/trees` before measuring capacity and
  deriving `fast_cas_budget`. `TreeCache::open` moves any remaining entry into `.graveyard` as
  well, so no tree from an earlier process is ever bound and no stale `tree_pins` exist.

### 7.4 Controller stores and S3

- `S3Cache` (`gang-aws`) implements both store ports with an `aws_sdk_s3::Client` built through
  `gang-aws-sdk`'s config loader and HTTP client (never `aws_config::load_defaults`), keyed exactly
  as today (`key_prefix`, CAS/AC prefixes, AC namespace `[actions] environment`), proven by a
  key-layout parity test against the current NativeLink keys so buckets stay warm.
- `consider_expired_after` has identical meaning on `ContentStore::{read, find_missing}` and
  `ActionStore::get`: an object older than it is absent, so clients re-upload and lifecycle
  rules may delete it.
- Reads of a whole blob verify SHA-256 while streaming; a mismatch ends the stream with
  `PortError` (data loss), logged with the key. Ranged reads are not verifiable and are not
  verified. `DiskCas` inserts verify always.
- S3 tuning lives in `[cache.s3.tuning]` (`S3Tuning`, `crates/gang-aws/src/config.rs`), which
  owns `last_modified` and `consider_expired_after` handling:
  - Writes: blobs ≤ `single_put_max` are buffered, verified, then `PutObject`; larger blobs use
    multipart upload with on-the-fly SHA-256 and `AbortMultipartUpload` on mismatch, so nothing
    unverified is ever visible. `single_put_max` is validated `≥ 5 MiB`, S3's minimum size for a
    non-final multipart part.
  - `find_missing`: bounded-concurrency `HeadObject`s (`head_concurrency`) behind a positive
    existence cache inside `S3Cache` (`existence_cache_entries`; an entry expires at
    `min(last_modified + consider_expired_after, now + existence_ttl)`).
- Controller-side transport limits stay in `[server]`: in-flight blob bytes are bounded by
  `max_inflight_blob_bytes` (a byte semaphore), and ByteStream reads stream `read_chunk` frames
  and never buffer whole blobs.

### 7.5 Compression

ZSTD and IDENTITY for ByteStream (`compressed-blobs/zstd/<hash>/<size>`) and batch RPCs, through
the `zstd` crate (already in `Cargo.lock`). Storage is uncompressed; compression is transport
only. **Decoder bound:** the streaming decoder's output is capped at `digest.size + 1` bytes
(`saturating_add`); reaching the extra byte fails at once with `Rejection::DigestMismatch`,
before hashing completes, so a compression bomb cannot exceed the declared size. Capabilities
advertise ZSTD + IDENTITY; `gang bazelrc` does not render `--remote_cache_compression`.

---

## 8. Protocols

### 8.1 REAPI v2 subset (public listener; the worker listener serves CAS, ByteStream and AC)

| Service | Methods | Notes |
|---|---|---|
| `Capabilities` | `GetCapabilities` | API 2.0–2.3; SHA256 only; `max_batch_total_size_bytes` = `max_message_size`; ZSTD + IDENTITY; `exec_enabled`; priority range `[i32::MIN, i32::MAX]` (higher first); `symlink_absolute_path_strategy = ALLOWED` |
| `ContentAddressableStorage` | `FindMissingBlobs`, `BatchReadBlobs`, `BatchUpdateBlobs`, `GetTree` | `GetTree` paginated; `SplitBlob`/`SpliceBlob` UNIMPLEMENTED |
| `google.bytestream.ByteStream` | `Read`, `Write`, `QueryWriteStatus` | resumable writes keyed by `uploads/<uuid>/blobs/...` |
| `ActionCache` | `GetActionResult`, `UpdateActionResult` | client updates allowed (as today, `read_only: false`); no completeness checking |
| `Execution` | `Execute`, `WaitExecution` | the stream sends the `Operation` on change and every `keepalive_interval` |
| `google.longrunning.Operations` | `GetOperation`, `CancelOperation` | `ListOperations`, `DeleteOperation`, `WaitOperation` UNIMPLEMENTED |

`RequestMetadata` is decoded once by the moved `observer.rs` and passed typed to `execute`; the
gate's `Admitted` principal is the operation's source. `gang-e2e` moves from `nativelink-proto`
to `gang-reapi`.

### 8.2 `leadingterm.gang.worker.v1`

```proto
service WorkerService {
  // One session per worker generation. The first WorkerMessage is `register`.
  rpc Connect(stream WorkerMessage) returns (stream ControllerMessage);
}

message WorkerMessage {
  oneof kind {
    Register register = 1;
    Accept accept = 2;
    Decline decline = 3;
    Executed executed = 4;     // processes gone: early CPU/memory release, usage, kill
    Completed completed = 5;   // outputs uploaded, AC written: disk release, publication
    KeepAlive keep_alive = 6;  // cache stats + capacity fact
    GoingAway going_away = 7;  // stop requested; no new assignments
  }
}
message Register {
  string instance = 1; string fleet = 2; string gang_version = 3;
  map<string, string> properties = 4;     // FleetProperties::worker_properties()
  Resources capacity = 5;                 // registered ledger capacity (post-reserve memory)
  Resources ceilings = 6;                 // effective per-action ceilings
  uint32 max_inflight = 7;                // 0 = unlimited
  bool single_use = 8;
  ActionEnvironment environment = 9;
}
message Accept    { string attempt = 1; }
message Decline   { string attempt = 1; DeclineReason reason = 2; uint64 free_bytes = 3; }
message Executed  { string attempt = 1; ResourceUsage usage = 2; optional Kill kill = 3;
                    google.rpc.Status status = 4; int32 exit_code = 5;
                    google.protobuf.Timestamp started = 6; google.protobuf.Timestamp finished = 7; }
message Completed { string attempt = 1; oneof outcome {
                    build.bazel.remote.execution.v2.ActionResult result = 2;
                    google.rpc.Status status = 3; } }
message KeepAlive { CacheStats cache = 1; optional uint64 memory_capacity = 2; }

message ControllerMessage {
  oneof kind {
    Registered registered = 1;   // worker id, keepalive interval
    Assign assign = 2;
    Kill kill = 3;
    SetDraining set_draining = 4;
    Rehome rehome = 5;           // a draining old controller ends this session
  }
}
message Assign {
  string attempt = 1; string operation = 2;
  build.bazel.remote.execution.v2.Digest action_digest = 3;
  optional bytes action = 4;   // inlined only when action.len() + command.len() <= assign_inline_max
  optional bytes command = 5;  // otherwise both absent: the worker reads them from the CAS
  map<string, string> platform = 6;          // authoritative; replaces client properties
  Resources reservation = 7;
  google.protobuf.Timestamp attempt_deadline = 8;
  google.protobuf.Duration timeout = 9;
  string target = 10; string mnemonic = 11;
  bool skip_cache_lookup = 12; bool do_not_cache = 13;
  uint32 cpu_weight = 14;      // 1..=10000; never part of platform or environment
}
message Kill { string attempt = 1; KillReason reason = 2; }
```

`action` and `command` are both present or both absent. `assign_inline_max` (64 KiB) must be
below `max_message_size` minus the fixed `Assign` overhead, checked at config parse, so a
`Connect` frame never exceeds the message limit. Ordering: per-session FIFO both ways; `Executed`
precedes `Completed` for an attempt; a message for an unknown attempt is ignored and counted.
Liveness: HTTP/2 PING (`[worker] keepalive_interval`) plus `KeepAlive` (`connect_timeout / 2`);
the controller declares the worker lost after `[server] worker_timeout` without a `WorkerMessage`.

---

## 9. Configuration

Every existing key keeps its name, type and default, except `[worker] cleanup_wait`. New typed
keys (owned by `gang-config` unless the table names the `S3Tuning` plugin section; documented
defaults; unknown keys rejected):

| Key | Default | Owner |
|---|---|---|
| `[server] scheduler_queue_depth` | 4096 | each actor inbox capacity |
| `[server] scheduler_batch` | 64 | worker/client messages per actor iteration (§5.10) |
| `[server] worker_outbound_depth` | 256 | per-worker outbound channel |
| `[server] dispatch_ack_timeout` | `10s` | §5.5 |
| `[server] dispatch_scan_limit` | 256 | §5.4 |
| `[server] assign_inline_max` | `64KiB` | §8.2 |
| `[server] max_inflight_blob_bytes` / `read_chunk` | `1GiB` / `64KiB` | §7.4 (`ServerConfig`) |
| `[cache.s3.tuning] head_concurrency` / `single_put_max` | 128 / `5MiB` (validated `≥ 5MiB`) | §7.4 (`S3Tuning`, `gang-aws`) |
| `[cache.s3.tuning] existence_cache_entries` / `existence_ttl` | 1048576 / `1h` | §7.4 (`S3Tuning`, `gang-aws`) |
| `[worker] download_concurrency` / `batch_blob_max` | 64 / `1MiB` | §6.3 |
| `[worker] hash_concurrency` | `min(vCPUs, 4)` | §6.5, §3.4 |
| `[worker] compress_min_size` | `4KiB` | §6.5 |
| `[worker] directory_cache_budget` | `64MiB` | §6.3 |
| `[worker] tree_cache_min_files` / `tree_cache_max_trees` / `tree_pin_percent` | 512 / 256 / 50 | §6.3, §7.1 |
| `[worker] graveyard_workers` | 2 | §6.5 |
| `[execution] input_trees` | `bind` | `bind` (effective only when the sandbox binds read-only) or `link` (no tree cache) |

The graveyard pressure credit (10% of `disk_per_instance`), its 2% emergency floor and
`MAX_RO_BINDS = 64` are safety invariants, not tunables. `[worker] cleanup_wait` is removed in
H1: its only consumer is NativeLink's `max_cleanup_wait` (`gang-nativelink/src/worker.rs:162`);
the configuration rejects it as an unknown key and `CHANGELOG.md` announces the break.

---

## 10. Verification

### 10.1 Unit and property tests (per crate, Bazel `rust_test`)

- `gang-scheduler`: every §5.5 row; queue order against a reference sort of
  `(Reverse(priority), seq)`; dedup promotion keeps `seq` and changes weight; QoS mapping
  boundaries (99/100/199/200, `i32::MIN`, `i32::MAX`); ledger conservation; distributed placement
  and its tie-breaks; preference; escalation table from SPEC §3.2; cordons across overlapping
  generations; `retain_drains` grace and reappearance; hand_off drain set; `WorkerId` minting
  round-trip; codec golden fixtures; projection truncation; zero allocation in steady state
  (`allocation-counter`).
- `gang-cas`: concurrent inserts of one digest; mismatch leaves nothing; `EMLINK` fallback;
  per-mode inodes; normalized `mtime`; LRU O(1) and exact accounting under churn; pins never
  evicted; tree pin cap at 0 ↔ 1 transitions; bypass when evicting every unpinned entry cannot fit
  the blob (and when `blob.size > fast_cas_budget`); startup rebuild from `atime`/`ctime`;
  single-flight with the leader caller dropped; tree exclusion rules (both output-prefix
  directions, one-way `cwd.starts_with(tree)` with `cwd = ""` still binding `external/<x>`,
  symlink ancestor, 64-bind cap); leased trees never evicted and no-room fallback to unbound
  staging; repeated evict/re-materialize of one digest yields distinct `tree-<uuid>` graveyard
  entries; startup empties `<scratch>/trees`; zstd bomb capped at `size + 1`.
- `gang-exec`: a bind target with a symlinked intermediate component is refused (proves the
  component-by-component `O_NOFOLLOW | O_DIRECTORY` walk).
- `gang-engine` worker: the action sees `PWD`/`TMPDIR` defaults, a client override of either, no
  worker environment variable, and reserved `GANG_*` values that the client cannot override.
- `gang-macros`: the `__gang_delegate_ContentStore!`/`__gang_delegate_ActionStore!` delegation
  test over a two-variant enum (WP 0.2); the full `compose!` test with two cache plugins (WP H1).
- `gang-cgroup`: `Cgroupfs`/`TestTree` assert sibling weights after `SUBTREE_CONTROL` and
  per-scope weight from `ActionLimits`.
- Store contract suite (empty digest, `ByteRange` edges, verify, expiry) run against `DiskCas`,
  `S3Cache` (the e2e fake S3), `RemoteCas` and the `gang-testkit` fake.

### 10.2 Deterministic simulation (`A12`)

A seeded simulator drives the pure `Scheduler` with random interleavings of executes
(including duplicate digests with rising priorities), registrations across overlapping
generations, accepts/declines, `Executed`/`Completed`, kills, disconnects, cordons, hand_off,
clock jumps (including backward wall clock) and restores, and checks after every step:
terminal exactly once; ledger = Σ live reservations; no assignment to an ineligible worker;
queue order; promotions never lower priority; retry budgets bounded; `next_wakeup` never in the
past after `advance`; snapshot invariants. Failing seeds are committed as regressions.

### 10.3 Ported regressions

From the fork and `gang-nativelink`: `absolute_internal_output_symlinks_are_rebased_from_their_parent`,
`…through_a_symlinked_work_root`, `a_memory_kill_survives_an_output_upload_error`,
`an_output_upload_error_is_not_a_successful_action`, cleanup-order/publication cases
(`aee3d6d8`, `7e2b3348`), deadline cases (`0e75592a`, `c74086b3`, `14cb9658`), disk-kill
escalation, pressure enforcement, capacity facts (`11314249`), CPU percentile (`5fe6df70`), every
test in `handoff_tests.rs`, `cordons.rs`, `placeable.rs`, `fitted.rs`, `durations.rs`,
`source_rates.rs`, and the observer/controller observation tests. New regressions: retry with the
same `operation_id` never waits on cleanup; post-upload gap; graveyard backlog causes no pressure
kill and failed unlinks still decrement; `stop` returns with an empty work directory;
higher-priority-first order; post-drain learned flush after `run_controller` returns.

### 10.4 End-to-end

`gang-e2e` Gate 1 and Gate 2 scenarios unchanged, passing on the engine (Local Mode on Linux with
`bwrap` + `cgroup`, and macOS `noop`), plus cluster handoff scenarios with fake DynamoDB/PostgreSQL
and fake S3. Gate 1 keeps the input-poisoning assertion and adds one for a bound tree (`EROFS`).

### 10.5 A/B benchmark (`G3`, gate for deleting NativeLink)

Same host class, scratch volume and bucket, cold and warm runs, `v0.3.9` (`f0ec0a55`) vs. the
candidate, provenance and cache state recorded for both sides:

- **Canary:** 24 concurrent actions × 20,000 input files (a shared toolchain subtree ≥ 15k files
  plus per-action sources); full `gang-e2e` timing.
- **QoS contention canary:** at 3× CPU oversubscription, gate (`200`) actions' concurrent CPU
  demand is capped at ≤ 1× physical vCPUs, while interactive (`100`) and background (`0`) actions
  supply the remaining 2× oversubscription; pass when (a) single-core gate-tier p95 wall time
  ≤ 1.25× its uncontended baseline and background still completes (no starvation), and (b) a
  12-thread gate action declaring `cpu_millicores = 12000` (without `cpu_count`) under 3×
  contention receives ≥ 11 cores of `cpu.stat` `usage_usec` per wall second while contended.
- **ActionCache invariance:** the same build on both binaries; candidate AC hit rate ≥ 99% of the
  v0.3.9-populated cache.
- **Metrics:** stage-in p50/p95; Execute→Accept p99; post-upload gap p99; end-to-end action latency
  p50/p95; controller CPU and RSS peak; worker RSS peak; S3 GET/HEAD/PUT counts; slow-read bytes;
  local hit ratio; `Cargo.lock` package count (685 today).
- **Pass:** no metric regresses beyond noise (3 runs, max-of-min); warm stage-in p95 ≥ 5× faster
  with `bind`; post-upload gap p99 ≤ 5 ms; dispatch p99 ≤ 2 ms at idle capacity; warm
  `FindMissingBlobs` S3 HEADs ≥ 10× fewer; controller and worker RSS ≤ v0.3.9.

---

## 11. Work breakdown

47 work packages: **42 `[Hydrogen]`, 5 `[Neon]`** (`A12`, `D2`, `D5`, `E3`, `G2`). A Hydrogen
package is implementable from this document without design decisions; the sections cited per
package are its spec. Each package has a disjoint file lease and a done-check (`bazel test` of
the named targets; `./tools/lint.sh` once per milestone).

### Phase 0 — contracts and scaffolding (one owner, additive)

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| 0.1 `Digest` (+`EMPTY`), `AttemptId`, `OperationName`, `CpuWeight` | Hydrogen | `gang-types/src/digest.rs`, `attempt.rs`, `cpu_weight.rs` | §3.1, §3.2 | — |
| 0.4 `worker.v1` proto (before 0.3 generates) | Hydrogen | `proto/leadingterm/gang/worker/v1/worker.proto` | §8.2 | 0.1 |
| 0.2 `ContentStore`/`ActionStore` `#[port]` traits (`: std::fmt::Debug`), `BlobStream`, `ByteRange`, `Rejection::{DigestMismatch, OutOfRange}`, `gang-spi/src/{lib.rs,private.rs}` re-exports (`::bytes`, `gang_types::Digest`), `#[port]` two-argument delegation arm + isolated-module `__gang_delegate_*!` test over a two-variant enum, store contract suite skeleton (empty digest, ranges). Strictly additive: no `domain.rs` or `CacheStorePort` change | Hydrogen | `gang-spi/src/{blob_store.rs,errors.rs,lib.rs,private.rs}`; `gang-macros/src/port.rs`; `gang-testkit/src/stores.rs` | §3.1 | 0.1 |
| 0.3 `gang-reapi` + `gang-protogen` (tooling), vendored protos, drift test, `NOTICE`, PLAN rows | Hydrogen | `third_party/remote-apis/`, `crates/gang-reapi/`, `crates/gang-protogen/`, `docs/PLAN.md` | §4 | 0.4 |
| 0.5 `gang-config` keys of §9 incl. `hash_concurrency = min(vCPUs, 4)`, `assign_inline_max` check (the `[cache.s3.tuning]` keys belong to C1) | Hydrogen | `gang-config/src/{server_config,worker_config,execution_config}.rs` | §9 | — |
| 0.6 skeletons `gang-scheduler`, `gang-cas`, `gang-engine` + `docs/PLAN.md` crate-table rows for all three; `aws-sdk-s3` in root and `gang-aws` `Cargo.toml`; one lock repin; gazelle | Hydrogen | new manifests, root `Cargo.toml`, `docs/PLAN.md`, `BUILD.bazel` source set, locks | §4 | — |

### Phase 1 — independent tracks

**A. `gang-scheduler`** (lease `crates/gang-scheduler/src/*`)

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| A1 operations, dedup + promotion, queue indices, QoS mapping, watchers, client-less timeout | Hydrogen | `operations.rs`, `queue.rs`, `qos.rs` | §3.2, §5.2, §5.5 | 0.1 |
| A2 ledger, instances/generations, eligibility, distributed placement, worker-driven scan, `WorkerId` minting | Hydrogen | `ledger.rs`, `placement.rs`, `workers.rs` | §5.2, §5.4 | A1 |
| A3 `ResourceSketch` port + v1 codec + golden fixtures | Hydrogen | `sketch.rs`, `history.rs` | §5.6 | 0.1 |
| A4 move durations and rates | Hydrogen | `durations.rs`, `rates.rs` | §2 | — |
| A5 admission, fitting, capacity facts | Hydrogen | `admission.rs`, `fitting.rs`, `capacity.rs` | §5.3 | A1, A3 |
| A6 escalation + kill floors | Hydrogen | `escalation.rs` | §5.7 | A5 |
| A7 cordons, `retain_drains` (`Census`), stale-build replacement, hand_off drain set | Hydrogen | `cordons.rs`, `handoff.rs` | §5.2, §6.1 | A2 |
| A8 attempts: ack, deadlines, worker loss, retries, `Executed`/`Completed` | Hydrogen | `attempts.rs`, `timers.rs` | §5.5 | A2, A6 |
| A9 projections | Hydrogen | `snapshot.rs`, `listing.rs` | §5.9 | A1, A2, A4, A7, A8 |
| A10 worker pressure policy incl. graveyard credit | Hydrogen | `pressure.rs` | §5.8, §6.5 | — |
| A11 `Scheduler` facade, `Effects`, `Learned` | Hydrogen | `scheduler.rs`, `effects.rs`, `learned.rs` | §5.1 | A1–A9 |
| A12 deterministic simulator | **Neon** | `tests/simulation.rs` | §10.2 | A11 |

**B. `gang-cas`** (lease `crates/gang-cas/src/*`)

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| B2 O(1) sharded LRU + exact stats | Hydrogen | `lru.rs`, `stats.rs` | §7.1 | — |
| B1 `DiskCas`: layout, atomic insert, verify, per-mode inodes, `mtime` normalization, pins + caps, bypass, startup scan | Hydrogen | `disk.rs`, `layout.rs`, `pins.rs` | §7.1 | 0.1, 0.2, B2 |
| B3 `SingleFlight` | Hydrogen | `flight.rs` | §7.2 | — |
| B4 zstd codec with `size + 1` cap; ByteStream resource names | Hydrogen | `codec.rs`, `resource.rs` | §7.5 | 0.3 |
| B5 `Tiered` fast/slow read-through/write-through | Hydrogen | `tiered.rs` | §6.3, §7.1 | B1, B3 |
| B6 `RemoteCas` (ViaController) | Hydrogen | `remote.rs` | §3.1, §8.1 | 0.2, 0.3, B4 |
| B7 `TreeCache` (bind-only, single-flight, caps, `TreeLease`, unleased-only eviction to `tree-<uuid>`, startup emptying; no manifest, no clone path) | Hydrogen | `trees.rs` | §7.3 | B1, B3 |

**C. cache plugins**

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| C1 `S3Cache` stores via `gang-aws-sdk`: multipart verify, full-read verify, expiry on CAS and AC, existence cache, `[cache.s3.tuning]` keys (`head_concurrency`, `single_put_max ≥ 5MiB`, `existence_cache_entries`, `existence_ttl`), key-layout parity test | Hydrogen | `gang-aws/src/{s3_store.rs,s3_existence.rs,config.rs}` | §7.4, §9 | 0.2, 0.6 |
| C2 `FilesystemCache` over `DiskCas` + disk `ActionStore` | Hydrogen | `gang-local/src/filesystem_cache.rs` | §4, §7.1 | B1 |

**D. `gang-engine` controller** (lease `crates/gang-engine/src/{controller*,reapi_*}` + moved transport)

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| D1 move `serve`, `gate`, `standing`, `refusals`, `observer`, `handoff`, `acquisition` (scripted move + import rewrite) | Hydrogen | those files | §2 | 0.6 |
| D2 actor shell: three biased inboxes, timers, effect executor, outbound channels, `LearnedSlot` | **Neon** | `controller.rs`, `actor.rs` | §5.10, §6.1 | A11, D1 |
| D3 CAS, ByteStream, AC, Capabilities services; in-flight byte budget (`max_inflight_blob_bytes`, `read_chunk`) | Hydrogen | `reapi_cas.rs`, `reapi_bytestream.rs`, `reapi_ac.rs`, `reapi_capabilities.rs` | §7.4, §8.1 | 0.2, 0.3, 0.5, B4, D1 |
| D4 Execution, WaitExecution, Operations; per-op watch streams; gate-tier audit log | Hydrogen | `reapi_execution.rs` | §8.1, §3.2 | D2 |
| D5 `worker.v1` server session | **Neon** | `worker_service.rs` | §8.2, §6.1 | D2, 0.4 |
| D6 inherent `Engine` controller + worker entrypoints (`endpoints`, control methods, `LearnedSlot` persistence §6.1 — no `SchedulerPort` impl yet) | Hydrogen | `engine.rs`, `learned_slot.rs` | §6.1 | D2, D3, D4, D5, E5 |

**E. `gang-engine` worker** (lease `crates/gang-engine/src/worker_*`)

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| E1 session client: register, backoff, keepalive, accept/decline, drain, `Rehome`, revoked stop | Hydrogen | `worker_session.rs` | §6.2, §8.2 | 0.4 |
| E2 staging pipeline incl. read-only tree selection | Hydrogen | `worker_staging.rs` | §6.3 | B5, B7 |
| E3 runner: env, side channel (`read_only_trees`, `cpu_weight`), spawn, sampling, timeouts, kills, reaper | **Neon** | `worker_runner.rs`, `worker_sampling.rs` | §6.4 | A10 |
| E4 upload: outputs, symlink rebase, `Tree`, parallel hash, `DiskCas` insert, AC put | Hydrogen | `worker_upload.rs` | §6.5 | B5 |
| E5 graveyard, unlink pool, disk credit accounting, `stop`/single-use drain | Hydrogen | `worker_graveyard.rs`, `worker.rs` | §6.5 | B6, E1, E2, E3, E4 |

**F. sandbox and limits**

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| F1 `ActionExecutionSpec::read_only_trees`, `ActionIsolationPort::binds_read_only`, `ActionLimits::cpu_weight` + all constructors | Hydrogen | `gang-spi/src/{isolation_port.rs,limits_port.rs}` + callers | §3.3 | 0.1 |
| F2 `gang-bwrap` `--ro-bind` (≤ 64); `gang-noop` `false`; `gang-exec` reads `read_only_trees` + `cpu_weight` from the side channel, component-by-component `O_NOFOLLOW \| O_DIRECTORY` walk, `.graveyard` reserved | Hydrogen | `gang-bwrap/src/command_line.rs`, `gang-noop`, `gang-exec/src/{side_channel.rs,action_directory.rs}` | §3.3 | F1 |
| F3 `gang-cgroup` sibling weights after `SUBTREE_CONTROL`, per-scope `cpu.weight` | Hydrogen | `gang-cgroup/src/*` | §3.4 | F1 |

**G. testkit and e2e preparation**

| WP | Tag | Files | Spec | Depends |
|---|---|---|---|---|
| G1 `gang-e2e` REAPI types on `gang-reapi`; testkit store fakes wired to the contract suite | Hydrogen | `gang-testkit/src/stores.rs`, `gang-e2e/src/*` | §3.1, §10.1 | 0.2, 0.3 |

Through Phase 1, D1–D6 and E1–E5 expose inherent `Engine` entrypoints (no `SchedulerPort` impl
yet), so `bazel test //crates/gang-engine/...` compiles and tests beside `gang-nativelink` while
`RenderedStoreSpec` still exists.

### Phase 2 — switch the candidate to the engine and prove end-to-end parity

| WP | Tag | Work | Depends |
|---|---|---|---|
| H1 | Hydrogen | One atomic contract switch: remove `RenderedStoreSpec`; `CacheStorePort` `type Content`/`type Actions` + `DOMAINS` registration in `gang-macros/src/domain.rs` (plus `domain.rs` unit tests) + full two-plugin `compose!` test; `ControllerStores`/`WorkerStores`; `impl SchedulerPort for Engine`; `WorkerSpec::trees_directory: Option<PathBuf>` from `WorkerLayout` (`Some` iff `sandbox.binds_read_only() && input_trees == Bind`); `gang-worker` branches on `cache.worker_access()` and calls `content_store()`/`action_store()` only for `StoreAccess::Direct`; update `FakeScheduler`, `FakeCache`, and `gang-spi/tests/*`; wire `gang-server`/`gang-worker`/`gang-main` to `Engine`; `retain_drains` doc wording; `gang bazelrc`/`gang setup` render `--remote_execution_priority=100`; remove `[worker] cleanup_wait`; remove `gang-nativelink` from `[workspace].members` and `docs/PLAN.md`, add `# gazelle:exclude crates/gang-nativelink` in root `BUILD.bazel`, and delete `crates/gang-nativelink/BUILD.bazel` (directory stays until H2; done-check tests all non-`gang-e2e` targets) | D6, C1, C2, F2, F3, G1 |
| G2 | **Neon** | Migrate `gang-e2e/Cargo.toml`, `embedded.rs`, and `history_contract.rs` off `gang-nativelink`/`RenderedStoreSpec`; port §10.3 regressions; live `gang-e2e` green on Linux + macOS; cluster handoff scenarios; Phase 2 milestone gate `./tools/lint.sh` + `bazel test //...` | G1, H1 |

### Phase 3 — performance and QoS evidence on the switched candidate

| WP | Tag | Work | Depends |
|---|---|---|---|
| G3 | Hydrogen | A/B against a `v0.3.9` (`f0ec0a55`) baseline build: `bind` warm stage-in ≥ 5×, QoS contention canary, AC invariance (§10.5); report with provenance | G2 |

### Phase 4 — deletion (after G2, G3 and the milestone review)

| WP | Tag | Work | Depends |
|---|---|---|---|
| H2 | Hydrogen | Delete `crates/gang-nativelink`, remove `# gazelle:exclude crates/gang-nativelink` from root `BUILD.bazel`, delete the 9 `nativelink-*` dependencies (root, `gang-aws` and `gang-e2e` dev-deps); repin `Cargo.lock`, `Cargo.Bazel.lock`, `MODULE.bazel.lock` through Bazel; drop NativeLink's toolchain-pin rationale from PLAN | G2, G3 |
| H3 | Hydrogen | SPEC §3.2 rewritten around the engine (fork commit list deleted), §3.7/§3.8 wording, PLAN crate table and pillars, README, CHANGELOG (config break, priority default, ZSTD capability, cpu weights, read-only trees) | H2 |

### DAG

```
0.1 ─┬─ 0.4 ─ 0.3 ─┬─ B4 ─┬─ B6
     │             │      └─ D3 (also 0.2, 0.5, D1)
     │             └─ G1 (also 0.2)
     ├─ 0.2 ─┬─ B1 (also B2) ─┬─ B5 (also B3) ─┬─ E2 (also B7)
     │       │                ├─ B7 (also B3)  └─ E4
     │       │                └─ C2
     │       └─ C1 (also 0.6)
     ├─ A1 ─┬─ A2 ─┬─ A7 ─────────────────┐
     │      │      └─ A8 (also A6) ─ A9 (also A1, A4, A7) ─ A11 ─┬─ A12
     │      └─ A5 (also A3) ─ A6                                └─ D2 (also D1) ─┬─ D4
     ├─ A3                                                                       └─ D5 (also 0.4)
     └─ F1 ─┬─ F2
            └─ F3
A10 ─ E3 ;  0.4 ─ E1 ;  B6, E1, E2, E3, E4 ─ E5
D2, D3, D4, D5, E5 ─ D6 ─┐
D6, C1, C2, F2, F3, G1 ──┴─ H1 ─┐
G1 ─────────────────────────────┴─ G2 ─ G3 ─ H2 ─ H3
0.5, 0.6, D1 → as listed
```

Critical path: 0.1 → A1 → A2 → A8 → A9 → A11 → D2 → D5 → D6 → H1 → G2 → G3 → H2 → H3.

---

## 12. Decisions (binding; formerly open questions)

- **Q1 Priority.** Higher runs first; queue key `(Reverse<i32>, seq)`; negative values accepted
  and advertised (`[i32::MIN, i32::MAX]`); dedup promotes. `CpuWeight` tiers `≥200 → 1000`,
  `100..=199 → 100`, `<100 → 20`, carried out of band (never platform or environment).
  `gang bazelrc`/`gang setup` render `--remote_execution_priority=100`. Every authenticated caller
  may use `≥ 200` initially, audited by `(provider, principal, priority)`. G3 runs the 3×
  contention canary.
- **Q2 Placement.** Distributed: lowest reserved CPU fraction in the highest-preference fleet,
  then fewest gate-tier attempts, then LRU, then `WorkerId`. No `[server] placement` knob.
- **Q3 Operations.** `GetOperation` and `CancelOperation` only; `List`/`Delete`/`Wait`
  UNIMPLEMENTED.
- **Q4 Compression.** ZSTD + IDENTITY advertised; `gang bazelrc` does not render
  `--remote_cache_compression` yet.
- **Q5 Read-only trees.** `input_trees = "bind"` by default, effective only when the sandbox
  binds read-only; `admit_after = 1`; `link` is the escape hatch.
- **Q6 Handoff.** Single session per worker with `Rehome` (ADR-59 parity); dual-session workers
  deferred.
- **Q7 ActionCache.** No completeness checking; `consider_expired_after` applies identically to
  `ActionStore::get` and `ContentStore::{read, find_missing}`.
- **`[worker] cleanup_wait`.** Removed in H1; rejected as an unknown key; announced in
  `CHANGELOG.md`.
