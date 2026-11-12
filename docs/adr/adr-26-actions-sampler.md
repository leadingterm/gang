# ADR-26: Running actions are attributed by a background sampler

Status: Accepted

## Context
- NativeLink v1.8.3 gives no worker-to-action attribution: `WorkerSummary` has only `running_actions`. Digest, start time and the Bazel target are reachable through `ClientStateManager::filter_operations`, where a `worker_id` filter is one pass over the executing set per worker.

## Decision
- **`SchedulerPort::actions(limits, into)`** fills a bounded view of executing and queued actions from `ClientStateManager`: one executing listing, one listing per worker that runs actions (which claims each operation for its worker), and the head of the priority-ordered queue.
- **One `ActionsRefresher` task** in `gang-server` runs beside the control loop from controller start to shutdown, every `detail_interval` (default 10s), independent of viewers. It fills a private buffer and swaps it into the cache under a short lock. The publisher reads the cache each pass; viewers and requests never trigger a listing.
- **A failed refresh** keeps the previous sample, so staleness is visible. Frames carry the sample time and a server-owned `stale_after` threshold (3 × `detail_interval` unless configured); `actions` is null until the first sample.

## Consequences
- Cost is O(busy workers × executing) per refresh, paid whether or not anyone watches, off the request and control paths; a deterministic call-count test guards it.
- The live running count and the cached list can differ by up to one interval.
- A future upstream change exposing each worker's running actions would make it O(executing).

## Rejected alternatives
- **Per-request or control-loop-coupled listings:** cost scales with viewers or slows the pass.
- **Viewer-gated start and stop:** adds lifecycle and first-viewer latency.
- **A NativeLink fork.**
