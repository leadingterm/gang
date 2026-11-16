# ADR-41: A fleet's minimum follows a weekly schedule

Status: Accepted. Builds on ADR-11.

## Context
- A fleet scaled to zero pays a cold start on the first build of the day: an AWS worker is in service about 1 minute after launch, and the first action from zero runs after about 2.5 minutes.
- `min_instances` is a constant: keeping a worker during office hours costs it all night and all weekend.
- The reconciler owns desired capacity and derives it each pass from demand, the fleet's bounds and the injected clock (ADR-11). Warm pools exclude Spot and still boot and enroll (ADR-22).

## Decision
1. **Windows in the fleet's config.** `[[fleets.scheduled_minimum]]` entries hold `instances`, `days`, `start`, `end` (local `HH:MM`, `start < end`, `end` up to `24:00`) and an IANA `time_zone`. All are checked at parse; `min_instances < instances <= max_instances`. Absent means no window.
2. **The floor is a pure function of the pass's wall time:** the largest `instances` of the windows in force, never below `min_instances`. Overlaps are allowed. `InstanceBounds::min_at(wall)` is the only reader; the pass's needed instances are raised to it, so scale-out, drain resumption and idle drain follow without a rule of their own.
3. **A window is in force when the zone's local clock reads one of its days and a time in `[start, end)`.** A start inside a spring-forward gap begins after the gap; a repeated fall-back hour counts on both readings.
4. **Time zones come from the tz database bundled in the binary** (`jiff`, `tzdb-bundle-always`), pinned by the lockfile, never the host's zoneinfo.
5. **Audit and dashboard say why.** `ScaleOut` and `CancelDrain` carry cause `minimum` when the floor, not the work, sets the need. `Policy.min` in the live state is the floor in force at the pass.

## Consequences
- No state is kept: a restart or a wall-clock step is an ordinary pass.
- A worker is never drained below the window's floor inside it; at the window's end an idle worker drains on the next pass.
- A start should precede arrivals by the cold-start time; the schedule cannot anticipate demand.
- Updating zone rules means a new Gang release.

## Rejected alternatives
- **Auto Scaling scheduled actions or `MinSize`:** AWS-only, and a second owner of desired capacity that the reconciler would drain against.
- **Warm pools for this purpose:** no Spot, and a warm start still boots and enrolls (ADR-22).
- **Cron expressions; windows that cross midnight; rejecting overlaps:** each adds parsing or rules that two plain windows already express.
- **The host's tz database or a UTC default zone:** results would differ per image, or shift silently by an hour or two.
