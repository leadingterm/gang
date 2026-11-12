# ADR-14: Fleets are named bundles of properties; declaration order is preference

Status: Accepted. Replaces placement keyed by ISA.

## Context
- `[fleets.<isa>]` allowed one fleet per ISA and could not express spot versus on-demand or sandboxes.
- NativeLink v1.8.3 checks only the keys an action sets: an exact key the worker lacks fails; an undeclared key is `Unknown` (a worker that declares it must equal it, one that does not is unrestricted). Every selection key must therefore be declared exact.

## Decision
- **A fleet** is a named worker pool with an `isa`, one `sandbox` (ADR-16), an instance size, scaling limits and provisioner options (ADR-15). `[[fleets]]` is an array.
- **Declaration order is the preference order,** the only cost-preference concept; spot is preferred by being declared first. There is no `priority` field.
- **Keys an action may set** are `fleet` (pin), `ISA` and `sandbox` (exact), `cpu_count`, `memory_kb` and `disk_kb` (minimum), `network` (ignored), and any provisioner option key (exact). The vocabulary table is in SPEC §3.2.
- **One match function.** `FleetCatalog::accepting` in `gang-types` is the only rule. The scheduler declaration iterates the same `declared()`, and a `gang-nativelink` test asserts agreement with NativeLink's `is_satisfied_by`.
- **Absent keys mean any accepting fleet,** preferring the earlier; there are no per-property defaults. `default_isa` is client-only and the controller never reads it.
- **Attribution** (`gang-reconciler`): a queued action goes to the first candidate whose uncapped need fits `max_instances`, else to the last candidate; none means unplaceable and counts nowhere. A full spot fleet overflows to the next declared fleet.

## Consequences
- One owner per fact: the match rule, worker advertisement and option keys each have one place.
- Contradictory keys match no fleet, so the action waits until `client_action_timeout`.

## Rejected alternatives
- **Server-side per-property defaults:** `Add` overwrites, and absent-means-any plus ordered first-fit gives the same intent.
- **A `priority` field per fleet:** declaration order gives the same result with one concept fewer.
- **Copying fleet shape and ASG names into plugin sections:** two owners for one fact.
