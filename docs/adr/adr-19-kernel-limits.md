# ADR-19: A worker action pool provides the kernel memory backstop

Status: Accepted, revised for the worker-wide action memory pool

## Context
Reservations pack actions but do not predict every action's peak. A per-action kernel ceiling can kill a healthy transient spike, while unrestricted actions can exhaust the worker and its control processes.

## Decision

- The optional `ActionLimitsPort` prepares a worker-wide action pool beneath the delegated cgroup v2 subtree. Its `memory.max` is effective machine memory less the configured reserve, read back after kernel rounding; swap is bounded by `swap_max`.
- Worker control processes, `gang exec`, and the guardian remain in a sibling worker leaf outside the pool. Declared fleet memory must fit the pool before enrollment.
- Each action gets a fresh child scope with `memory.oom.group=1`, CPU and process limits, but no per-action `memory.max`. The pool has `memory.oom.group=0` so a kernel OOM selects an action rather than the whole pool.
- The sandbox joins its scope through `gang-join`; the wrapper stays outside it, records available kernel peak and OOM evidence on leave, and removes the scope. A guardian reaps abandoned scopes.
- Pressure enforcement is the normal userspace path; soft enforcement is optional. Without the limits plugin, including on macOS, pool metadata and exact kernel peaks are unavailable.

## Consequences

- Linux deployment must provide a delegated cgroup v2 subtree and reserve enough memory for worker control processes.
- The pool is a shared safety backstop, not an individual reservation ceiling. Kernel OOM and resource-pressure kills can leave the wrapper alive to report usage.

## Rejected alternatives

- Reservation-derived per-action `memory.max`: a healthy transient spike can be killed before the shared pool is under pressure.
- Moving the wrapper into the action scope: a scope OOM would prevent it from reporting the outcome.
- Joining with unsafe pre-exec code or `clone3(CLONE_INTO_CGROUP)`: the workspace forbids unsafe code.
