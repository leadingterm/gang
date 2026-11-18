# ADR-54: Gang relays action termination without consuming the kill grace

Status: Accepted. Complements ADR-17; abandoned limit-scope recovery remains in ADR-19.

## Context
- NativeLink owns the termination deadline and can kill `gang exec` by PID after the grace period. Gang must let the action use that grace without leaving a sandbox or limit scope behind.
- The noop action shares its original process group; bwrap creates a separate action group. One group-signalling policy cannot serve both.

## Decision
- `gang exec` owns termination observation and relay. A request before action start prevents the start; a later request reaches the running sandbox through `SandboxedAction::relay`.
- After noop starts, `gang exec` moves to the worker group so the action-group kill spares cleanup. With bwrap it stays in the original group to relay signals; the outer bwrap blocks TERM/INT and `gang-reset` clears the action's inherited mask.
- TERM/INT use handlers, not ignored dispositions. Normal completion leaves the limit scope; PID-targeted death is covered by ADR-19's guardian.

## Consequences
- NativeLink remains the sole owner of the grace deadline. Gang does not add a second grace timer.
- Signal disposition, process-group membership and cleanup ownership are part of the sandbox contract; the detailed execution order lives in SPECIFICATION §3.4 and §3.7–3.8.

## Rejected alternatives
- Ignoring TERM/INT: ignored dispositions survive exec and can make the action ignore termination.
- Keeping `gang exec` in the noop action group: the group kill can end it before scope cleanup.
- Relying only on normal cleanup: NativeLink's PID-targeted SIGKILL bypasses it.
