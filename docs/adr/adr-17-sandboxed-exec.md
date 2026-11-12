# ADR-17: Actions run through `gang exec` in a pluggable sandbox

Status: Accepted

## Context
- Actions are untrusted build code running on a worker that holds a certificate, instance credentials and a shared content cache.
- Isolation, input integrity (ADR-18) and resource limits (ADR-19, ADR-20) are separate concerns.

## Decision
- **Sandbox domain.** `ActionIsolationPort` has one or more plugins active; the worker's fleet selects the one that runs an action (ADR-16).
  - `gang-bwrap` (Linux) runs `bwrap` with all namespaces unshared, `--clearenv`, read-only binds, the working directory bound read-write and the CAS directory invisible. With the network namespace unshared the action reaches neither IMDS nor the controller.
  - `gang-noop` runs the action directly with an isolated `TMPDIR`, for macOS Local Mode and environments where the pod or job already isolates.
- **Network** is shared only when the action's `network = requested` (exported as `GANG_NETWORK`) and `permit_network_property = true` on `gang-bwrap` are both set; `gang-noop` ignores it.
- **`gang exec` is the boundary Gang owns.** It is NativeLink's worker `entrypoint`, reached by argv0 multicall through the `gang-exec` link. Its order is fixed: read the sandbox file, apply `input_links`, enter the limits scope, run the action, leave the scope, exit. `Exited(c)` exits `c` and `Signaled(n)` exits `128 + n`.
- **The sandbox file** written by `gang worker` beside the link is the only configuration `gang exec` reads. The environment is untrusted; a working directory outside the recorded work root is rejected.

## Consequences
- Isolation, integrity and limits are independent seams; each can be absent.
- Actions still run under the worker's uid.

## Rejected alternatives
- **A separate uid per action:** needs privilege; deferred hardening.
