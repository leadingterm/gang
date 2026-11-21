# ADR-17: Actions run through `gang exec` in a pluggable sandbox

Status: Accepted; the former host-sharing network policy is superseded by ADR-58.

## Context
- Actions are untrusted build code running on a worker that holds a certificate, instance credentials and a shared content cache.
- Isolation, input integrity (ADR-18) and resource limits (ADR-19, ADR-20) are separate concerns.

## Decision
- **Sandbox domain.** `ActionIsolationPort` has one or more plugins active; the worker's fleet selects the one that runs an action (ADR-16).
  - `gang-bwrap` (Linux) runs `bwrap` with isolated namespaces, `--clearenv`, read-only binds, the action directory bound read-write with the command in its working directory, and the CAS directory and other actions' directories invisible. Offline actions use an unshared network namespace; granted actions use isolated pasta networking and the host policy in SPEC §3.4.
  - `gang-noop` runs the action directly with an isolated `TMPDIR`, for macOS Local Mode and environments where the pod or job already isolates.
- **Network** never shares the host network; requests are rejected by default without a selected-fleet grant (ADR-58).
- **`gang exec` is the boundary Gang owns.** It is NativeLink's worker `entrypoint`, reached by argv0 multicall through the `gang-exec` link. Its order is fixed: read the sandbox file, apply `input_links`, enter the limits scope, run the action, leave the scope, exit. `Exited(c)` exits `c` and `Signaled(n)` exits `128 + n`. Termination ownership is recorded in ADR-54.
- **The sandbox file** written by `gang worker` beside the link is the only configuration `gang exec` reads. The environment is untrusted; a working directory outside the recorded work root is rejected.

## Consequences
- Isolation, integrity and limits are independent seams; each can be absent.
- An action sees its whole action directory, so inputs beside a nonempty REAPI working directory resolve; the directory `gang-bwrap` exposes and the one `input_links = break` walks are the same derived path.
- Actions still run under the worker's uid.

## Rejected alternatives
- **A separate uid per action:** needs privilege; deferred hardening.
