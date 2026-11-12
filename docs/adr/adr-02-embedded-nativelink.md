# ADR-02: NativeLink is embedded as a library and receives accepted connections

Status: Accepted

## Context
- NativeLink ships as a binary driven by a JSON5 file, with its own listeners, TLS from files and an admin HTTP API.
- Running it as a subprocess makes Gang a supervisor and puts TLS and revocation inside NativeLink, where changing certificate material needs a restart. Gang owns identity, so the controller must not own transport security.

## Decision
- **Linked as libraries.** The NativeLink crates are Cargo git dependencies pinned to tag `v1.8.3`, linked into the `gang` binary. `gang-nativelink` reimplements only the glue of upstream `src/bin/nativelink.rs`. There is no process, config file, admin HTTP or loopback listener.
- **Typed configuration in memory.** Stores, scheduler and services are built from typed `nativelink-config` values deserialized in memory, so NativeLink's defaults apply.
- **Direct calls.** `SchedulerPort` reads and changes scheduler state by function call, not by HTTP polling.
- **The controller listens for nothing.** `run_controller` serves gRPC on connections `gang-server` has already accepted through `gang-tls` (ADR-03). Per-request authentication reaches it as a request gate (ADR-05). It holds no TLS, revocation or listener addresses.
- **The worker runs in process.** `run_worker` runs NativeLink's local worker inside `gang worker`; actions run through the `gang-exec` argv0 link (ADR-17).

## Consequences
- One artifact on laptops, CI, controller and workers; no supervision, rendering or file handoff.
- Authentication and revocation are live because Gang owns every connection (ADR-08).
- The scheduler internals Gang calls are not a stable API: an upgrade bumps the tag and re-verifies the glue and the direct calls, and follows upstream's binary changes.
- The binary is larger and builds more slowly.

## Rejected alternatives
- **A supervised `nativelink` subprocess** with rendered JSON5 and the admin HTTP API: a second process and a loopback listener, and TLS and CRL are read only at listener start, so every revocation would restart the controller.
