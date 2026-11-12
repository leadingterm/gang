# ADR-21: The Linux binary is fully static

Status: Accepted

## Context
- NativeLink starts `gang exec` with the action's environment. A dynamic loader would run an action's `LD_PRELOAD`, `LD_LIBRARY_PATH` or `LD_AUDIT` library as the worker user (worker key, content store, instance credentials) before `main`.

## Decision
- The Linux `gang` binary is fully static (`x86_64` and `aarch64` musl): no ELF interpreter and no `DT_NEEDED`, so no loader exists to read those variables.
- Two integration tests of the built binary guard it: it has no `PT_INTERP` or `DT_NEEDED`, and `gang-exec` run with an `LD_PRELOAD` library that creates a marker file creates none, while a dynamic control process does.

## Consequences
- The Linux build is musl-only; no plugin is dynamically loaded (consistent with ADR-01).
- macOS cannot link statically, and its actions run under `noop` isolation.

## Rejected alternatives
- **Scrubbing the environment inside `gang exec`:** it would run after the loader.
