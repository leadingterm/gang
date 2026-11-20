# ADR-61: Digested action root filesystem

Status: Accepted.

## Context

Actions need a predictable glibc environment and Bash toolbox without exposing the worker's Nix store or leaking nix-ld loader variables. A manually chosen cache revision can drift from the actual environment.

## Decision

- Bwrap actions use a read-only, per-architecture Debian 13 rootfs assembled from pinned packages, with static builder identity, public trust roots and the approved GNU toolbox.
- The action-environment identity is a shared digest of both architecture trees. Workers register the identity of their actual sandbox; admission and dispatch enforce it exactly.
- Noop instead uses a disjoint, operator-asserted `host:<identifier>` identity and makes no isolation claim.
- Client setup obtains the fleet launch identity from the server. Explicit Bazel platforms and target properties merge the same identity as default properties.

## Consequences

- Changing the rootfs changes the cache identity and requires clients to rerun `gang setup`. Mixed identities can run only on matching workers; queued work can still reach its existing timeout after its last matching worker leaves.
- Bwrap no longer exposes host store or loader binds. `/gang-reset` remains outside the rootfs to preserve signal semantics. Tools outside the toolbox must be declared by the action.
- This updates ADR-46's worker closure, not its one-image-per-architecture decision. See the specification for the construction, admission and migration contracts.

## Rejected alternatives

- No shell: Bazel test wrappers require Bash.
- Nixpkgs glibc at FHS paths: its loader needs a rebuilt configuration and store-linked userland.
- Keep nix-ld and host binds: they expose host contents and loader environment effects.
- Hand-maintained revision: it can drift from the actual action environment.
