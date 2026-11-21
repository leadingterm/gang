# ADR-61: Digested action root filesystem

Status: Accepted; the identity and admission decision was superseded in the same release.

## Context

Actions need a predictable glibc environment and Bash toolbox without exposing the worker's Nix store or leaking nix-ld loader variables. Cache identity must be managed separately from the image's content digest.

## Decision

- Bwrap actions use a read-only, per-architecture Debian 13 rootfs assembled from hash-pinned packages, with static builder identity (UID and GID 1000), public trust roots and the approved GNU toolbox.
- The content-derived rootfs tree digest is an operational image fact recorded in image metadata and AMI tags, not a request identity.
- The server config's deliberate versioned name, `[actions] environment` (for example, `debian-13/1`), namespaces the action cache; the CAS is not namespaced by it. The initial version uses the existing unprefixed action-cache layout, accepting cross-sandbox reuse as with any single default image. Requests carry no environment identity.
- Noop makes no isolation claim.

## Consequences

- The v0.3.4 deploy keeps the existing action cache warm. Security and patch image rebuilds keep the environment name and the cache warm. A later reviewed identity bump for a change to what actions see costs one cold action cache. Image updates need no client action; running builds continue on old workers during rollouts.
- Bwrap no longer exposes host store or loader binds. `/gang-reset` remains outside the rootfs to preserve signal semantics. Tools outside the toolbox must be declared by the action.
- This updates ADR-46's worker closure, not its one-image-per-architecture decision. See the specification for the construction and rollout contracts.

## Rejected alternatives

- No shell: Bazel test wrappers require Bash.
- Nixpkgs glibc at FHS paths: its loader needs a rebuilt configuration and store-linked userland.
- Keep nix-ld and host binds: they expose host contents and loader environment effects.
- Using the image digest as request and cache identity: security and patch rebuilds would needlessly invalidate the action cache and couple clients to image rollouts.
