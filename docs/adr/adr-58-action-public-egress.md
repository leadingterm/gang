# ADR-58: Network grants use isolated public egress, never the host network

## Status
Accepted for the grant and isolation model; its original slirp, per-action nftables and execution-gate mechanism is superseded by [SPEC §3.4](../SPECIFICATION.md#34-sandboxes-gang-noop-gang-bwrap). It supersedes the host-sharing network decision in ADR-17 and the network policy in ADR-16.

## Context
Some actions need public network access, but untrusted action code must not reach worker credentials, infrastructure or other protected networks.

## Decision
- Public egress requires both an explicit `network = requested` action property and the selected fleet's grant. Without a grant the request is rejected; an unrequested bwrap action remains offline.
- Each granted action gets an isolated pasta network namespace. One host nftables policy scopes public TCP/UDP egress to the action cgroup subtree, rejecting host, forbidden and protected destinations. `protected_networks` must cover complete protected address spaces, including public infrastructure addresses.
- A sealed resolver memfd lists only configured public `dns_servers`; host DNS is not used. The host policy and action cgroup are prerequisites checked before a granted action starts; NixOS prepares them before worker startup. `gang-reset` only clears the inherited signal mask before execution.
- The scheduler's platform properties and enforced property-derived environment are authoritative over client-supplied values.

## Consequences
- Network grants require supported isolation and complete protected address-space and public DNS configuration; setup fails closed.
- Each action has isolated egress rather than access to the worker's network namespace.

## Rejected alternatives
- Host `--share-net` exposes the host network and infrastructure.
- A privileged veth daemon adds privileged shared infrastructure to the trust boundary.
- A shared action network namespace weakens isolation between actions.
