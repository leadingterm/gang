# ADR-58: Network grants use isolated public egress, never the host network

## Status
Accepted; supersedes the host-sharing network decision in ADR-17 and the network policy in ADR-16.

## Context
Some actions need public network access, but untrusted action code must not reach worker credentials, infrastructure or other protected networks.

## Decision
- Public egress requires both an explicit `network = requested` action property and the selected fleet's grant. Without a grant the request is rejected; an unrequested bwrap action remains offline.
- Each granted action gets an isolated bwrap network namespace and rootless slirp4netns. An immutable output-only nftables `table inet` has a drop policy: loopback and `fib daddr type local` accepts precede minimal IPv6 NDP/MLD accepts, then forbidden, protected, host and slirp-alias destinations are rejected with ICMP admin-prohibited before public TCP/UDP is accepted. The local accept lets reject-generated ICMP errors reach the sandbox's own sockets. `protected_networks` must cover complete protected address spaces, including public infrastructure addresses.
- A sealed resolver memfd lists only configured public `dns_servers`; host DNS is not used and slirp DNS is disabled. Slirp readiness requires ASCII `1` on `--ready-fd`. Before release, `nsenter` configures `fd00::100/64` on `tap0` with `nodad` and a default route via `fd00::2`, avoiding a SLAAC race.
- A trusted `gang-reset` positive execution gate passes pidfd and network namespace descriptors using SCM_RIGHTS and releases the payload only after network setup succeeds; EOF, error or an invalid release aborts. Normal bwrap identity is preserved.
- The scheduler's platform properties and enforced property-derived environment are authoritative over client-supplied values.

## Consequences
- Network grants require supported isolation and complete protected address-space and public DNS configuration; setup fails closed.
- Each action has isolated egress rather than access to the worker's network namespace.

## Rejected alternatives
- Host `--share-net` exposes the host network and infrastructure.
- A privileged veth daemon adds privileged shared infrastructure to the trust boundary.
- A shared action network namespace weakens isolation between actions.
