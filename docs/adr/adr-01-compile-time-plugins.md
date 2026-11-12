# ADR-01: Backends are compile-time plugins composed into one binary

Status: Accepted; amended: fixed adapters gang-mtls and gang-tls sit at tiers 27 and 28.

## Context
- Backends for compute, storage, database, signing and action isolation differ per deployment: AWS, Local Mode, later Kubernetes and Slurm.
- A runtime registry costs dynamic dispatch, stringly typed selection and unused code in every binary. The reconcile path must not allocate.

## Decision
- **Domains.** Each backend area is a domain whose ports live in `gang-spi`: provisioner, cache, signer, scheduler, database, sandbox, identity provider, client authentication, worker authentication, and the optional action limits. The domain table is in SPECIFICATION §1.1.
- **Plugins.** A plugin implements its domain's ports directly, has a typed `Config` (`deny_unknown_fields`) and `build(config, &PluginContext)`. A crate may provide plugins in several domains.
- **Composition.** `gang-main` lists plugins per domain in one `gang_spi::compose!` call, which generates the typed configuration enum, a statically dispatched enum with delegating port impls, and the `PluginSet`. A new backend is a crate plus one line.
- **Domain-transparent configuration.** A section such as `[provisioner.aws]` both activates the backend and is its typed configuration. There is no selection key; an unknown or missing backend is a parse error naming the accepted keys.
- **Isolation.** Plugin crates sit in Ring 3 at tier 30 and cannot depend on each other; only `gang-main` imports them. The fixed adapters `gang-mtls` (tier 27) and `gang-tls` (tier 28) sit below the plugins, so plugins may use them (`gang-oidc` uses `gang-tls`). The Ring 4 crates may import the fixed adapters (`gang-mtls`, `gang-tls`, `gang-ui`; `gang-embed` is a build tool), which are not plugins. Rings 0-2 hold no cloud, SQL, TLS, storage or isolation mechanics.

## Consequences
- Port calls on the reconcile path are statically dispatched and do not box or allocate.
- A misspelled backend fails at parse time; core crates test against in-memory adapters.
- Every plugin links into the single `gang` binary, so adding one needs a rebuild.
- Plugins share data only through ports and host objects the server lends (ADR-05).
- Ring and tier ordering has no automated check; review enforces it.
- Provisioner port names stay provisional until a provisioner beyond `aws` and `local` is specified.

## Rejected alternatives
- **Runtime plugin registry** with trait objects and a `[selection]` or `[plugins.*]` table: dynamic dispatch, unchecked names, allocation on hot paths.
- **Cargo features to choose backends:** they hide the compiled set from `compose!` and the typed configuration.
