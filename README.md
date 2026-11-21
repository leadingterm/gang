# Gang

Gang runs Bazel remote execution on workers that exist only while there is work. One `gang` binary includes the Gang scheduler, authenticates callers with the plugins chosen in `gang.toml` (a short-lived bearer token for people and CI, mutual TLS for workers), reconciles named worker fleets from queued demand, and runs every action in a sandbox. Provisioner, cache, signer, database, sandbox, identity provider, client and worker authentication, and action limits backends are composed at compile time and chosen by writing their sections in `gang.toml`; the scheduler is built in and has no backend choice.

## Commands

`gang` is the only artifact; laptops, CI, the controller and workers run the same build. Clients use `setup`, `login`, `get`, `bazelrc`, `doctor`, `revoke` and `restore`. The controller and workers use `server`, `worker` and the hidden `exec`. Local Mode uses `local up` and `local down`. `options` lists each provisioner's fleet options. `gang --help` lists the commands people run; `get`, `worker` and `exec` are hidden from it. [`docs/SPECIFICATION.md`](docs/SPECIFICATION.md) §6 specifies them all.

## Authentication

Clients and CI send a bearer token. `gang login` exchanges an identity provider proof for a short-lived token, and Bazel calls `gang get` as its credential helper. CI obtains its job token with `gang login --workload --token-command`. Workers keep mutual TLS.

Gang terminates TLS itself when `[server] transport = "tls"`. Behind a tunnel or proxy that terminates TLS, `transport = "plaintext"` runs Gang without TLS; the public listener is one name on port 443 and the worker listener stays private. `gang local up --insecure` runs Local Mode in plaintext with no authentication.

## Dashboard

`gang server` serves a read-only operator dashboard at `{endpoint}/ui/`: the fleets, workers, running and queued actions, scaling events and principals, streamed live. The view is derived from the scheduler and the provisioner on every pass and is never authoritative. `[dashboard] enabled = false` turns it off.

`gang local up --insecure` binds the `administrator` role to the anonymous caller and prints the dashboard URL as the `ui` field; open it.

In production a viewer signs in through `[dashboard.login]` and sees data only when a `[[server.roles]]` role grants `state.read`. Setup is in section 1.3 of the specification ("Dashboard sign-in"), and the routes are in [`docs/API.md`](docs/API.md).

## Documentation

- [`docs/PLAN.md`](docs/PLAN.md): architecture pillars, crate graph, and implementation sequence with its verification gates.
- [`docs/SPECIFICATION.md`](docs/SPECIFICATION.md): configuration, ports, plugins, identity, the reconciler, and the gates.
- [`docs/API.md`](docs/API.md): the HTTP API reference: routes, bodies, examples, and errors.
- [`docs/adr`](docs/adr): the architecture decision records.

## Build

The flake devShell is the supported build entry point: it supplies Bazel and the action shell and tools pinned by `flake.lock`.
On macOS, actions additionally see `/usr/bin` for Apple's toolchain.
Repository rules see the client's environment (`--incompatible_repo_env_ignores_action_env` in `.bazelrc`), so the actions' pinned `PATH` never hides the host toolchain that `cc_configure` needs on macOS.

```sh
nix develop -c bazel build //...
nix develop -c bazel test //...
```

CI is the merge gate: `ci.yml` runs on every pull request, including drafts and updates, pushes to `main` and `master` and `v*` tags, and manual dispatch. Only documentation-only pull requests skip heavy jobs; `docs/API.md` and `docs/PLAN.md` are live test inputs and do not qualify. The PR critical path is exclusively x86-64: portable lint, Nix VM checks, the complete partitioned Linux test suite, a parallel binary and harness bundle build, and parallel end-to-end gates. ARM and cross validation run on default-branch pushes and version tags and gate release artifact reuse through `release-validation`. macOS runs as a standalone advisory on public release events; it neither delays nor gates releases, and private RunsOn repositories skip it. Superseded PR and branch runs are cancelled. Public CI uses GitHub's free standard runners; private CI uses ephemeral RunsOn Linux runners with 4 CPUs and 16 GB RAM. Require a successful `required` check on the latest PR revision before merging. CI uses read-only contents permission, no secrets, no OIDC and no Gang farm. See [specification §7.1](docs/SPECIFICATION.md#71-continuous-integration) for exact checks, skip policy, runners and cache budgets.

The complete x86 test suite uses four shards. Each end-to-end gate uses eight deterministic, warm-timing-balanced shards with four parallel scenarios per harness: every Gate 1 scenario and Gate 2 addition runs without reduced checks or repeated Gate 1 coverage. The binary and harness still build from source and upload separately; only their redundant cache working-set export is removed, with build BEP/profile artifacts retained even on failure. The nonrequired cache fan-in merges the four test-shard exports plus lint. Five-minute warm CI and two-minute whole E2E jobs remain targets pending real workflow measurements.

Portable lint runs `bazel run //:gazelle.check`, infrastructure and workflow validation, and the behavioral guards, including `tools/check-required-jobs_test.sh`. Rustfmt and Clippy for every x86 Rust target, tests included, run in the full test shards rather than recompiling the workspace in lint; portable lint has a three-minute budget. Darwin all-target Clippy belongs to macOS release validation, not the PR gate. `tools/lint.sh --portable` and `tools/lint.sh --nix-vm` select the two local lint partitions; on x86 Linux, `tools/lint.sh` runs both. The required-jobs guard checks fail-closed status aggregation and dependency coverage. Run it locally with `nix develop -c tools/check-required-jobs_test.sh`. Cross-target toolchains are described in [specification §8.4](docs/SPECIFICATION.md#84-nixos-images).

OpenTofu provider ZIPs are checksum-pinned in `MODULE.bazel` and fetched through
Bazel's repository cache, so separate output bases can reuse the downloads. When
updating a provider, regenerate the Terraform lockfiles and update its platform
checksums from the OpenTofu registry; the archive checksums must match the
lockfiles' `zh:` entries. A missing checksum or mismatched archive fails the
fetch.

## Quick start: Local Mode

Local Mode runs the controller and on-demand workers on one machine from `configs/gang.local.toml` (specification section 1.4). Actions run under `bwrap` on Linux. On macOS, set the fleet's `sandbox = "noop"` in that file first, because bubblewrap is Linux-only. Install the binary first.

```sh
nix develop -c bazel build -c opt //:gang && install bazel-bin/crates/gang-main/gang ~/.local/bin/gang
gang local up --insecure          # from this checkout; loopback only, plaintext, no authentication; prints the endpoints and the next step
cd <your Bazel workspace>
gang setup --server http://127.0.0.1:50051
nix develop <path-to-gang-checkout> -c bazel build //...  # workers start while actions are queued and stop when idle
gang local down                   # from this checkout
```

Against a deployment, run setup from the Bazel workspace:

```sh
gang setup --server https://gang.example.com
```

Setup writes Gang-owned files under `.gang/` and one `try-import` line in `.bazelrc`, safe to commit; it shows every change first (`--dry-run` only previews, `--yes` applies for scripts). `gang bazelrc` remains the printing primitive for scripts and isolated rc files.

Generated rc files use `--remote_download_outputs=toplevel`: requested build artifacts and test outputs remain local, while unused intermediate outputs stay in the remote cache. `bazel run` still downloads what it needs to run locally. Pass Bazel's own `--remote_download_outputs=all` flag when you need every intermediate output.

TLS Local Mode (`gang local up` without `--insecure`) accepts client tokens only from the providers in `[authentication.clients.token] providers`. Add an interactive provider (specification section 1.3) before you `gang login` to it. Run the end-to-end harness from this checkout with `nix develop -c bazel run //crates/gang-e2e:local -- --gate 1` (specification section 6.5). The `gang` binary and pinned Bazel come from the harness's runfiles; `--gang` and `--bazel` override them. `nix develop -c tools/gate.sh` mirrors the x86 PR coverage on one x86 Linux host: both local lint partitions, the full workspace, the binary and harness bundle, and every end-to-end gate scenario (specification section 7). It is an optional local diagnostic, not a mandatory duplicate of CI, and does not run Darwin Clippy. Every Rust target, tests included, uses the lint policy in `[workspace.lints]` of `Cargo.toml`.

The harness settings `startup_timeout` and `command_timeout` default to `2m`; `build_timeout`
defaults to `30m` for each Bazel invocation, including shutdown. `handshake_timeout` defaults to
`5s` and bounds connection setup.

Gang-issued client tokens and local workload proofs accept a minimum TTL of `20s`; AWS STS workload proofs retain their `60s` minimum. Defaults remain `10m` for client tokens and `15m` for workload proofs. The browser sessions E2E fixture uses `25s` tokens and a `60s` absolute session age; this changes neither production defaults nor expiry/renewal coverage. Its 60-second shard weight is provisional until a new workflow measures it.

## Deploying

Gang is deployed from a separate repository that calls its modules and reusable workflow at one release tag. This repository holds no deployment.

The deployment module defaults to `database.engine = "sqlite"` for one controller. Choose `postgres` or `dynamodb` for one or several controllers behind load balancers; shared engines require a shared S3 cache. See the [configuration reference](docs/SPECIFICATION.md) for the settings.
Rate-based purchases and N−1 utilization scale-in are configured under `[scaling]`; see the [configuration reference](docs/SPECIFICATION.md#12-cratesgang-configsrc) for defaults, bounds and rollback constraints.

Action scheduling and resource policy are configured under `[actions]`,
`[actions.memory]`, `[actions.disk]`, and `[actions.cold_start]`. Memory
pressure enforcement uses a worker-wide action pool; `reserve` keeps memory
outside that pool and sets its shortage watermark. By default, a kill requires
10% PSI stalls and available pool memory below the configured reserve together
for 5 seconds. After a pressure kill, the worker waits for the victim's cleanup,
rechecks available space, and requires a fresh continuous pressure window.
Each fleet must fit the configured minimum reservations and must have more
memory than the reserve. See specification §3.2 for the full field reference and
defaults.

The server-owned `[actions] environment` name (for example, `debian-13/1`) namespaces the action cache. The initial version uses the existing unprefixed cache, keeping it warm at the v0.3.4 deploy. Security and patch image updates keep the name and need no client action; later reviewed identity bumps create a cold action cache.

```hcl
module "gang" {
  source = "git::https://github.com/example-org/gang//infra/terraform/modules/deployment?ref=v0.3.4"
  # variables as in the example root
}
```

Call `.github/workflows/deploy.yml` as its header comment shows, pinned to the same tag, with `gang_sha256` set to the release's checksum of the binary for the deploy runner (the `gang-x86_64-unknown-linux-musl` line of the release's `SHA256SUMS` for the default x86 runner). To use ARM, set `runner: ubuntu-24.04-arm` and pin the checksum of `gang-aarch64-unknown-linux-musl`. The workflow downloads that binary and refuses any other; it never compiles Gang.

The apply job uses the `production` GitHub environment by default. Set `apply_environment` to a different name to use that environment, or explicitly to `''` to run without one. Without an environment there are no environment protection rules or environment secrets; the apply role's OIDC trust must instead match the `main` branch subject. See the deployment contract for the trust policy.

Every `v*` tag has a GitHub release with Linux binaries, disk images for x86_64 and aarch64, and `SHA256SUMS`; the macOS binary is included when available. Release dry runs and tags require successful `required` and `release-validation` checks from a same-repository push to `main` for the exact commit, never PR artifacts or caches. `gang-amis.json` lists the primary publication regions only; remaining public AMI copies and lifecycle work run independently (specification section 7.2). Deployment uses verified checksums and a provider-region AMI lookup, not the manifest. Check downloaded assets with `sha256sum --check --ignore-missing SHA256SUMS`, and pin the binary checksum where you deploy it.

See section 8 of [`docs/SPECIFICATION.md`](docs/SPECIFICATION.md) for the deployment contract, and the example root [`infra/terraform/environments/example`](infra/terraform/environments/example).

To avoid a cold start on the first build of the day, a fleet can keep instances during given hours. Set `start` a few minutes before people arrive; a worker needs about a minute to come into service. Outside the windows the fleet scales to `min_instances`.

The deployment module's `fleets[].scheduled_minimum` input renders these windows into `gang.toml`.

```toml
[[fleets]]
name = "workers-x86-64"
# … the fleet's other keys

[[fleets.scheduled_minimum]]
instances = 1
days = ["mon", "tue", "wed", "thu", "fri"]
start = "07:55"
end = "19:00"
time_zone = "Europe/Paris"
```

## Action capabilities

Actions have no network grant by default. Unknown platform properties (except the harmless `supports-workers` marker and legacy request cache label `gang_action_environment`), network requests without an eligible fleet grant, and requests exceeding action ceilings are rejected at admission. Workers register their operational `ActionEnvironment` as `gang_action_environment`, ignored by matching. To grant public egress, extend an existing `bwrap` fleet in `gang.toml`:

```toml
[fleets.actions]
grants = ["network"]

[fleets.actions.resources]
cpu_count = 4
memory = "8GiB"
disk = "20GiB"

[execution.egress]
protected_networks = ["10.20.0.0/16", "203.0.113.10/32"]
dns_servers = ["1.1.1.1"]
setup_timeout = "10s"

[sandbox.bwrap.egress]
```

The resource ceilings are optional per-action request ceilings, not total worker capacity. For a hand-written `gang.toml`, replace the example protected networks with the complete operator-specific private, controller, public load-balancer and worker address spaces, including IPv6. The runtime `protected_networks` and `dns_servers` must be nonempty; DNS servers must be public and outside protected networks. In the Terraform/OpenTofu deployment module, `execution_egress.protected_networks` instead adds address spaces to the module-derived VPC IPv4 and assigned IPv6 CIDRs, controller public IP when present, and public load-balancer static IPs; this optional operator list defaults to `[]`. The module omits `[fleets.actions]` when grants are empty and `[fleets.actions.resources]` when no ceilings are set. Actions use only the configured resolvers, not host DNS. `setup_timeout` bounds network setup and defaults to `10s`. A network grant requires an enforcing `bwrap` sandbox; `noop` cannot grant it. Request network explicitly on the relevant Bazel rule or execution platform:

```starlark
exec_properties = {"ISA": "x86-64", "network": "requested"}
```

Explicit execution properties may override defaults, so retain the ISA of your selected execution platform. Bazel may not apply the `gang bazelrc` default ISA when explicit execution properties are present.

The `requires-network` tag alone does not transmit that remote request. See the [specification](docs/SPECIFICATION.md#34-sandboxes-gang-noop-gang-bwrap) for the complete policy, resolver and sandbox contract.

## Troubleshooting

A build fails with `Action ... is missing from CAS`, or uploads go to a cache other than Gang's. Run `gang doctor` in the workspace, after `gang login`. It names the file and section that set each flag it finds wrong.

The usual cause is a `~/.bazelrc`, an imported rc file, or a `--config` that sets `--remote_cache` or `--remote_executor`. Those flags override the workspace's committed Gang flags. Remove them from that file or config.

## License

Gang's source is licensed under the [Apache License 2.0](LICENSE). Distributed binaries include third-party code under additional licenses with use restrictions; see [NOTICE](NOTICE) for terms and attribution.

