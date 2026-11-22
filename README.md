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
- [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md): contributor workflow and merging with GitHub's merge queue.
- [`docs/adr`](docs/adr): the architecture decision records.

## Build

The flake devShell is the supported build entry point: it supplies Bazel and action tools pinned by `flake.lock`. Without `--config=local`, Bazel uses its normal defaults and workspace settings; remote actions need FHS-compatible shell and tool paths provided by the worker root filesystem.
Use `--config=local` for local builds and tests to select the pinned Nix shell and tools and restrict execution to local strategies. On macOS, local actions additionally see `/usr/bin` for Apple's toolchain.
Do not use `--config=local` on remote clients: its host-specific Nix store paths are not available on workers, and it prevents remote execution. Repository rules see the client's environment (`--incompatible_repo_env_ignores_action_env` in `.bazelrc`), so the local actions' pinned `PATH` never hides the host toolchain that `cc_configure` needs on macOS.

```sh
nix develop -c bazel build --config=local //...
nix develop -c bazel test --config=local //...
```

Pull requests, including drafts and forks, run `.github/workflows/pr.yml` on the synthetic PR checkout. It runs portable lint, builds `//:gang`, runs all generated Clippy and rustfmt tests, and selects affected Rust `*_unit_test` targets through reverse dependencies. Uncertain ownership, deleted paths and graph-wide changes conservatively select all unit tests. Documentation-only PRs still run the real build and lint; a legitimate empty affected-unit selection is not a full test run.

`.github/workflows/ci.yml` runs the full suite only for `merge_group` events, including documentation-only groups. It runs four x86 test shards, eight Gate 1 and eight Gate 2 addition shards, NativeLink memory tests, portable lint, Nix VM checks, x86 and ARM binary and harness builds, native ARM boundary tests and the ARM-to-x86 cross build. Public macOS validation is advisory on queue runs; private repositories skip it.

GitHub's `main` merge queue replaces up-to-date-branch rebases: its HEADGREEN strategy checks the combined head tree before merging, while preserving squash-only, linear, signed commits. GitHub may speculatively build queue prefixes. The six ruleset-required check names remain `lint`, `cross`, `linux-x86_64`, `linux-arm64`, `e2e-gate-1` and `e2e-gate-2`. In a PR context each means the entire real preflight succeeded, not that ARM, cross or E2E ran. In a queue context each means full required qualification succeeded through `release-validation` and its ten direct critical dependencies. Failure, cancellation, skipped or missing producers cannot pass.

After a main push, `cache-promotion.yml` validates the qualifying queue run and imports the five x86 working sets (four test shards and lint) through the bounded cache fan-in, without a Bazel build or test. `pr-conflicts.yml` checks every open PR against trusted main and reports conflicts without running PR code. Releases consume only a qualified same-repository queue run for the exact tagged main commit and tree, not PR or push CI; see [verification and releases](docs/SPECIFICATION.md#7-verification-gates). `tools/gate.sh` is a full x86 queue diagnostic, not PR parity.

The devShell supplies the same source-built, patched OpenTofu to both the shell and
Bazel through `GANG_OPENTOFU`. Bazel verifies the package version and executable
checksum, then copies the executable into its toolchain; it does not use a host
`tofu` or an unpatched release binary. The patch fixes an
[upstream forget-node defect](https://github.com/opentofu/opentofu/issues/4869)
that affects ordinary destroy applies as well as test cleanup: retained-resource
cleanup must not evaluate configuration against already-removed dependencies.

OpenTofu provider ZIPs are checksum-pinned in `MODULE.bazel` and fetched through
Bazel's repository cache, so separate output bases can reuse the downloads. When
updating a provider, regenerate the Terraform lockfiles and update its platform
checksums from the OpenTofu registry; the archive checksums must match the
lockfiles' `zh:` entries. A missing checksum or mismatched archive fails the
fetch.

## Quick start: Local Mode

Local Mode runs the controller and on-demand workers on one machine from `configs/gang.local.toml` (specification section 1.4). Actions run under `bwrap` on Linux. On macOS, set the fleet's `sandbox = "noop"` in that file first, because bubblewrap is Linux-only. Install the binary first.

```sh
nix develop -c bazel build --config=local -c opt //:gang && install bazel-bin/crates/gang-main/gang ~/.local/bin/gang
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

TLS Local Mode (`gang local up` without `--insecure`) accepts client tokens only from the providers in `[authentication.clients.token] providers`. Add an interactive provider (specification section 1.3) before you `gang login` to it. Run the end-to-end harness from this checkout with `nix develop -c bazel run --config=local //crates/gang-e2e:local -- --gate 1` (specification section 6.5). The `gang` binary and pinned Bazel come from the harness's runfiles; `--gang` and `--bazel` override them. `GANG_CONTROLLER_BINARY=<qualified-same-commit-ARM64-static-gang> nix develop -c tools/gate.sh` is an optional full x86 queue diagnostic on one x86 Linux host: both local lint partitions, the full workspace, the binary and harness bundle, and every end-to-end gate scenario (specification section 7). It is not PR parity and does not run Darwin Clippy. Every Rust target, tests included, uses the lint policy in `[workspace.lints]` of `Cargo.toml`.

The harness settings `startup_timeout` and `command_timeout` default to `2m`; `build_timeout`
defaults to `30m` for each Bazel invocation, including shutdown. `handshake_timeout` defaults to
`5s` and bounds connection setup.

Gang-issued client tokens and local workload proofs accept a minimum TTL of `20s`; AWS STS workload proofs retain their `60s` minimum. Defaults remain `10m` for client tokens and `15m` for workload proofs. The browser sessions E2E fixture uses `25s` tokens and a `60s` absolute session age; this changes neither production defaults nor expiry/renewal coverage. Its 60-second shard weight is provisional until a new workflow measures it.

## Deploying

Gang is deployed from a separate repository that calls its modules and reusable workflow at one release tag. This repository holds no deployment.

The deployment module defaults to `database.engine = "sqlite"` for one standalone EC2 controller without HA. Choose `postgres` or `dynamodb` for a shared database behind load balancers; the module deploys one active controller, requires a shared S3 cache and takes `controller_image_digest` for the image in the deployment's private ECR repository. Gang's shared-database runtime supports multiple controllers in separately managed deployments. The release's `gang-controller.json` records the image digest. ECS EC2 controllers use cooperative handoff followed by bounded SIGTERM drain. See the [configuration reference](docs/SPECIFICATION.md) for the settings.

Upgrading an existing shared-database controller ASG requires a staged migration, not a normal one-step apply: retain the old controller while creating the ECS service and IP target groups, preserve its routing during cutover, then stop it and let admitted work drain before removing old resources. See [the deployment contract](docs/SPECIFICATION.md#83-configuration-delivery-and-trust-anchor).
Resource-based scale-out and N−1 utilization scale-in are configured under `[actions]` and `[scaling]`; see the [configuration reference](docs/SPECIFICATION.md#12-cratesgang-configsrc) for defaults, bounds and rollback constraints.

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

Deletion or replacement of any Auto Scaling group is blocked by default, including ECS capacity and worker groups. For an intentional destruction, set the workflow's `allow_asg_destroy` input to the full exact Terraform resource address, one address per line for multiple groups. The review comment discloses those exceptions and apply checks them again. Empty input or blank lines grant no exceptions; wildcards and prefixes do not match, and other protected resource types cannot be exempted.

Every `v*` tag has a GitHub release with Linux binaries, disk images for x86_64 and aarch64, the controller OCI image asset and `gang-controller.json`, generated `CHANGELOG.md` and `SHA256SUMS`; the macOS binary is included when available. Release artifact reuse requires a qualified same-repository `ci.yml` merge-group run whose head SHA and tree exactly match the tagged commit on `main`, with the full gate, all six readiness checks and both unexpired Linux artifacts successful. PR and push runs are not fallbacks, and advisory macOS is not waited for. Dispatch remains optional; publication itself builds and UEFI-boots the images, checks reproducibility with a rebuild, and retains checksums, signed provenance and AMI verification. Release notes begin with the generated current changelog section; contributor fragments and retained history are described in [Contributing](docs/CONTRIBUTING.md#changelog-entries). `gang-amis.json` lists the primary publication regions only; remaining public AMI copies and lifecycle work run independently (specification section 7.2). AMI deployment uses verified checksums and a provider-region lookup. The deploy workflow verifies the controller OCI asset checksum, pushes it to private ECR and checks the image digest before apply. Check downloaded assets with `sha256sum --check --ignore-missing SHA256SUMS`, and pin the binary checksum where you deploy it.

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

The resource ceilings are optional per-action request ceilings, not total worker capacity. For a hand-written `gang.toml`, replace the example protected networks with the complete operator-specific private, controller, public load-balancer and worker address spaces, including IPv6. The runtime `protected_networks` and `dns_servers` must be nonempty; DNS servers must be public and outside protected networks. In the Terraform/OpenTofu deployment module, `execution_egress.protected_networks` instead adds address spaces to the module-derived VPC IPv4 and assigned IPv6 CIDRs, controller public IP when present, and public load-balancer static IPs; this optional operator list defaults to `[]`. The module omits `[fleets.actions]` when grants are empty and `[fleets.actions.resources]` when no ceilings are set. Actions use only the configured resolvers, not host DNS. `setup_timeout` remains a nonzero configuration field defaulting to `10s`. Granted actions use isolated pasta networking and require a prepared host nftables policy and action cgroup; missing or mismatched prerequisites fail closed. NixOS prepares these automatically before the worker starts. A network grant requires an enforcing `bwrap` sandbox; `noop` cannot grant it. Request network explicitly on the relevant Bazel rule or execution platform:

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

