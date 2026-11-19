# Gang

Gang runs Bazel remote execution on workers that exist only while there is work. One `gang` binary embeds the NativeLink scheduler as a library, authenticates callers with the plugins chosen in `gang.toml` (a short-lived bearer token for people and CI, mutual TLS for workers), reconciles named worker fleets from queued demand, and runs every action in a sandbox. Every backend (provisioner, cache, signer, scheduler, database, sandbox, identity provider, client and worker authentication, action limits) is a plugin composed at compile time and chosen by writing its section in `gang.toml`.

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

CI is the merge gate: `ci.yml` runs on every pull request, including drafts and updates, pushes to `main` and `master`, and manual dispatch. Only documentation-only pull requests skip heavy jobs; pushes to `main` or `master` and manual runs get full validation. Ordinary docs and notes Markdown is allowlisted, but `docs/API.md` and `docs/PLAN.md` are live test inputs and run heavy jobs. Superseded PR and branch runs are cancelled. Public CI uses GitHub's free standard runners; private CI uses ephemeral RunsOn Linux runners with 4 CPUs and 16 GB RAM and skips unsupported macOS, which public CI validates before release. x86 runs the full Linux suite; native ARM and macOS run targeted tests, with real end-to-end gates on ARM and Darwin Clippy covering every Cargo target on Linux. For work-in-progress branches without an open PR, use focused local checks; open a draft PR for CI feedback. Require a successful `required` check on the latest PR revision before merging. The workflow tests a synthetic merge checkout, uses read-only contents permission, no secrets and no OIDC. See [specification §7.1](docs/SPECIFICATION.md#71-continuous-integration) for the exact checks, skip policy, runners and caches.

Portable lint runs `tools/check-required-jobs_test.sh` once; its first case runs the structural guard on the actual workflow, and subsequent cases exercise added and dropped jobs, status gating and conditional aggregator rejection. The guard checks unconditional `always()` and complete dependencies for each aggregator, protecting the public seven-check policy through `lint`; the docs-only allowlist cannot change CI or its scripts. Run the test locally with `nix develop -c tools/check-required-jobs_test.sh`. `tools/lint.sh --portable` and `tools/lint.sh --nix-vm` select the two lint partitions; on x86, `tools/lint.sh` runs both. Remote ARM execution and cross-target toolchains are described in [specification §8.4](docs/SPECIFICATION.md#84-nixos-images).

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

TLS Local Mode (`gang local up` without `--insecure`) accepts client tokens only from the providers in `[authentication.clients.token] providers`. Add an interactive provider (specification section 1.3) before you `gang login` to it. Run the end-to-end harness from this checkout with `nix develop -c bazel run //crates/gang-e2e:local -- --gate 1` (specification section 6.5). The `gang` binary and pinned Bazel come from the harness's runfiles; `--gang` and `--bazel` override them. `nix develop -c tools/gate.sh` is an equivalent local diagnostic, not a mandatory duplicate of the CI merge gate: it runs both CI lint partitions (`tools/lint.sh`, Clippy of every Cargo target for macOS included), the full workspace and the end-to-end gates on one x86 Linux host (specification section 7). Every Rust target, tests included, is linted with the one lint policy in `[workspace.lints]` of `Cargo.toml`.

The harness settings `startup_timeout` and `command_timeout` default to `2m`; `build_timeout`
defaults to `30m` for each Bazel invocation, including shutdown. `handshake_timeout` defaults to
`5s` and bounds connection setup.

## Deploying

Gang is deployed from a separate repository that calls its modules and reusable workflow at one release tag. This repository holds no deployment.

The deployment module defaults to `database.engine = "sqlite"` for one controller. Choose `postgres` or `dynamodb` for one or several controllers behind load balancers; shared engines require a shared S3 cache. See the [configuration reference](docs/SPECIFICATION.md) for the settings.

```hcl
module "gang" {
  source = "git::https://github.com/example-org/gang//infra/terraform/modules/deployment?ref=v0.3.1"
  # variables as in the example root
}
```

Call `.github/workflows/deploy.yml` as its header comment shows, pinned to the same tag, with `gang_sha256` set to the release's checksum of the binary for the deploy runner (the `gang-x86_64-unknown-linux-musl` line of the release's `SHA256SUMS` for the default x86 runner). To use ARM, set `runner: ubuntu-24.04-arm` and pin the checksum of `gang-aarch64-unknown-linux-musl`. The workflow downloads that binary and refuses any other; it never compiles Gang.

The apply job uses the `production` GitHub environment by default. Set `apply_environment` to a different name to use that environment, or explicitly to `''` to run without one. Without an environment there are no environment protection rules or environment secrets; the apply role's OIDC trust must instead match the `main` branch subject. See the deployment contract for the trust policy.

Every `v*` tag has a GitHub release with `gang-x86_64-unknown-linux-musl`, `gang-aarch64-unknown-linux-musl`, `gang-aarch64-apple-darwin`, disk images for x86_64 and aarch64, and `SHA256SUMS`. Public AMIs of each architecture in every image region are listed in `gang-amis.json` (specification section 7.2). Check downloaded assets with `sha256sum --check --ignore-missing SHA256SUMS`, and pin the binary checksum where you deploy it.

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

## Troubleshooting

A build fails with `Action ... is missing from CAS`, or uploads go to a cache other than Gang's. Run `gang doctor` in the workspace, after `gang login`. It names the file and section that set each flag it finds wrong.

The usual cause is a `~/.bazelrc`, an imported rc file, or a `--config` that sets `--remote_cache` or `--remote_executor`. Those flags override the workspace's committed Gang flags. Remove them from that file or config.

## License

Gang's source is licensed under the [Apache License 2.0](LICENSE); see [NOTICE](NOTICE).

The `gang` binary links the NativeLink crates, which are licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (FSL-1.1-ALv2). That license permits any use except a Competing Use (making NativeLink available to others in a commercial product or service that substitutes for it), and it converts each release to Apache 2.0 two years after the release ships; for v1.8.3, the version Gang embeds, that is 2028-10-28. [NOTICE](NOTICE) names the license text and every third-party component.
