# Gang

Gang runs Bazel remote execution on workers that exist only while there is work. One `gang` binary embeds the NativeLink scheduler as a library, authenticates callers with the plugins chosen in `gang.toml` (a short-lived bearer token for people and CI, mutual TLS for workers), reconciles named worker fleets from queued demand, and runs every action in a sandbox. Every backend (provisioner, cache, signer, scheduler, database, sandbox, identity provider, client and worker authentication, action limits) is a plugin composed at compile time and chosen by writing its section in `gang.toml`.

## Commands

`gang` is the only artifact; laptops, CI, the controller and workers run the same build. Clients use `login`, `get`, `bazelrc`, `doctor`, `revoke` and `restore`. The controller and workers use `server`, `worker` and the hidden `exec`. Local Mode uses `local up` and `local down`. `options` lists each provisioner's fleet options. `gang --help` lists the commands people run; `get`, `worker` and `exec` are hidden from it. [`docs/SPECIFICATION.md`](docs/SPECIFICATION.md) §6 specifies them all.

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

## Quick start: Local Mode

Local Mode runs the controller and on-demand workers on one machine from `configs/gang.local.toml` (specification section 1.4). Actions run under `bwrap` on Linux. On macOS, set the fleet's `sandbox = "noop"` in that file first, because bubblewrap is Linux-only. Install the binary first: Bazel runs `gang` as its credential helper, and `gang bazelrc` and `gang doctor` act on the current directory.

```sh
nix develop -c bazel build -c opt //:gang && install bazel-bin/crates/gang-main/gang ~/.local/bin/gang
gang local up --insecure          # from this checkout; loopback only, plaintext, no authentication; prints the endpoints and the next step
cd <your Bazel workspace>
gang bazelrc --server http://127.0.0.1:50051 > .bazelrc
gang doctor --server http://127.0.0.1:50051
nix develop <path-to-gang-checkout> -c bazel build //...  # workers start while actions are queued and stop when idle
gang local down                   # from this checkout
```

Against a deployment, sign in first. `gang doctor` then takes the server from the stored login.

```sh
gang login --server https://gang.example.com
gang bazelrc --server https://gang.example.com > .bazelrc
gang doctor
```

TLS Local Mode (`gang local up` without `--insecure`) accepts client tokens only from the providers in `[authentication.clients.token] providers`. Add an interactive provider (specification section 1.3) before you `gang login` to it. Run the end-to-end harness from this checkout with `nix develop -c bazel run //crates/gang-e2e:local -- --gate 1` (specification section 6.5). The `gang` binary and pinned Bazel come from the harness's runfiles; `--gang` and `--bazel` override them.

The harness settings `startup_timeout` and `command_timeout` default to `2m`; `build_timeout`
defaults to `30m` for each Bazel invocation, including shutdown. `handshake_timeout` defaults to
`5s` and bounds connection setup.

## Deploying

Gang is deployed from a separate repository that calls its modules and reusable workflow at one release tag. This repository holds no deployment.

```hcl
module "gang" {
  source = "git::https://github.com/example-org/gang//infra/terraform/modules/deployment?ref=v0.1.5"
  # variables as in the example root
}
```

Call `.github/workflows/deploy.yml` as its header comment shows, pinned to the same tag, with `gang_sha256` set to the release's checksum of the binary for the deploy runner (the `gang-x86_64-unknown-linux-musl` line of the release's `SHA256SUMS` for the default runner). The workflow downloads that binary and refuses any other; it never compiles Gang.

Every `v*` tag has a GitHub release with `gang-x86_64-unknown-linux-musl`, `gang-aarch64-unknown-linux-musl`, `gang-aarch64-apple-darwin` and `SHA256SUMS` (specification section 7.2). When the repository variable `IMAGE_DESTINATION` is set, the release also stores the controller and worker disk images for x86_64 and aarch64 there, listed in the same `SHA256SUMS` (specification section 7.2). Check a downloaded binary with `sha256sum --check --ignore-missing SHA256SUMS`, and pin its checksum where you deploy it.

See section 8 of [`docs/SPECIFICATION.md`](docs/SPECIFICATION.md) for the deployment contract, and the example root [`infra/terraform/environments/example`](infra/terraform/environments/example).

## Troubleshooting

A build fails with `Action ... is missing from CAS`, or uploads go to a cache other than Gang's. Run `gang doctor` in the workspace, after `gang login`. It names the file and section that set each flag it finds wrong.

The usual cause is a `~/.bazelrc`, an imported rc file, or a `--config` that sets `--remote_cache` or `--remote_executor`. Those flags override the workspace's committed Gang flags. Remove them from that file or config.

## License

Gang's source is licensed under the [Apache License 2.0](LICENSE); see [NOTICE](NOTICE).

The `gang` binary links the NativeLink crates, which are licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (FSL-1.1-ALv2). That license permits any use except a Competing Use (making NativeLink available to others in a commercial product or service that substitutes for it), and it converts each release to Apache 2.0 two years after the release ships; for v1.8.3, the version Gang embeds, that is 2028-10-28. [NOTICE](NOTICE) names the license text and every third-party component.
