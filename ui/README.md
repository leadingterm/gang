# Dashboard UI

The browser dashboard of ADR-31: Preact, `@preact/signals` and TypeScript, bundled by esbuild into a
hashed, precompressed bundle that the server embeds and serves under `/ui`. It reads the HTTP API
(`docs/API.md`): `GET /v1/session`, the `GET /v1/state:watch` stream, `GET /v1/principals`,
`GET /v1/roles` and `GET /v1/events`. Only sign-in and sign-out (`/ui/login`, `/ui/logout`) are `/ui` routes.

## Layout

| Path | Holds |
| --- | --- |
| `src/wire.ts` | The wire format: state, deltas, ticks, `closed`, session, principals, roles, events |
| `src/store.ts` | The keyed reducer: a snapshot replaced by deltas; a foreign `base` asks for a resync |
| `src/history.ts` | What the browser keeps from frames it saw: phase lanes, pass outages, the done rate |
| `src/client.ts` | The one client of the state API: the SSE stream, the server clock, failure handling |
| `src/views/` | The screens: Overview, Actions, Workers, Activity, Access, sign-in and errors |
| `fixtures/` | Recorded streams (`*.sse`, each frame after an `: at <ms>` send time) and JSON bodies |
| `tools/build.mjs` | The bundle build |
| `tools/preview.mjs` | A preview server for the bundle against the fixtures |
| `headers.json` | The security headers `gang-ui` sends on `/ui`, printed by `gang-bindings headers`; `//ui:headers_json_test` fails when the checked-in copy differs from production, and the preview server sends them |
| `tsconfig.json`, `tsconfig.test.json` | `tsc --strict` settings: `src` is checked against DOM types only, the tests add Node's |

## Embedding

The server embeds `//ui:dist` at build time. `crates/gang-embed`, a Bazel-run `rust_binary` with no
compression dependency, hashes each identity file for its `ETag`, attaches the `.gz` and `.br` files
that `//ui:dist` ships beside it (fonts stay uncompressed), and generates `crates/gang-ui/src/assets.rs`:
the `Asset` table, with `include_bytes!` paths relative to the generated file.

`gang-ui` serves the table under `/ui` with strict security headers (CSP, `nosniff`, `no-referrer`,
COOP, CORP, `Permissions-Policy`), `Accept-Encoding` negotiation, `ETag`/`If-None-Match` (304), `HEAD`,
immutable caching for hashed `assets/*`, and `no-store` for `index.html` and the single-page fallback
(paths without an extension). Missing files with an extension, and paths containing `..` or `\`, get 404.
`crates/gang-server` mounts it through `Gateway`/`Assets`. The header table is `gang_ui::SECURITY_HEADERS`; `ui/headers.json` is generated from it.

`gang-ui` and everything that depends on it build with Bazel only: Cargo cannot produce the generated
`assets.rs`. `bazel test //crates/gang-ui/...` includes a test that the embedded table equals `//ui:dist`.

## Build, test, preview

```
bazel build //ui:dist        # bazel-bin/ui/dist: index.html and assets/*, text files with .gz and .br
bazel test //ui/...          # tsc --strict, store, history, client, formatter and view tests, bundle budgets, headers
bazel run //ui:preview       # http://127.0.0.1:8412/ui/
```

The bundler runs for the execution platform even when Bazel cross-builds the server; the UI bundle
does not depend on the server's target platform.

`test/views.snapshot` holds every screen rendered per scenario with `preact-render-to-string`. After
an intended change, refresh it from `ui/` with
`node tools/bundle-test.mjs test/views.test.tsx test-bundle/views.test.mjs && node --test --test-update-snapshots test-bundle/views.test.mjs`.

Add `?scenario=` to a preview page to pick what the fixtures serve: `live` (default), `stale` (the
control pass is skipped), `lagging` (the action sample is older than its threshold), `empty`, `cold`
(scaling from zero), `disconnected` (the stream drops and stays down), `forbidden` (signed in without
`state.read`) or `signedout`.

Budgets (ADR-31): JavaScript at most 60 KB gzip, CSS at most 15 KB gzip; `//ui:budget_test` enforces
them.

## Content Security Policy

The bundle runs under ADR-30's policy: no inline script or style. Dynamic geometry goes through CSS
custom properties set with CSSOM (`style.setProperty`), never a `style` attribute string.

## Fonts

`fonts/` holds Atkinson Hyperlegible Next and Atkinson Hyperlegible Mono, Latin subsets, under the SIL
Open Font License 1.1 (`fonts/OFL-sans.txt`, `fonts/OFL-mono.txt`).
