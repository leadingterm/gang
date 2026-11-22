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
| `src/client.ts` | The one client of the state API: the SSE stream, the server clock, failure handling |
| `src/views/` | The screens: Overview, Actions, Workers, Activity, Access, sign-in and errors |
| `fixtures/` | Recorded streams (`*.sse`, each frame after an `: at <ms>` send time) and JSON bodies |
| `tools/build.mjs` | The bundle build |
| `tools/preview.mjs` | A preview server for the bundle against the fixtures |
| `tools/preview-server.mjs` | Fixture-backed preview server shared by the CLI and layout test |
| `tools/chromium.mjs` | DevTools driver for the layout test |
| `test/layout.test.mjs` | Browser layout regression test |
| `headers.json` | The security headers `gang-ui` sends on `/ui`, printed by `gang-bindings headers`; `//ui:headers_json_test` fails when the checked-in copy differs from production, and the preview server sends them |
| `tsconfig.json`, `tsconfig.test.json` | `tsc --strict` settings: `src` is checked against DOM types only, the tests add Node's |

`StateClient` owns one session admission at a time. Re-admission retires its previous renewal even
without a stop; concurrent renewals within the same admission join one flight, and an older response
cannot change the new admission. Hidden tabs suspend the stream while retaining rows and renewing the
session; returning visible opens a fresh stream and snapshot without a lost banner. Genuine visible
stream failures still show the lost banner and retry with backoff.

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
bazel test //ui:layout_test  # every screen in the devShell's pinned Chromium
bazel test //ui:overview_layout_test # Overview tile layout in pinned Chromium
bazel run //ui:preview       # http://127.0.0.1:8412/ui/
```

The layout test renders every screen from the fixtures and fails when an element leaves the document, two side-by-side boxes overlap, or an identifier or time wraps. The layout and driver tests need the Linux devShell's Chromium; on macOS `bazel test //ui/...` skips them, and naming them explicitly reports them incompatible.
It uses a settled layout snapshot and batches identifier-head font reads.

Browser shutdown terminates the browser process group and waits for inherited stderr to close before
the test removes the profile.

To opt in to screenshots, run `bazel test //ui:layout_test --test_env=GANG_LAYOUT_SCREENSHOTS=1`; it writes `<page>-<width>.png` to the test's undeclared outputs.

The bundler runs for the execution platform even when Bazel cross-builds the server; the UI bundle
does not depend on the server's target platform.

Add `?scenario=` to a preview page to pick what the fixtures serve: `live` (default), `stale` (the
control pass is skipped), `lagging` (the action sample is older than its threshold), `empty`, `isolated`
(a synthetic chart-edge fixture), `cold` (scaling from zero), `disconnected` (the stream drops and stays
down), `forbidden` (signed in without `state.read`), `long-session` (signed in through a long provider
and OIDC subject) or `signedout`.

Budgets (ADR-31): JavaScript at most 60 KB gzip, CSS at most 15 KB gzip; `//ui:budget_test` enforces
them.

## Screen layout

Identifiers truncate in the middle, keeping the part after their last `:` (`Ident`, `splitTail`) and the full value on hover; a Bazel label follows the same rule.
Dates, clocks, and durations use `<time>` via `Day`, `Clock`, `Duration`, `Since`, or `Until` and never wrap; `.line` keeps an identifier with its marks.
`Table` is a grid whose rows use its subgrid: `share` columns take free width, `fit` columns cap identifiers, and value columns fit one-line values.
Tables are laid out as grids, so `Table`, `Row`, and `Cell` set table roles explicitly; at phone width the column heads are visually hidden, not removed.
`.cols` lays columns side by side or wraps them with the aside below the main column. `html { scrollbar-gutter: stable }` prevents page shifts.
The five Overview tiles share label, value, meter, caption, and chart tracks with equal insets.
Row spacing lives inside each tile. Shared `Tiles` encloses the grid with hairlines and makes the last
tile fill an incomplete row at both desktop and two-column phone widths, so any tile count has clean borders.
Action cache shows the lookup-weighted hit rate and lookup count over the server's configured recent window, or `no lookups` when there are none. Its chart plots lookups as a faint area and hits as a solid line on one shared count scale.
The Overview Instances tile has one simple sparkline, matching the other tiles, of total in-service instances summed across fleets over the recent window. Workers holds the per-fleet instance history charts: pending, in-service, terminating and warm, with desired and maximum reference lines. Missing observations remain gaps, not zeros; chart labels are accessible. When no fleets are configured, Workers shows its empty state without the chart toolbar or axis; configured idle fleets still show charts.
Fleet rows show the server's current `launchWarning` only when the stronger `launchFailure` is absent. Each publication reevaluates the latest retained canonical launch outcome against current supply and desired capacity; independent observer supply updates do not replace canonical records with an empty snapshot. A later successful launch or met desired capacity clears the warning. Historical AWS failures stay in the Scaling log and do not themselves establish a current warning.
Charts fit the content width and contain their markers. For complete-step plots, live gauges, fleet history and control-pass strip semantics, see [the dashboard specification](../docs/SPECIFICATION.md#52-live-state-and-dashboard-gang-state-gang-server-gang-ui-gang-embed) and [state API](../docs/API.md#state).
Event icons align with the first line when messages wrap, including when narrow layouts reflow details.
`//ui:layout_test` enforces the screen layout; `//ui:overview_layout_test` covers the Overview tiles.

## Content Security Policy

The bundle runs under ADR-30's policy: no inline script or style. Dynamic geometry goes through CSS
custom properties set with CSSOM (`style.setProperty`), never a `style` attribute string.

## Fonts

`fonts/` holds Atkinson Hyperlegible Next and Atkinson Hyperlegible Mono, Latin subsets, under the SIL
Open Font License 1.1 (`fonts/OFL-sans.txt`, `fonts/OFL-mono.txt`). The layout test's isolated
fontconfig uses pinned DejaVu native fonts declared in the build graph and a profile-local cache.
It checks the loaded app fonts, the fonts actually rendering identifier heads, and a visible sans
text node on each screen. At 320px, both Access and the long-session You screen must leave at least
1px beyond the first head glyph and its ellipsis.
