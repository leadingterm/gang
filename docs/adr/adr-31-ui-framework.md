# ADR-31: The dashboard UI is Preact, signals and esbuild

Status: Accepted

## Context
- The UI state is a keyed store updated by deltas (ADR-27), so only changed rows should re-render. The build is hermetic (ADR-30), and maintainers must find the code familiar.

## Decision
- **TypeScript, Preact and `@preact/signals`,** bundled by esbuild. Signals re-render only what changed at about 5 KB gzip of runtime, with typed JSX.
- **Budgets:** JavaScript at most 60 KB gzip and CSS at most 15 KB gzip, enforced by `//ui:budget_test`.
- **Build:** `aspect_rules_js` and `rules_ts` with `npm_translate_lock` from an integrity-pinned `ui/pnpm-lock.yaml`. esbuild's JS API runs from one `js_binary` that names each bundle file by the sha256 of its contents (esbuild's own file hash differed between hosts) and precompresses deterministically.

## Consequences
- A JavaScript toolchain sits in the hermetic graph.
- The UI mirrors the wire types in `ui/src/wire.ts` (ADR-35).

## Rejected alternatives
- **Vanilla TypeScript:** hand-written DOM diffing becomes an unmaintained framework.
- **React:** about 45 KB gzip.
- **Svelte or Solid:** a compiler in the hermetic graph and lower contributor familiarity; Solid is the technical runner-up.
- **Lit.**
- **`rules_esbuild` file-name hashing:** differed between hosts.
