# ADR-30: Dashboard assets are embedded and precompressed at build time

Status: Accepted

## Context
- The dashboard must ship inside the one `gang` binary, built hermetically, behind a strict CSP with no CDN.

## Decision
- **Bazel builds and compresses.** `//ui:dist` ships each text asset with `.gz` and `.br` beside it. `gang-embed` (a Rust build tool run by Bazel) only embeds: it hashes the identity bytes (sha256) for the `ETag`, attaches the variants and emits the asset table of `gang-ui`. There is no runtime compression and no allocation beyond the response.
- **Serving.** A `Gateway` is the public listener's service: `/ui` and `/ui/**` go to the browser routes, then to the embedded assets; everything else goes to the API. Static responses are `GET` and `HEAD` only, negotiate `Accept-Encoding`, answer `If-None-Match` with 304, mark content-hashed assets immutable and everything else `no-store`, fall back to the shell for extension-less paths, and answer 404 for traversal.
- **Strict CSP and headers on every static response:** same-origin everything, no inline script or style, `frame-ancestors 'none'`, plus `nosniff`, no referrer, same-origin COOP and CORP. HSTS is left to the proxy. There are no CDN or external fonts.

## Consequences
- Cargo cannot build `gang-ui` without Bazel output; CI is Bazel-only (accepted).
- Dynamic styling uses CSSOM and CSS custom properties, never inline style.
- The full header list and routing details are in SPEC §5.2.

## Rejected alternatives
- **Runtime or stream compression.**
- **Compression inside `gang-embed`:** it would need a compression dependency in the build tool.
