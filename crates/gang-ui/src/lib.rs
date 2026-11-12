//! Serves the embedded dashboard bundle under `/ui`.
//!
//! What is served:
//! - Every file of the dashboard bundle, at `/ui/<path>`.
//! - Unknown extension-less paths under `/ui` fall back to `index.html`, so the
//!   single-page application can handle its own client-side routes.
//! - Every response carries a strict content security policy and related
//!   hardening headers.
//! - Content-hashed assets are cached as immutable; `index.html` and the
//!   fallback are never cached.
//! - Conditional GET: a matching `If-None-Match` yields `304 Not Modified`.
//! - Content negotiation: brotli, then gzip, then identity, per the request's
//!   `Accept-Encoding`. Precompressed variants are embedded; nothing is
//!   compressed at runtime.
//!
//! How assets get here: the bundle is built by the dashboard `dist` target.
//! The `gang-embed` tool walks that tree at build time, hashes each file, and
//! generates the `assets` module of this crate, which references the identity
//! file and its `.br` and `.gz` siblings.
//!
//! Bazel only: the `assets` module is generated at build time by `gang-embed`
//! from the `//ui:dist` bundle. It does not exist in the source tree, so Cargo
//! cannot compile this crate; only Bazel builds it.
//!
//! ```
//! use http::{HeaderMap, Method, StatusCode};
//!
//! assert!(gang_ui::lookup("missing.js").is_none());
//! let response = gang_ui::respond(&Method::GET, "/ui/missing.js", &HeaderMap::new());
//! assert_eq!(response.status(), StatusCode::NOT_FOUND);
//! ```

mod asset;
mod assets;
mod conditional;
mod headers;
mod negotiate;
mod respond;
mod route;
mod table;

pub use asset::Asset;
pub use headers::SECURITY_HEADERS;
pub use respond::respond;
pub use table::lookup;
