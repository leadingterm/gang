//! Build tool that embeds a compressed web bundle into Rust source.
//!
//! `gang-embed` walks a dist directory, hashes every identity file, attaches
//! any `.br` / `.gz` sibling variants that exist, and renders a Rust source
//! file defining a static asset table.
//!
//! Generated `include_bytes!` paths are relative to the generated file, so the
//! generated file MUST be written into the same output tree as the dist
//! directory (the two share a root). No environment variables are involved.
//!
//! This crate has no compression dependency: compression is owned by the dist
//! build, and this tool only discovers the variants that already exist.
//!
//! ```
//! assert_eq!(
//!     gang_embed::content_type_of("assets/app.js"),
//!     "text/javascript; charset=utf-8",
//! );
//! ```

mod bundle;
mod content_type;
mod errors;
mod generate;
mod render;

pub use bundle::{Bundle, Entry};
pub use content_type::content_type_of;
pub use errors::EmbedError;
pub use generate::generate;
