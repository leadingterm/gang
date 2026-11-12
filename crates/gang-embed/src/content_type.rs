//! Maps bundle file paths to their `Content-Type` values by extension.

use std::path::Path;

const HTML: &str = "text/html; charset=utf-8";
const JAVASCRIPT: &str = "text/javascript; charset=utf-8";
const CSS: &str = "text/css; charset=utf-8";
const WOFF2: &str = "font/woff2";
const SVG: &str = "image/svg+xml";
const JSON: &str = "application/json";
const OCTET_STREAM: &str = "application/octet-stream";

/// Returns the `Content-Type` for a relative bundle path.
///
/// The extension is the last `.`-suffix of the final path segment and is
/// matched case-sensitively. Unknown extensions, a missing extension, and
/// dotted directory names all map to `application/octet-stream`.
#[must_use]
pub fn content_type_of(path: &str) -> &'static str {
    match Path::new(path).extension().and_then(|ext| ext.to_str()) {
        Some("html") => HTML,
        Some("js") => JAVASCRIPT,
        Some("css") => CSS,
        Some("woff2") => WOFF2,
        Some("svg") => SVG,
        Some("json" | "map") => JSON,
        _ => OCTET_STREAM,
    }
}

#[cfg(test)]
mod tests {
    use super::content_type_of;

    #[test]
    fn maps_every_known_extension() {
        let cases = [
            ("index.html", "text/html; charset=utf-8"),
            ("assets/main-0123.js", "text/javascript; charset=utf-8"),
            ("assets/main-0123.css", "text/css; charset=utf-8"),
            ("assets/inter.woff2", "font/woff2"),
            ("assets/logo.svg", "image/svg+xml"),
            ("assets/data.json", "application/json"),
            ("assets/main-0123.js.map", "application/json"),
        ];
        for (path, expected) in cases {
            assert_eq!(content_type_of(path), expected, "content type of {path:?}");
        }
    }

    #[test]
    fn unknown_extension_is_octet_stream() {
        assert_eq!(
            content_type_of("assets/image.png"),
            "application/octet-stream",
            "unknown extension"
        );
    }

    #[test]
    fn missing_extension_is_octet_stream() {
        assert_eq!(
            content_type_of("assets/LICENSE"),
            "application/octet-stream",
            "no extension"
        );
    }

    #[test]
    fn dotted_directory_is_octet_stream() {
        assert_eq!(
            content_type_of("a.b/c"),
            "application/octet-stream",
            "extension on a directory segment must not count"
        );
    }

    #[test]
    fn uppercase_extension_is_octet_stream() {
        assert_eq!(
            content_type_of("X.JS"),
            "application/octet-stream",
            "extension matching is case-sensitive"
        );
    }
}
