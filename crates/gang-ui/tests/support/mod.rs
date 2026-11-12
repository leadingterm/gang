//! Shared helpers for the gang-ui integration tests.

use bytes::Bytes;
use gang_ui::{Asset, SECURITY_HEADERS, lookup, respond};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Response};

const GET: &str = "GET";
const INDEX_PATH: &str = "index.html";
const UI_PREFIX: &str = "/ui/";
const ASSETS_DIR: &str = "assets/";
const DOT_SLASH: &str = "./";
const SCRIPT_EXT: &str = ".js";
const STYLE_EXT: &str = ".css";
const FONT_EXT: &str = ".woff2";
const SCRIPT_ATTR: &str = " src=\"";
const STYLE_ATTR: &str = " href=\"";
const URL_OPEN: &str = "url(";
const QUOTES: [char; 2] = ['"', '\''];

/// Sends a request with the given method, full path and headers through `respond`.
pub fn send(method: &str, path: &str, headers: &[(&str, &str)]) -> Response<Bytes> {
    let method = Method::from_bytes(method.as_bytes()).expect("valid http method");
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes()).expect("valid header name");
        let value = HeaderValue::from_str(value).expect("valid header value");
        map.append(name, value);
    }
    respond(&method, path, &map)
}

/// Sends a GET request with the given full path and headers.
pub fn get(path: &str, headers: &[(&str, &str)]) -> Response<Bytes> {
    send(GET, path, headers)
}

/// Returns the first value of the named header as text, if present and valid.
pub fn header<'a>(response: &'a Response<Bytes>, name: &str) -> Option<&'a str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

/// Returns the embedded `index.html` asset.
pub fn index() -> &'static Asset {
    lookup(INDEX_PATH).expect("index.html is embedded")
}

/// Returns a hashed, brotli- and gzip-encoded script referenced by `index.html`.
pub fn script() -> &'static Asset {
    referenced()
        .into_iter()
        .filter(|path| path.starts_with(ASSETS_DIR) && path.ends_with(SCRIPT_EXT))
        .filter_map(lookup)
        .find(|asset| asset.br.is_some() && asset.gz.is_some())
        .expect("index.html references a script with brotli and gzip variants")
}

/// Returns a woff2 font referenced from a stylesheet referenced by `index.html`.
pub fn font() -> &'static Asset {
    let target = stylesheets()
        .into_iter()
        .find_map(|asset| woff2_target(text(asset)))
        .expect("a stylesheet references a woff2 font");
    lookup(&table_path(target)).expect("referenced woff2 font is embedded")
}

/// Returns the table paths referenced from `index.html`, without the `/ui/` prefix.
pub fn referenced() -> Vec<&'static str> {
    let html = text(index());
    [SCRIPT_ATTR, STYLE_ATTR]
        .into_iter()
        .flat_map(|attr| attribute_values(html, attr))
        .filter_map(|value| value.strip_prefix(UI_PREFIX))
        .collect()
}

/// Asserts that a response carries all six security headers with their exact values.
pub fn assert_secure(response: &Response<Bytes>) {
    for (name, value) in SECURITY_HEADERS {
        assert_eq!(
            header(response, name),
            Some(value),
            "security header {name} on status {}",
            response.status()
        );
    }
}

fn text(asset: &'static Asset) -> &'static str {
    std::str::from_utf8(asset.identity).expect("embedded asset is utf-8")
}

fn stylesheets() -> Vec<&'static Asset> {
    referenced()
        .into_iter()
        .filter(|path| path.starts_with(ASSETS_DIR) && path.ends_with(STYLE_EXT))
        .filter_map(lookup)
        .collect()
}

fn attribute_values(html: &'static str, attr: &str) -> Vec<&'static str> {
    html.split(attr)
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .collect()
}

fn woff2_target(css: &'static str) -> Option<&'static str> {
    css.split(URL_OPEN)
        .skip(1)
        .filter_map(|rest| rest.split(')').next())
        .map(|raw| raw.trim().trim_matches(QUOTES))
        .find(|target| target.ends_with(FONT_EXT))
}

fn table_path(target: &str) -> String {
    match target.rfind(ASSETS_DIR) {
        Some(at) => target[at..].to_owned(),
        None => format!("{ASSETS_DIR}{}", target.trim_start_matches(DOT_SLASH)),
    }
}
