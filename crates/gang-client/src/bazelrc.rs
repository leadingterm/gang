//! The Bazel rc text that points a workspace's Bazel at the Gang executor, the cache, and the
//! credential helper that mints bearer tokens.

use gang_types::{Url, property};
use gang_wire::{Authentication, Server};
use serde_json::Value;

/// Name of the CA certificate file that `gang bazelrc` writes next to the rc file.
pub const CA_FILE_NAME: &str = "gang-ca.pem";

/// The comment that opens every generated file.
const HEADER: &str = "# Written by `gang bazelrc`. Do not edit.";

/// Renders the Bazel rc text for `info`. `host` is the host the credential helper serves
/// and `helper` is the command Bazel runs for it; a server without token authentication gets no
/// helper line, and a server without a CA certificate gets no certificate line.
pub(crate) fn render(info: &Server<Value>, host: &str, helper: &str) -> String {
    let mut lines = vec![HEADER.to_owned()];
    lines.push(format!(
        "build --remote_executor={}",
        quote(display_url(&info.remote_executor))
    ));
    lines.push(format!(
        "build --remote_cache={}",
        quote(display_url(&info.remote_cache))
    ));
    if matches!(info.authentication, Authentication::Token(_)) {
        lines.push(format!(
            "build --credential_helper={}",
            quote(&format!("{host}={helper}"))
        ));
    }
    lines.push(format!(
        "build --remote_default_exec_properties={}",
        quote(&format!("{}={}", property::ISA, info.default_isa))
    ));
    if info.ca_certificate.is_some() {
        lines.push(format!("build --tls_certificate={CA_FILE_NAME}"));
    }
    lines.join("\n") + "\n"
}

/// The URL as text, without the trailing `/` of a bare root path; other paths are kept verbatim.
pub(crate) fn display_url(url: &Url) -> &str {
    let text = url.as_str();
    if url.path() == "/" && url.query().is_none() && url.fragment().is_none() {
        text.strip_suffix('/').unwrap_or(text)
    } else {
        text
    }
}

/// Writes `value` bare when it holds only safe characters, otherwise in double quotes with `\` and
/// `"` escaped, so that Bazel's rc tokenizer reads it back unchanged.
fn quote(value: &str) -> String {
    if value.chars().all(is_bare) {
        return value.to_owned();
    }
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for c in value.chars() {
        if c == '\\' || c == '"' {
            quoted.push('\\');
        }
        quoted.push(c);
    }
    quoted.push('"');
    quoted
}

fn is_bare(c: char) -> bool {
    c.is_ascii_alphanumeric() || "_./:@%+=,-".contains(c)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gang_types::{CertificatePem, Url};
    use gang_wire::{Isa, NoAuthentication, TokenAuthentication, name};

    use super::*;

    const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----";

    fn info(authentication: Authentication<Value>, ca_certificate: Option<&str>) -> Server<Value> {
        Server {
            name: name::SERVER.to_owned(),
            remote_executor: Url::parse("grpcs://exec.example.test:443").expect("valid url"),
            remote_cache: Url::parse("grpcs://cache.example.test:443/").expect("valid url"),
            ca_certificate: ca_certificate
                .map(|text| CertificatePem::try_new(text).expect("valid certificate")),
            default_isa: Isa::X86_64,
            authentication,
        }
    }

    fn token() -> Authentication<Value> {
        Authentication::Token(TokenAuthentication {
            login: BTreeMap::new(),
            device_login: None,
        })
    }

    #[test]
    fn display_url_preserves_paths_queries_and_fragments() {
        for (input, expected) in [
            ("https://h/", "https://h"),
            ("https://h:8443/", "https://h:8443"),
            ("https://h/p/", "https://h/p/"),
            ("https://h/?q=value", "https://h/?q=value"),
            ("https://h/?q=/", "https://h/?q=/"),
            ("https://h/#fragment/", "https://h/#fragment/"),
        ] {
            let url = Url::parse(input).expect("valid url");
            assert_eq!(display_url(&url), expected);
        }
    }

    #[test]
    fn renders_token_settings_with_ca() {
        let rendered = render(
            &info(token(), Some(CERTIFICATE)),
            "gang.example.test",
            "/usr/local/bin/gang",
        );
        assert_eq!(
            rendered,
            "# Written by `gang bazelrc`. Do not edit.\n\
             build --remote_executor=grpcs://exec.example.test:443\n\
             build --remote_cache=grpcs://cache.example.test:443\n\
             build --credential_helper=gang.example.test=/usr/local/bin/gang\n\
             build --remote_default_exec_properties=ISA=x86-64\n\
             build --tls_certificate=gang-ca.pem\n"
        );
    }

    #[test]
    fn scheme_none_has_no_helper_and_no_ca_without_a_certificate() {
        let rendered = render(
            &info(Authentication::None(NoAuthentication {}), None),
            "gang.example.test",
            "/usr/local/bin/gang",
        );
        assert_eq!(
            rendered,
            "# Written by `gang bazelrc`. Do not edit.\n\
             build --remote_executor=grpcs://exec.example.test:443\n\
             build --remote_cache=grpcs://cache.example.test:443\n\
             build --remote_default_exec_properties=ISA=x86-64\n"
        );
    }

    #[test]
    fn quotes_a_helper_path_with_a_space() {
        let rendered = render(
            &info(token(), None),
            "gang.example.test",
            "/opt/My Tools/gang",
        );
        assert!(
            rendered
                .contains("build --credential_helper=\"gang.example.test=/opt/My Tools/gang\"\n"),
            "{rendered}"
        );
    }
}
