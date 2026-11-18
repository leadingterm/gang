//! The Bazel rc text that points a workspace's Bazel at the Gang executor, the cache, and the
//! credential helper that mints bearer tokens.

use gang_types::{Url, property};
use gang_wire::{Authentication, Server};
use serde_json::Value;

/// Name of the CA certificate file that `gang bazelrc` writes next to the rc file.
pub const CA_FILE_NAME: &str = "gang-ca.pem";

/// A controller hand-off or crash take-over can interrupt connections for the lease TTL (10 s),
/// clock skew (2 s), renew interval (3 s), and NLB health window (about 10 s): 25–30 s.
/// Bazel's 12 retries, doubling from 100 ms up to 5 s, span about
/// 0.1 + 0.2 + 0.4 + 0.8 + 1.6 + 3.2 + 5 × 6 = 36 s, longer than that gap.
pub const REMOTE_RETRIES: u32 = 12;

/// The command responsible for writing a Bazel rc file.
#[derive(Debug, Clone, Copy)]
pub enum BazelrcWriter {
    /// The `gang bazelrc` command.
    Bazelrc,
    /// The `gang setup` command.
    Setup,
}

impl BazelrcWriter {
    /// The command to name in the generated file.
    const fn command(self) -> &'static str {
        match self {
            Self::Bazelrc => "gang bazelrc",
            Self::Setup => "gang setup",
        }
    }
}

/// Renders the Bazel rc text for `info`, identifying `writer` as the command that generated it.
/// `host` is the host the credential helper serves and `helper` is the command Bazel runs for it.
/// The helper comment advises running `writer` again if gang moves. A server without token
/// authentication gets no helper line, and a server without a CA certificate gets no certificate
/// line. `certificate` is the certificate path used by Bazel.
#[must_use]
pub fn render(
    info: &Server<Value>,
    host: &str,
    helper: &str,
    certificate: &str,
    writer: BazelrcWriter,
) -> String {
    let writer = writer.command();
    let mut lines = vec![format!("# Written by `{writer}`. Do not edit.")];
    lines.push(format!(
        "build --remote_executor={}",
        quote(display_url(&info.remote_executor))
    ));
    lines.push(format!(
        "build --remote_cache={}",
        quote(display_url(&info.remote_cache))
    ));
    lines.push(format!("build --remote_retries={REMOTE_RETRIES}"));
    lines.push(format!(
        "build --remote_retry_max_delay={}s",
        gang_types::health::CLIENT_RETRY_MAX_DELAY.as_secs()
    ));
    // Bazel's Execute stream has no deadline, so keepalive drops a frozen controller (ADR-51).
    lines.push(format!(
        "build --grpc_keepalive_time={}s",
        gang_types::health::CLIENT_KEEPALIVE_TIME.as_secs()
    ));
    lines.push(format!(
        "build --grpc_keepalive_timeout={}s",
        gang_types::health::CLIENT_KEEPALIVE_TIMEOUT.as_secs()
    ));
    if matches!(info.authentication, Authentication::Token(_)) {
        lines.push(format!(
            "# The gang that Bazel runs for credentials; run `{writer}` again if it moves."
        ));
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
        lines.push(format!("build --tls_certificate={}", quote(certificate)));
    }
    lines.join("\n") + "\n"
}

/// The URL as text, without the trailing `/` of a bare root path; other paths are kept verbatim.
#[must_use]
pub fn display_url(url: &Url) -> &str {
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
            version: None,
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
            CA_FILE_NAME,
            BazelrcWriter::Bazelrc,
        );
        assert!(
            rendered
                .lines()
                .next()
                .is_some_and(|line| line.contains("gang bazelrc"))
        );
        assert!(rendered.lines().any(|line| line.starts_with('#')
            && line.contains("gang bazelrc")
            && line.contains("credentials")));
        for line in [
            "build --remote_executor=grpcs://exec.example.test:443",
            "build --remote_cache=grpcs://cache.example.test:443",
            "build --credential_helper=gang.example.test=/usr/local/bin/gang",
            "build --remote_default_exec_properties=ISA=x86-64",
            "build --tls_certificate=gang-ca.pem",
        ] {
            assert!(
                rendered.lines().any(|actual| actual == line),
                "missing {line}"
            );
        }
    }

    #[test]
    fn both_writers_render_the_remote_retry_budget_after_the_cache() {
        for writer in [BazelrcWriter::Bazelrc, BazelrcWriter::Setup] {
            let rendered = render(&info(token(), None), "host", "gang", CA_FILE_NAME, writer);
            assert!(rendered.contains(
                "build --remote_cache=grpcs://cache.example.test:443\n\
                 build --remote_retries=12\n\
                 build --remote_retry_max_delay=5s\n"
            ));
        }
    }

    #[test]
    fn both_writers_render_keepalive_from_health_constants() {
        for writer in [BazelrcWriter::Bazelrc, BazelrcWriter::Setup] {
            let rendered = render(&info(token(), None), "host", "gang", CA_FILE_NAME, writer);
            for line in [
                format!(
                    "build --grpc_keepalive_time={}s",
                    gang_types::health::CLIENT_KEEPALIVE_TIME.as_secs()
                ),
                format!(
                    "build --grpc_keepalive_timeout={}s",
                    gang_types::health::CLIENT_KEEPALIVE_TIMEOUT.as_secs()
                ),
            ] {
                assert!(
                    rendered.lines().any(|actual| actual == line),
                    "missing {line}"
                );
            }
        }
    }

    /// Setup output identifies setup in both generated comments.
    #[test]
    fn setup_names_its_writer_in_header_and_helper_comment() {
        let rendered = render(
            &info(token(), None),
            "host",
            "gang",
            CA_FILE_NAME,
            BazelrcWriter::Setup,
        );
        assert!(
            rendered
                .lines()
                .next()
                .is_some_and(|line| line.contains("gang setup")),
            "setup header must name gang setup"
        );
        assert!(
            rendered.lines().any(|line| line.starts_with('#')
                && line.contains("credentials")
                && line.contains("gang setup")),
            "setup helper comment must name gang setup"
        );
    }

    #[test]
    fn scheme_none_has_no_helper_and_no_ca_without_a_certificate() {
        let rendered = render(
            &info(Authentication::None(NoAuthentication {}), None),
            "gang.example.test",
            "/usr/local/bin/gang",
            CA_FILE_NAME,
            BazelrcWriter::Bazelrc,
        );
        assert!(
            rendered
                .lines()
                .next()
                .is_some_and(|line| line.contains("gang bazelrc"))
        );
        assert!(rendered.contains("build --remote_executor=grpcs://exec.example.test:443\n"));
        assert!(rendered.contains("build --remote_cache=grpcs://cache.example.test:443\n"));
        assert!(rendered.contains("build --remote_default_exec_properties=ISA=x86-64\n"));
        assert!(!rendered.contains("--credential_helper"));
        assert!(!rendered.contains("--tls_certificate"));
    }

    #[test]
    fn a_server_without_login_gets_no_helper_comment() {
        let rendered = render(
            &info(Authentication::None(NoAuthentication {}), None),
            "gang.example.test",
            "/usr/local/bin/gang",
            CA_FILE_NAME,
            BazelrcWriter::Bazelrc,
        );
        assert!(
            !rendered
                .lines()
                .any(|line| line.starts_with('#') && line.contains("credentials"))
        );
        assert!(!rendered.contains("--credential_helper"));
    }

    #[test]
    fn quotes_a_helper_path_with_a_space() {
        let rendered = render(
            &info(token(), None),
            "gang.example.test",
            "/opt/My Tools/gang",
            CA_FILE_NAME,
            BazelrcWriter::Bazelrc,
        );
        assert!(
            rendered
                .contains("build --credential_helper=\"gang.example.test=/opt/My Tools/gang\"\n")
        );
    }

    #[test]
    fn quotes_absolute_certificate_path() {
        let rendered = render(
            &info(token(), Some(CERTIFICATE)),
            "host",
            "gang",
            "/workspace with spaces/.gang/ca.pem",
            BazelrcWriter::Bazelrc,
        );
        assert!(
            rendered.contains("build --tls_certificate=\"/workspace with spaces/.gang/ca.pem\"\n")
        );
    }
}
