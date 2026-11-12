//! The pure checks behind `gang doctor`. Each function turns facts the doctor gathered into one
//! [`Finding`]; none of them talks to the network, to Bazel, or to the clock.

use std::path::Path;

use gang_clock::Timestamp;
use gang_tls::ServerTrust;
use gang_types::{NonZeroDuration, error_chain, is_loopback_url};
use gang_wire::{Authentication, Server};
use serde_json::Value;
use url::Url;

use crate::bazel_options::{
    CREDENTIAL_HELPER, EffectiveOptions, OptionSource, REMOTE_CACHE, REMOTE_EXECUTOR,
    TLS_CERTIFICATE,
};
use crate::bazelrc::display_url;
use crate::doctor_report::{Check, Finding, Status};
use crate::errors::{BazelError, REACH_FIX, TRUST_FIX};
use crate::server_probe::{CredentialFault, ServerFault};

/// The server as the doctor found it: the URL the user gave and the information it answered with.
pub(crate) struct Gang<'a> {
    /// The server URL the user gave.
    pub server: &'a Url,
    /// The server information the server answered with.
    pub info: &'a Server<Value>,
}

/// The two remote endpoints Bazel is pointed at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Endpoint {
    /// Bazel's `--remote_executor`.
    Executor,
    /// Bazel's `--remote_cache`.
    Cache,
}

impl Endpoint {
    /// The check that reports on this endpoint.
    pub(crate) const fn check(self) -> Check {
        match self {
            Self::Executor => Check::RemoteExecutor,
            Self::Cache => Check::RemoteCache,
        }
    }

    /// The Bazel flag that names this endpoint, without its leading dashes.
    pub(crate) const fn flag(self) -> &'static str {
        match self {
            Self::Executor => REMOTE_EXECUTOR,
            Self::Cache => REMOTE_CACHE,
        }
    }

    /// The URL the server serves for this endpoint.
    pub(crate) fn expected(self, info: &Server<Value>) -> &Url {
        match self {
            Self::Executor => &info.remote_executor,
            Self::Cache => &info.remote_cache,
        }
    }
}

/// Appended to the fix of a failing cache row when the executor is Gang's own.
const CAS_HINT: &str = "; actions upload elsewhere and fail with \"missing from CAS\"";

/// Fix shown when the server rejects the URL the user gave.
const WRONG_URL_FIX: &str =
    "use the public URL of the Gang server, the one `gang bazelrc --server` takes";

/// Whether a flag value names `expected`, ignoring one trailing `/`.
fn same_url(value: &str, expected: &str) -> bool {
    value.strip_suffix('/').unwrap_or(value) == expected.strip_suffix('/').unwrap_or(expected)
}

/// Where a Bazel setting came from, as the ` from ...` tail of a summary; empty when unknown.
fn origin_suffix(source: &OptionSource) -> String {
    match source {
        OptionSource::RcFile { path, .. } => format!(" from {}", path.display()),
        OptionSource::Client => " from the Bazel client".to_owned(),
        OptionSource::Unknown => String::new(),
    }
}

/// How to fix a Bazel setting that disagrees with the server, given where it came from.
fn endpoint_fix(flag: &str, source: &OptionSource) -> String {
    match source {
        OptionSource::RcFile { path, section } => {
            format!(
                "remove or change --{flag} in section `{section}` of {}",
                path.display()
            )
        }
        OptionSource::Client => format!("stop passing --{flag} to Bazel"),
        OptionSource::Unknown => format!(
            "stop passing --{flag} on the command line, or find where it is set with `bazel info --announce_rc`"
        ),
    }
}

/// The host part of a credential helper scope, without a leading `scheme://` or a trailing `:port`.
fn scope_host(scope: &str) -> &str {
    let without_scheme = scope.split_once("://").map_or(scope, |(_, rest)| rest);
    match without_scheme.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => without_scheme,
    }
}

/// A credential helper's scope, ordered by its specificity rather than its spelling.
#[derive(Clone, Copy, Eq)]
enum HelperScope<'a> {
    /// An unscoped helper or a bare wildcard.
    Any,
    /// A wildcard matching the suffix itself and its subdomains.
    Suffix(&'a str),
    /// A single host.
    Exact(&'a str),
}

impl<'a> HelperScope<'a> {
    /// Parse an optional scope from an entry.
    fn parse(scope: Option<&'a str>) -> Self {
        match scope.map(scope_host) {
            None | Some("*") => Self::Any,
            Some(name) => match name.strip_prefix("*.") {
                Some(suffix) => Self::Suffix(suffix),
                None => Self::Exact(name),
            },
        }
    }

    /// Return the sort key for Bazel's scope precedence.
    fn specificity(self) -> (u8, usize) {
        match self {
            Self::Any => (0, 0),
            Self::Suffix(suffix) => (1, suffix.len()),
            Self::Exact(_) => (2, 0),
        }
    }

    /// Match the host case-insensitively at a DNS label boundary.
    fn covers(&self, host: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(name) => name.eq_ignore_ascii_case(host),
            Self::Suffix(suffix) => {
                host.eq_ignore_ascii_case(suffix)
                    || host
                        .len()
                        .checked_sub(suffix.len() + 1)
                        .and_then(|start| host.get(start..))
                        .and_then(|tail| tail.strip_prefix('.'))
                        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
            }
        }
    }
}

impl Ord for HelperScope<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.specificity().cmp(&other.specificity())
    }
}

impl PartialOrd for HelperScope<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HelperScope<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

/// The most specific helper covering `host`, preferring the later entry for equal scopes.
///
/// An entry is `[scope=]helper`. An entry without a scope or with scope `*` covers every host.
/// A `*.suffix` scope also covers the suffix itself; host matching ignores ASCII case.
fn covering_helper<'a>(options: &'a EffectiveOptions, host: &str) -> Option<&'a str> {
    options
        .values(CREDENTIAL_HELPER)
        .enumerate()
        .map(|(index, setting)| {
            let (scope, helper) = match setting.value.split_once('=') {
                Some((scope, helper)) => (Some(scope), helper),
                None => (None, setting.value.as_str()),
            };
            (HelperScope::parse(scope), index, helper)
        })
        .filter(|(scope, _, _)| scope.covers(host))
        .max_by_key(|(scope, index, _)| (*scope, *index))
        .map(|(_, _, helper)| helper)
}

/// The helper Bazel runs for the server's cache host; `None` when no `--credential_helper` covers it.
pub(crate) fn helper_for<'a>(gang: &Gang, options: &'a EffectiveOptions) -> Option<&'a str> {
    covering_helper(
        options,
        gang.info.remote_cache.host_str().unwrap_or_default(),
    )
}

/// The finding for a server that answered with Gang's information. The summary adds the executor
/// and the cache only when they are not served from the server's own host.
pub(crate) fn server_reached(gang: &Gang) -> Finding {
    let host = gang.server.host_str();
    let mut deviations = Vec::new();
    if gang.info.remote_executor.host_str() != host {
        deviations.push(format!(
            "executor {}",
            display_url(&gang.info.remote_executor)
        ));
    }
    if gang.info.remote_cache.host_str() != host {
        deviations.push(format!("cache {}", display_url(&gang.info.remote_cache)));
    }
    if matches!(&gang.info.authentication, Authentication::Unrecognized(_)) {
        deviations.push("login scheme unknown".to_owned());
    }
    let mut summary = format!("{} answers", display_url(gang.server));
    if !deviations.is_empty() {
        summary.push_str("; ");
        summary.push_str(&deviations.join(", "));
    }
    Finding::ok(Check::Server, summary)
}

/// The `server` and `tls` findings for a server that did not answer with Gang's information.
pub(crate) fn server_failed(
    server: &Url,
    fault: &ServerFault,
    trust: ServerTrust<'_>,
) -> (Finding, Finding) {
    let name = display_url(server);
    match fault {
        ServerFault::Untrusted(text) => (
            Finding::fail(
                Check::Server,
                format!("{name} did not answer"),
                "see the tls check",
            )
            .with_cause(text.as_str()),
            Finding::fail(
                Check::Tls,
                "the server certificate is not trusted",
                TRUST_FIX,
            )
            .with_cause(text.as_str()),
        ),
        ServerFault::Unreachable(text) => (
            Finding::fail(Check::Server, format!("cannot reach {name}"), REACH_FIX)
                .with_cause(text.as_str()),
            Finding::skip(Check::Tls, "needs the server"),
        ),
        ServerFault::Refused { status, message } => (
            Finding::fail(
                Check::Server,
                format!("{name} answered {status}"),
                WRONG_URL_FIX,
            )
            .with_cause(message.as_str()),
            tls_trusted(server, trust),
        ),
        ServerFault::NotGang(text) => (
            Finding::fail(
                Check::Server,
                format!("{name} is not a Gang server"),
                WRONG_URL_FIX,
            )
            .with_cause(text.as_str()),
            tls_trusted(server, trust),
        ),
    }
}

/// The `tls` finding for the server's URL: trusted over https, acceptable over loopback http,
/// and a warning over any other plaintext http. Over https the summary names the trust that
/// verified the server.
pub(crate) fn tls_trusted(server: &Url, trust: ServerTrust<'_>) -> Finding {
    match server.scheme() {
        "https" => Finding::ok(Check::Tls, https_trust(trust)),
        "http" if is_loopback_url(server) => Finding::ok(Check::Tls, "plaintext on loopback"),
        _ => Finding::warn(
            Check::Tls,
            "plaintext http",
            "serve the Gang server over https; bearer tokens and build data travel unencrypted",
        ),
    }
}

/// The summary of an https server's `tls` row: the trust that verified its certificate.
fn https_trust(trust: ServerTrust<'_>) -> String {
    match trust {
        ServerTrust::Pin(pin) => format!("trusted by the CA pinned as {pin}"),
        ServerTrust::System => "trusted by the system roots".to_owned(),
        ServerTrust::Ca(_) => "trusted by the configured CA certificate".to_owned(),
        ServerTrust::Bundled => "trusted by the bundled web roots".to_owned(),
    }
}

/// The `bazel` finding when Bazel reported its effective options.
pub(crate) fn bazel_reported() -> Finding {
    Finding::ok(Check::Bazel, "Bazel reported its effective flags")
}

/// The `bazel` finding when Bazel's effective options could not be read.
pub(crate) fn bazel_failed(error: &BazelError) -> Finding {
    Finding::fail(
        Check::Bazel,
        "cannot read Bazel's effective flags",
        bazel_fix(error),
    )
    .with_cause(error_chain(error).to_string())
}

/// The fix for a failure to read Bazel's effective options.
fn bazel_fix(error: &BazelError) -> &'static str {
    match error {
        BazelError::Spawn { .. } => "install Bazel, or pass --bazel <program>",
        BazelError::TimedOut { .. } => {
            "run `bazel build --nobuild` in this directory to see what stalls it"
        }
        BazelError::NoCommandLine { .. } | BazelError::Events { .. } => {
            "run `bazel build --nobuild` in this workspace directory and fix what it reports; `gang doctor` must run inside the workspace"
        }
    }
}

/// The finding for one remote endpoint Bazel is pointed at.
///
/// A failing cache row gets an extra hint when the executor is Gang's own, because then uploads
/// go to a cache that the executor does not read.
pub(crate) fn remote_endpoint(
    gang: &Gang,
    options: &EffectiveOptions,
    endpoint: Endpoint,
    executor_is_gang: bool,
) -> Finding {
    if endpoint == Endpoint::Cache
        && let Some(finding) = executor_serves_cache(gang, options)
    {
        return finding;
    }
    let finding = endpoint_finding(gang, options, endpoint);
    if finding.status == Status::Fail && endpoint == Endpoint::Cache && executor_is_gang {
        let fix = finding.fix.map(|fix| format!("{fix}{CAS_HINT}"));
        return Finding { fix, ..finding };
    }
    finding
}

/// The cache row when Bazel serves the cache through `--remote_executor`: `--remote_cache` is
/// absent or empty and `--remote_executor` is set. `None` in every other case.
fn executor_serves_cache(gang: &Gang, options: &EffectiveOptions) -> Option<Finding> {
    let cache_unset = options
        .value(REMOTE_CACHE)
        .is_none_or(|setting| setting.value.is_empty());
    if !cache_unset {
        return None;
    }
    let executor = options
        .value(REMOTE_EXECUTOR)
        .filter(|setting| !setting.value.is_empty())?;
    let expected = display_url(&gang.info.remote_cache);
    if same_url(&executor.value, expected) {
        return Some(Finding::ok(Check::RemoteCache, expected));
    }
    Some(Finding::fail(
        Check::RemoteCache,
        format!("{} from --remote_executor", executor.value),
        format!(
            "run `gang bazelrc --server {}` and put its output in the workspace .bazelrc; Bazel caches through --remote_executor instead of Gang's cache {expected}",
            display_url(gang.server)
        ),
    ))
}

/// The endpoint finding before the cache hint is considered.
fn endpoint_finding(gang: &Gang, options: &EffectiveOptions, endpoint: Endpoint) -> Finding {
    let check = endpoint.check();
    let flag = endpoint.flag();
    let expected = display_url(endpoint.expected(gang.info));
    let Some(setting) = options.value(flag) else {
        return Finding::fail(
            check,
            format!("Bazel sets no --{flag}"),
            format!(
                "run `gang bazelrc --server {}` and put its output in the workspace .bazelrc",
                display_url(gang.server)
            ),
        );
    };
    if same_url(&setting.value, expected) {
        return Finding::ok(check, expected);
    }
    let origin = origin_suffix(&setting.source);
    let summary = if setting.value.is_empty() {
        format!("Bazel disables --{flag}{origin}")
    } else {
        format!("{}{origin}", setting.value)
    };
    Finding::fail(
        check,
        summary,
        format!(
            "{}; Gang's is {expected}",
            endpoint_fix(flag, &setting.source)
        ),
    )
}

/// The `tls_certificate` finding: Bazel must trust the CA certificate when the server has one.
pub(crate) fn tls_certificate(gang: &Gang, options: &EffectiveOptions) -> Finding {
    if gang.info.ca_certificate.is_none() {
        return Finding::ok(Check::TlsCertificate, "none needed; the system roots apply");
    }
    match options.value(TLS_CERTIFICATE) {
        Some(setting) if !setting.value.is_empty() => {
            let name = Path::new(&setting.value)
                .file_name()
                .map_or(setting.value.as_str(), |name| {
                    name.to_str().unwrap_or(setting.value.as_str())
                });
            Finding::ok(Check::TlsCertificate, name)
        }
        _ => Finding::fail(
            Check::TlsCertificate,
            "Bazel has no --tls_certificate",
            format!(
                "run `gang bazelrc --server {}` and keep the gang-ca.pem it writes next to the rc file; the server uses Gang's own CA",
                display_url(gang.server)
            ),
        ),
    }
}

/// The `credential_helper` finding: a `--credential_helper` must cover the server's cache host.
pub(crate) fn credential_helper(gang: &Gang, options: &EffectiveOptions) -> Finding {
    if matches!(&gang.info.authentication, Authentication::None(_)) {
        return Finding::ok(Check::CredentialHelper, "the server needs no login");
    }
    let host = gang.info.remote_cache.host_str().unwrap_or_default();
    match covering_helper(options, host) {
        Some(helper) => Finding::ok(Check::CredentialHelper, format!("{host}={helper}")),
        None => Finding::fail(
            Check::CredentialHelper,
            format!("no --credential_helper covers {host}"),
            format!(
                "run `gang bazelrc --server {}` and put its output in the workspace .bazelrc",
                display_url(gang.server)
            ),
        ),
    }
}

/// The `credentials` finding: the outcome of running the credential helper that Bazel will run.
pub(crate) fn credentials(
    gang: &Gang,
    helper: &str,
    outcome: &Result<(), CredentialFault>,
) -> Finding {
    let cache = display_url(&gang.info.remote_cache);
    let server = display_url(gang.server);
    let by_hand = format!(
        "run `echo '{{\"uri\":\"{cache}\"}}' | {helper} get` to see why; if you are not logged in, run `gang login --server {server}`"
    );
    match outcome {
        Ok(()) => Finding::ok(Check::Credentials, format!("{helper} get returns a token")),
        Err(CredentialFault::NotRunnable(text)) => Finding::fail(
            Check::Credentials,
            format!("cannot run {helper}"),
            "install it, or correct the path in --credential_helper",
        )
        .with_cause(text.as_str()),
        Err(CredentialFault::Failed(status)) => Finding::fail(
            Check::Credentials,
            format!("{helper} get failed"),
            by_hand,
        )
        .with_cause(status.as_str()),
        Err(CredentialFault::TimedOut { seconds }) => Finding::fail(
            Check::Credentials,
            format!("{helper} get did not answer within {seconds}s"),
            by_hand,
        ),
        Err(CredentialFault::BadReply(text)) => Finding::fail(
            Check::Credentials,
            format!("{helper} get printed something Bazel cannot use"),
            "the helper must print a JSON object with `headers` on standard output",
        )
        .with_cause(text.as_str()),
        Err(CredentialFault::Refused { status: _, message }) => Finding::fail(
            Check::Credentials,
            format!("the server refused the token of {helper}"),
            format!(
                "log in again with `gang login --server {server}`; if it still refuses, ask the server's administrator whether your principal was revoked"
            ),
        )
        .with_cause(message.as_str()),
        Err(CredentialFault::Unreachable(text)) => Finding::fail(
            Check::Credentials,
            "cannot present the token to the server",
            "check connectivity to the server",
        )
        .with_cause(text.as_str()),
    }
}

/// The `credentials` finding for a server that needs no login.
pub(crate) fn credentials_not_needed() -> Finding {
    Finding::ok(Check::Credentials, "the server needs no login")
}

/// The skipped `credentials` finding when no credential helper covers the cache host.
pub(crate) fn needs_helper() -> Finding {
    Finding::skip(Check::Credentials, "needs a credential helper")
}

/// The `credentials` finding for a server whose authentication scheme this client does not know.
pub(crate) fn credentials_unsupported() -> Finding {
    Finding::fail(
        Check::Credentials,
        "unknown authentication scheme",
        "upgrade gang to the server's version",
    )
}

/// The `clock` finding: this machine's clock against the server's `Date` header.
///
/// This machine's time at the server's answer is taken as the midpoint of the request.
pub(crate) fn clock(
    server_time: Option<Timestamp>,
    sent_at: Timestamp,
    received_at: Timestamp,
    max_skew: NonZeroDuration,
) -> Finding {
    let Some(server_time) = server_time else {
        return Finding::warn(
            Check::Clock,
            "the server sent no Date header",
            "the clocks were not compared; compare `date -u` here with the server's clock",
        );
    };
    let local = sent_at + received_at.saturating_duration_since(sent_at) / 2;
    let skew = server_time
        .saturating_duration_since(local)
        .max(local.saturating_duration_since(server_time));
    if skew <= max_skew.get() {
        return Finding::ok(
            Check::Clock,
            format!("within {}s of the server", skew.as_secs()),
        );
    }
    let direction = if server_time > local {
        "behind"
    } else {
        "ahead of"
    };
    Finding::warn(
        Check::Clock,
        format!("{}s {direction} the server", skew.as_secs()),
        "sync this machine's clock; tokens are refused beyond the skew limit",
    )
}

/// The skipped row for a check that needs the server's settings.
pub(crate) fn needs_server(check: Check) -> Finding {
    Finding::skip(check, "needs the server")
}

/// The skipped row for a check that needs Bazel's effective options.
pub(crate) fn needs_bazel(check: Check) -> Finding {
    Finding::skip(check, "needs Bazel")
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::PathBuf;

    use gang_types::CaPin;
    use serde_json::json;

    use super::*;
    use crate::bazel_options::Setting;

    const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----";
    const CAS_TEXT: &str = "; actions upload elsewhere and fail with \"missing from CAS\"";
    const WRONG_URL: &str =
        "use the public URL of the Gang server, the one `gang bazelrc --server` takes";
    const BAZELRC_FIX: &str = "run `gang bazelrc --server https://gang.example.com` and put its output in the workspace .bazelrc";

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test url parses")
    }

    fn server() -> Url {
        url("https://gang.example.com/")
    }

    fn info(authentication: &Value, ca: bool) -> Server<Value> {
        let mut value = json!({
            "name": "server",
            "remoteExecutor": "grpcs://exec.example.com:443",
            "remoteCache": "grpcs://cache.example.com:443",
            "defaultIsa": "x86-64",
            "authentication": authentication,
        });
        if ca {
            value["caCertificate"] = json!(CERTIFICATE);
        }
        serde_json::from_value(value).expect("server parses")
    }

    fn token() -> Value {
        json!({"token": {"login": {}}})
    }

    fn token_info() -> Server<Value> {
        info(&token(), false)
    }

    fn options(entries: &[(&str, &str, OptionSource)]) -> EffectiveOptions {
        EffectiveOptions::new(
            entries
                .iter()
                .map(|(flag, value, source)| {
                    (
                        (*flag).to_owned(),
                        Setting {
                            value: (*value).to_owned(),
                            source: source.clone(),
                        },
                    )
                })
                .collect(),
        )
    }

    fn rc(path: &str, section: &str) -> OptionSource {
        OptionSource::RcFile {
            path: PathBuf::from(path),
            section: section.to_owned(),
        }
    }

    fn at(millis: u64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    fn assert_finding(
        finding: &Finding,
        check: Check,
        status: Status,
        summary: &str,
        fix: Option<&str>,
    ) {
        assert_eq!(finding.check, check);
        assert_eq!(finding.status, status);
        assert_eq!(finding.summary, summary);
        assert_eq!(finding.fix.as_deref(), fix);
    }

    fn assert_cause(finding: &Finding, cause: &str) {
        assert_eq!(finding.cause.as_deref(), Some(cause));
    }

    #[test]
    fn server_reached_names_endpoints_on_other_hosts() {
        let server = server();
        let info = token_info();
        let finding = server_reached(&Gang {
            server: &server,
            info: &info,
        });
        assert_finding(
            &finding,
            Check::Server,
            Status::Ok,
            "https://gang.example.com answers; executor grpcs://exec.example.com:443, cache grpcs://cache.example.com:443",
            None,
        );
    }

    #[test]
    fn server_reached_is_terse_when_the_server_serves_everything() {
        let server = server();
        let info = shared_info();
        let finding = server_reached(&Gang {
            server: &server,
            info: &info,
        });
        assert_finding(
            &finding,
            Check::Server,
            Status::Ok,
            "https://gang.example.com answers",
            None,
        );
    }

    #[test]
    fn server_reached_names_only_the_endpoint_on_another_host() {
        let server = server();
        let info = gang_cache_info();
        let finding = server_reached(&Gang {
            server: &server,
            info: &info,
        });
        assert_eq!(
            finding.summary,
            "https://gang.example.com answers; executor grpcs://exec.example.com:443"
        );
    }

    #[test]
    fn untrusted_server_fails_server_and_tls() {
        let (server_row, tls_row) = server_failed(
            &server(),
            &ServerFault::Untrusted("unknown issuer".into()),
            ServerTrust::System,
        );
        assert_finding(
            &server_row,
            Check::Server,
            Status::Fail,
            "https://gang.example.com did not answer",
            Some("see the tls check"),
        );
        assert_cause(&server_row, "unknown issuer");
        assert_finding(
            &tls_row,
            Check::Tls,
            Status::Fail,
            "the server certificate is not trusted",
            Some(TRUST_FIX),
        );
        assert_cause(&tls_row, "unknown issuer");
    }

    #[test]
    fn unreachable_server_skips_tls() {
        let (server_row, tls_row) = server_failed(
            &server(),
            &ServerFault::Unreachable("tcp connect error: Connection refused (os error 61)".into()),
            ServerTrust::System,
        );
        assert_finding(
            &server_row,
            Check::Server,
            Status::Fail,
            "cannot reach https://gang.example.com",
            Some(REACH_FIX),
        );
        assert_cause(
            &server_row,
            "tcp connect error: Connection refused (os error 61)",
        );
        assert_finding(&tls_row, Check::Tls, Status::Skip, "needs the server", None);
        assert_eq!(tls_row.cause, None);
    }

    #[test]
    fn refused_server_checks_the_tls_of_its_url() {
        let (server_row, tls_row) = server_failed(
            &server(),
            &ServerFault::Refused {
                status: 404,
                message: "not found".into(),
            },
            ServerTrust::System,
        );
        assert_finding(
            &server_row,
            Check::Server,
            Status::Fail,
            "https://gang.example.com answered 404",
            Some(WRONG_URL),
        );
        assert_cause(&server_row, "not found");
        assert_finding(
            &tls_row,
            Check::Tls,
            Status::Ok,
            "trusted by the system roots",
            None,
        );
    }

    #[test]
    fn not_gang_server_checks_the_tls_of_its_url() {
        let plain = url("http://gang.example.com/");
        let (server_row, tls_row) = server_failed(
            &plain,
            &ServerFault::NotGang("missing field".into()),
            ServerTrust::System,
        );
        assert_finding(
            &server_row,
            Check::Server,
            Status::Fail,
            "http://gang.example.com is not a Gang server",
            Some(WRONG_URL),
        );
        assert_cause(&server_row, "missing field");
        assert_finding(
            &tls_row,
            Check::Tls,
            Status::Warn,
            "plaintext http",
            Some(
                "serve the Gang server over https; bearer tokens and build data travel unencrypted",
            ),
        );
    }

    #[test]
    fn https_is_trusted() {
        assert_finding(
            &tls_trusted(&server(), ServerTrust::System),
            Check::Tls,
            Status::Ok,
            "trusted by the system roots",
            None,
        );
    }

    #[test]
    fn https_names_the_ca_pin_that_verified_the_server() {
        let pin: CaPin = format!("sha256:{}", "ab".repeat(32))
            .parse()
            .expect("valid ca pin");
        assert_finding(
            &tls_trusted(&server(), ServerTrust::Pin(&pin)),
            Check::Tls,
            Status::Ok,
            &format!("trusted by the CA pinned as sha256:{}", "ab".repeat(32)),
            None,
        );
    }

    #[test]
    fn plaintext_on_loopback_is_accepted() {
        for text in [
            "http://127.0.0.1:8080/",
            "http://[::1]:8080/",
            "http://localhost:8080/",
        ] {
            assert_finding(
                &tls_trusted(&url(text), ServerTrust::System),
                Check::Tls,
                Status::Ok,
                "plaintext on loopback",
                None,
            );
        }
    }

    #[test]
    fn plaintext_elsewhere_warns() {
        assert_finding(
            &tls_trusted(&url("http://gang.example.com/"), ServerTrust::System),
            Check::Tls,
            Status::Warn,
            "plaintext http",
            Some(
                "serve the Gang server over https; bearer tokens and build data travel unencrypted",
            ),
        );
    }

    #[test]
    fn bazel_reported_is_ok() {
        let finding = bazel_reported();
        assert_eq!(finding.check, Check::Bazel);
        assert_eq!(finding.status, Status::Ok);
        assert_eq!(finding.fix, None);
    }

    #[test]
    fn spawn_failure_asks_to_install_bazel() {
        let error = BazelError::Spawn {
            program: "bazel".into(),
            source: io::Error::new(io::ErrorKind::NotFound, "missing"),
        };
        let finding = bazel_failed(&error);
        assert_finding(
            &finding,
            Check::Bazel,
            Status::Fail,
            "cannot read Bazel's effective flags",
            Some("install Bazel, or pass --bazel <program>"),
        );
        assert_cause(&finding, "cannot run `bazel`: missing");
    }

    #[test]
    fn timeout_asks_to_run_bazel_by_hand() {
        let error = BazelError::TimedOut {
            program: "bazel".into(),
            seconds: 600,
        };
        let finding = bazel_failed(&error);
        assert_finding(
            &finding,
            Check::Bazel,
            Status::Fail,
            "cannot read Bazel's effective flags",
            Some("run `bazel build --nobuild` in this directory to see what stalls it"),
        );
        assert_cause(&finding, "`bazel` did not finish within 600s");
    }

    #[test]
    fn missing_command_line_asks_to_fix_the_workspace() {
        let error = BazelError::NoCommandLine {
            program: "bazel".into(),
            status: "exit status: 1".into(),
            detail: "boom".into(),
        };
        let finding = bazel_failed(&error);
        assert_finding(
            &finding,
            Check::Bazel,
            Status::Fail,
            "cannot read Bazel's effective flags",
            Some(
                "run `bazel build --nobuild` in this workspace directory and fix what it reports; `gang doctor` must run inside the workspace",
            ),
        );
        assert_cause(
            &finding,
            "`bazel` reported no command line (exit status: 1): boom",
        );
    }

    #[test]
    fn invalid_events_ask_to_fix_the_workspace() {
        let source = serde_json::from_str::<Value>("{").expect_err("truncated json fails");
        let text = format!("the build events of `bazel` are not valid: {source}");
        let error = BazelError::Events {
            program: "bazel".into(),
            source,
        };
        let finding = bazel_failed(&error);
        assert_finding(
            &finding,
            Check::Bazel,
            Status::Fail,
            "cannot read Bazel's effective flags",
            Some(
                "run `bazel build --nobuild` in this workspace directory and fix what it reports; `gang doctor` must run inside the workspace",
            ),
        );
        assert_cause(&finding, &text);
    }

    #[test]
    fn executor_matching_gang_is_ok() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_EXECUTOR,
            "grpcs://exec.example.com:443",
            OptionSource::Client,
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Executor, false),
            Check::RemoteExecutor,
            Status::Ok,
            "grpcs://exec.example.com:443",
            None,
        );
    }

    #[test]
    fn one_trailing_slash_is_ignored() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_CACHE,
            "grpcs://cache.example.com:443/",
            OptionSource::Client,
        )]);
        assert_eq!(
            remote_endpoint(&gang, &options, Endpoint::Cache, false).status,
            Status::Ok
        );
    }

    #[test]
    fn cache_from_an_rc_file_names_the_file_and_section() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_CACHE,
            "grpcs://other.example.com:443",
            rc("/home/u/.bazelrc", "build:ci"),
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Fail,
            "grpcs://other.example.com:443 from /home/u/.bazelrc",
            Some(
                "remove or change --remote_cache in section `build:ci` of /home/u/.bazelrc; Gang's is grpcs://cache.example.com:443",
            ),
        );
    }

    #[test]
    fn failing_cache_with_gang_executor_gets_cas_hint() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_CACHE,
            "grpcs://other.example.com:443",
            rc("/home/u/.bazelrc", "build:ci"),
        )]);
        let finding = remote_endpoint(&gang, &options, Endpoint::Cache, true);
        assert_eq!(finding.status, Status::Fail);
        assert_eq!(
            finding.summary,
            "grpcs://other.example.com:443 from /home/u/.bazelrc"
        );
        assert_eq!(
            finding.fix.as_deref(),
            Some(
                format!(
                    "remove or change --remote_cache in section `build:ci` of /home/u/.bazelrc; Gang's is grpcs://cache.example.com:443{CAS_TEXT}"
                )
                .as_str()
            )
        );
    }

    #[test]
    fn cache_hint_is_not_added_to_an_ok_cache() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_CACHE,
            "grpcs://cache.example.com:443",
            OptionSource::Client,
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, true),
            Check::RemoteCache,
            Status::Ok,
            "grpcs://cache.example.com:443",
            None,
        );
    }

    #[test]
    fn failing_executor_gets_no_cas_hint() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_EXECUTOR,
            "grpcs://other-exec.example.com:443",
            OptionSource::Client,
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Executor, true),
            Check::RemoteExecutor,
            Status::Fail,
            "grpcs://other-exec.example.com:443 from the Bazel client",
            Some("stop passing --remote_executor to Bazel; Gang's is grpcs://exec.example.com:443"),
        );
    }

    #[test]
    fn unknown_source_is_reported_as_such() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_CACHE,
            "grpcs://other.example.com:443",
            OptionSource::Unknown,
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Fail,
            "grpcs://other.example.com:443",
            Some(
                "stop passing --remote_cache on the command line, or find where it is set with `bazel info --announce_rc`; Gang's is grpcs://cache.example.com:443",
            ),
        );
    }

    #[test]
    fn empty_value_disables_the_endpoint() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(REMOTE_CACHE, "", OptionSource::Client)]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Fail,
            "Bazel disables --remote_cache from the Bazel client",
            Some("stop passing --remote_cache to Bazel; Gang's is grpcs://cache.example.com:443"),
        );
    }

    #[test]
    fn missing_value_asks_for_the_bazelrc_output() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = EffectiveOptions::default();
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Fail,
            "Bazel sets no --remote_cache",
            Some(BAZELRC_FIX),
        );
        assert_eq!(
            remote_endpoint(&gang, &options, Endpoint::Cache, true)
                .fix
                .as_deref(),
            Some(format!("{BAZELRC_FIX}{CAS_TEXT}").as_str())
        );
    }

    fn shared_info() -> Server<Value> {
        serde_json::from_value(json!({
            "name": "server",
            "remoteExecutor": "grpcs://gang.example.com:443",
            "remoteCache": "grpcs://gang.example.com:443",
            "defaultIsa": "x86-64",
            "authentication": token(),
        }))
        .expect("server parses")
    }

    #[test]
    fn unset_cache_is_served_by_a_gang_executor() {
        let server = server();
        let info = shared_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_EXECUTOR,
            "grpcs://gang.example.com:443",
            OptionSource::Client,
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Ok,
            "grpcs://gang.example.com:443",
            None,
        );
    }

    #[test]
    fn empty_cache_is_served_by_a_gang_executor() {
        let server = server();
        let info = shared_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[
            (REMOTE_CACHE, "", OptionSource::Client),
            (
                REMOTE_EXECUTOR,
                "grpcs://gang.example.com:443",
                OptionSource::Client,
            ),
        ]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Ok,
            "grpcs://gang.example.com:443",
            None,
        );
    }

    #[test]
    fn unset_cache_with_a_foreign_executor_fails_without_cas_hint() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_EXECUTOR,
            "grpcs://other-exec.example.com:443",
            OptionSource::Client,
        )]);
        let finding = remote_endpoint(&gang, &options, Endpoint::Cache, false);
        assert_eq!(finding.check, Check::RemoteCache);
        assert_eq!(finding.status, Status::Fail);
        assert_eq!(
            finding.summary,
            "grpcs://other-exec.example.com:443 from --remote_executor"
        );
        assert_eq!(
            finding.fix.as_deref(),
            Some(
                format!(
                    "{BAZELRC_FIX}; Bazel caches through --remote_executor instead of Gang's cache grpcs://cache.example.com:443"
                )
                .as_str()
            )
        );
        assert!(!finding.summary.contains("missing from CAS"));
    }

    #[test]
    fn no_cache_and_no_executor_sets_nothing() {
        let server = server();
        let info = shared_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        assert_finding(
            &remote_endpoint(&gang, &EffectiveOptions::default(), Endpoint::Cache, false),
            Check::RemoteCache,
            Status::Fail,
            "Bazel sets no --remote_cache",
            Some(BAZELRC_FIX),
        );
    }

    #[test]
    fn explicit_foreign_cache_with_gang_executor_keeps_cas_hint() {
        let server = server();
        let info = shared_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            REMOTE_CACHE,
            "grpcs://other.example.com:443",
            OptionSource::Client,
        )]);
        assert_finding(
            &remote_endpoint(&gang, &options, Endpoint::Cache, true),
            Check::RemoteCache,
            Status::Fail,
            "grpcs://other.example.com:443 from the Bazel client",
            Some(
                format!("stop passing --remote_cache to Bazel; Gang's is grpcs://gang.example.com:443{CAS_TEXT}")
                    .as_str(),
            ),
        );
    }

    #[test]
    fn ca_not_needed_by_the_server_is_ok() {
        let server = server();
        let info = info(&token(), false);
        let gang = Gang {
            server: &server,
            info: &info,
        };
        assert_finding(
            &tls_certificate(&gang, &EffectiveOptions::default()),
            Check::TlsCertificate,
            Status::Ok,
            "none needed; the system roots apply",
            None,
        );
    }

    #[test]
    fn ca_given_to_bazel_is_ok() {
        let server = server();
        let info = info(&token(), true);
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            TLS_CERTIFICATE,
            "/home/u/gang-ca.pem",
            rc("/home/u/.bazelrc", "build"),
        )]);
        assert_finding(
            &tls_certificate(&gang, &options),
            Check::TlsCertificate,
            Status::Ok,
            "gang-ca.pem",
            None,
        );
    }

    #[test]
    fn ca_missing_or_empty_fails() {
        let server = server();
        let info = info(&token(), true);
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let fix = "run `gang bazelrc --server https://gang.example.com` and keep the gang-ca.pem it writes next to the rc file; the server uses Gang's own CA";
        assert_finding(
            &tls_certificate(&gang, &EffectiveOptions::default()),
            Check::TlsCertificate,
            Status::Fail,
            "Bazel has no --tls_certificate",
            Some(fix),
        );
        let empty = options(&[(TLS_CERTIFICATE, "", OptionSource::Client)]);
        assert_finding(
            &tls_certificate(&gang, &empty),
            Check::TlsCertificate,
            Status::Fail,
            "Bazel has no --tls_certificate",
            Some(fix),
        );
    }

    #[test]
    fn no_login_needs_no_credential_helper() {
        let server = server();
        let info = info(&json!({"none": {}}), false);
        let gang = Gang {
            server: &server,
            info: &info,
        };
        assert_finding(
            &credential_helper(&gang, &EffectiveOptions::default()),
            Check::CredentialHelper,
            Status::Ok,
            "the server needs no login",
            None,
        );
    }

    #[test]
    fn no_helper_fails_for_the_cache_host() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        assert_finding(
            &credential_helper(&gang, &EffectiveOptions::default()),
            Check::CredentialHelper,
            Status::Fail,
            "no --credential_helper covers cache.example.com",
            Some(BAZELRC_FIX),
        );
    }

    #[test]
    fn unscoped_helper_covers_every_host() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            CREDENTIAL_HELPER,
            "/usr/bin/gang-helper",
            OptionSource::Client,
        )]);
        assert_finding(
            &credential_helper(&gang, &options),
            Check::CredentialHelper,
            Status::Ok,
            "cache.example.com=/usr/bin/gang-helper",
            None,
        );
    }

    #[test]
    fn scope_with_scheme_and_port_covers_the_host() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            CREDENTIAL_HELPER,
            "grpcs://cache.example.com:443=/usr/bin/gang-helper",
            OptionSource::Client,
        )]);
        assert_finding(
            &credential_helper(&gang, &options),
            Check::CredentialHelper,
            Status::Ok,
            "cache.example.com=/usr/bin/gang-helper",
            None,
        );
    }

    #[test]
    fn scope_for_another_host_does_not_cover() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            CREDENTIAL_HELPER,
            "other.example.com=/usr/bin/gang-helper",
            OptionSource::Client,
        )]);
        assert_finding(
            &credential_helper(&gang, &options),
            Check::CredentialHelper,
            Status::Fail,
            "no --credential_helper covers cache.example.com",
            Some(BAZELRC_FIX),
        );
    }

    #[test]
    fn wildcard_scope_counts_as_covering() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(
            CREDENTIAL_HELPER,
            "*.example.com=/usr/bin/wild",
            OptionSource::Client,
        )]);
        assert_finding(
            &credential_helper(&gang, &options),
            Check::CredentialHelper,
            Status::Ok,
            "cache.example.com=/usr/bin/wild",
            None,
        );
    }

    #[test]
    fn most_specific_covering_helper_wins() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[
            (
                CREDENTIAL_HELPER,
                "other.example.com=/bin/a",
                OptionSource::Client,
            ),
            (
                CREDENTIAL_HELPER,
                "cache.example.com=/bin/b",
                OptionSource::Client,
            ),
            (CREDENTIAL_HELPER, "/bin/c", OptionSource::Client),
        ]);
        assert_finding(
            &credential_helper(&gang, &options),
            Check::CredentialHelper,
            Status::Ok,
            "cache.example.com=/bin/b",
            None,
        );
    }

    /// Resolve entries against the cache host using the same path as the doctor.
    fn selected_helper(entries: &[&str]) -> Option<String> {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let settings: Vec<_> = entries
            .iter()
            .map(|entry| (CREDENTIAL_HELPER, *entry, OptionSource::Client))
            .collect();
        let options = options(&settings);
        helper_for(&gang, &options).map(str::to_owned)
    }

    #[test]
    fn scoped_helper_beats_earlier_unscoped_helper() {
        assert_eq!(
            selected_helper(&["/bin/default", "*.example.com=/bin/scoped"]),
            Some("/bin/scoped".to_owned())
        );
    }

    #[test]
    fn exact_helper_beats_later_wildcard() {
        assert_eq!(
            selected_helper(&["cache.example.com=/bin/exact", "*.example.com=/bin/wild"]),
            Some("/bin/exact".to_owned())
        );
    }

    #[test]
    fn longer_suffix_beats_later_shorter_suffix() {
        assert_eq!(
            selected_helper(&["*.example.com=/bin/long", "*.com=/bin/short"]),
            Some("/bin/long".to_owned())
        );
    }

    #[test]
    fn later_equal_scope_wins() {
        assert_eq!(
            selected_helper(&[
                "cache.example.com=/bin/first",
                "CACHE.EXAMPLE.COM=/bin/last"
            ]),
            Some("/bin/last".to_owned())
        );
    }

    #[test]
    fn uppercase_host_scope_matches() {
        assert_eq!(
            selected_helper(&["CACHE.EXAMPLE.COM=/bin/exact"]),
            Some("/bin/exact".to_owned())
        );
        assert_eq!(
            selected_helper(&["*.EXAMPLE.COM=/bin/wild"]),
            Some("/bin/wild".to_owned())
        );
    }

    fn gang_cache_info() -> Server<Value> {
        serde_json::from_value(json!({
            "name": "server",
            "remoteExecutor": "grpcs://exec.example.com:443",
            "remoteCache": "grpcs://gang.example.com:443",
            "defaultIsa": "x86-64",
            "authentication": token(),
        }))
        .expect("server parses")
    }

    fn assert_covered_by(entry: &str, status: Status, summary: &str) {
        let server = server();
        let info = gang_cache_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(CREDENTIAL_HELPER, entry, OptionSource::Client)]);
        let finding = credential_helper(&gang, &options);
        assert_eq!(finding.check, Check::CredentialHelper);
        assert_eq!(finding.status, status);
        assert_eq!(finding.summary, summary);
    }

    #[test]
    fn wildcard_for_another_domain_does_not_cover_gang_host() {
        assert_finding(
            &{
                let server = server();
                let info = gang_cache_info();
                let gang = Gang {
                    server: &server,
                    info: &info,
                };
                let options = options(&[(
                    CREDENTIAL_HELPER,
                    "*.other.example=/x",
                    OptionSource::Client,
                )]);
                credential_helper(&gang, &options)
            },
            Check::CredentialHelper,
            Status::Fail,
            "no --credential_helper covers gang.example.com",
            Some(BAZELRC_FIX),
        );
    }

    #[test]
    fn wildcard_for_parent_domain_covers_subdomain() {
        assert_covered_by("*.example.com=/x", Status::Ok, "gang.example.com=/x");
    }

    #[test]
    fn wildcard_for_the_host_itself_covers_it() {
        assert_covered_by("*.gang.example.com=/x", Status::Ok, "gang.example.com=/x");
    }

    #[test]
    fn wildcard_must_match_on_a_label_boundary() {
        let server = server();
        let info = gang_cache_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(CREDENTIAL_HELPER, "*.xample.com=/x", OptionSource::Client)]);
        assert_finding(
            &credential_helper(&gang, &options),
            Check::CredentialHelper,
            Status::Fail,
            "no --credential_helper covers gang.example.com",
            Some(BAZELRC_FIX),
        );
    }

    #[test]
    fn bare_wildcard_covers_every_host() {
        assert_covered_by("*=/x", Status::Ok, "gang.example.com=/x");
    }

    #[test]
    fn scope_with_scheme_and_port_matches_host() {
        assert_covered_by(
            "https://gang.example.com:443=/x",
            Status::Ok,
            "gang.example.com=/x",
        );
    }

    const BY_HAND: &str = "run `echo '{\"uri\":\"grpcs://cache.example.com:443\"}' | /x get` to see why; if you are not logged in, run `gang login --server https://gang.example.com`";

    #[test]
    fn credentials_not_needed_is_ok() {
        let finding = credentials_not_needed();
        assert_eq!(finding.check, Check::Credentials);
        assert_eq!(finding.status, Status::Ok);
        assert_eq!(finding.fix, None);
    }

    #[test]
    fn needs_helper_is_skipped() {
        let finding = needs_helper();
        assert_eq!(finding.check, Check::Credentials);
        assert_eq!(finding.status, Status::Skip);
        assert_eq!(finding.fix, None);
    }

    #[test]
    fn helper_for_returns_the_covering_helper() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let options = options(&[(CREDENTIAL_HELPER, "/x", rc("/home/u/.bazelrc", "build"))]);
        assert_eq!(helper_for(&gang, &options), Some("/x"));
        assert_eq!(helper_for(&gang, &EffectiveOptions::default()), None);
    }

    #[test]
    fn credential_returning_a_token_is_ok() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        assert_finding(
            &credentials(&gang, "/x", &Ok(())),
            Check::Credentials,
            Status::Ok,
            "/x get returns a token",
            None,
        );
    }

    #[test]
    fn helper_that_cannot_run_names_the_helper() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let outcome = Err(CredentialFault::NotRunnable("no such file".into()));
        let finding = credentials(&gang, "/x", &outcome);
        assert_finding(
            &finding,
            Check::Credentials,
            Status::Fail,
            "cannot run /x",
            Some("install it, or correct the path in --credential_helper"),
        );
        assert_cause(&finding, "no such file");
    }

    #[test]
    fn failed_helper_suggests_running_it_by_hand() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let outcome = Err(CredentialFault::Failed("exit status: 1".into()));
        let finding = credentials(&gang, "/x", &outcome);
        assert_finding(
            &finding,
            Check::Credentials,
            Status::Fail,
            "/x get failed",
            Some(BY_HAND),
        );
        assert_cause(&finding, "exit status: 1");
    }

    #[test]
    fn timed_out_helper_names_the_limit() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let outcome = Err(CredentialFault::TimedOut { seconds: 30 });
        assert_finding(
            &credentials(&gang, "/x", &outcome),
            Check::Credentials,
            Status::Fail,
            "/x get did not answer within 30s",
            Some(BY_HAND),
        );
    }

    #[test]
    fn unusable_reply_says_what_the_helper_must_print() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let outcome = Err(CredentialFault::BadReply(
            "the reply is not a JSON object with `headers`".into(),
        ));
        let finding = credentials(&gang, "/x", &outcome);
        assert_finding(
            &finding,
            Check::Credentials,
            Status::Fail,
            "/x get printed something Bazel cannot use",
            Some("the helper must print a JSON object with `headers` on standard output"),
        );
        assert_cause(&finding, "the reply is not a JSON object with `headers`");
    }

    #[test]
    fn refused_token_names_the_helper_and_the_server_message() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let outcome = Err(CredentialFault::Refused {
            status: 401,
            message: "principal revoked".into(),
        });
        let finding = credentials(&gang, "/x", &outcome);
        assert_finding(
            &finding,
            Check::Credentials,
            Status::Fail,
            "the server refused the token of /x",
            Some(
                "log in again with `gang login --server https://gang.example.com`; if it still refuses, ask the server's administrator whether your principal was revoked",
            ),
        );
        assert_cause(&finding, "principal revoked");
    }

    #[test]
    fn unreachable_server_cannot_be_presented_a_token() {
        let server = server();
        let info = token_info();
        let gang = Gang {
            server: &server,
            info: &info,
        };
        let outcome = Err(CredentialFault::Unreachable("connection refused".into()));
        let finding = credentials(&gang, "/x", &outcome);
        assert_finding(
            &finding,
            Check::Credentials,
            Status::Fail,
            "cannot present the token to the server",
            Some("check connectivity to the server"),
        );
        assert_cause(&finding, "connection refused");
    }

    #[test]
    fn clock_without_date_header_warns() {
        assert_finding(
            &clock(
                None,
                at(1_000_000),
                at(1_000_000),
                NonZeroDuration::from_secs(30),
            ),
            Check::Clock,
            Status::Warn,
            "the server sent no Date header",
            Some("the clocks were not compared; compare `date -u` here with the server's clock"),
        );
    }

    #[test]
    fn matching_clocks_are_ok() {
        assert_finding(
            &clock(
                Some(at(1_000_000)),
                at(1_000_000),
                at(1_000_000),
                NonZeroDuration::from_secs(30),
            ),
            Check::Clock,
            Status::Ok,
            "within 0s of the server",
            None,
        );
    }

    #[test]
    fn skew_is_measured_at_the_midpoint_of_the_request() {
        // Midpoint of 1_000_000..1_002_000 is 1_001_000, so the skew is 10s; the receive time
        // would give 9s. The limit equals the skew, which is still accepted.
        assert_finding(
            &clock(
                Some(at(1_011_000)),
                at(1_000_000),
                at(1_002_000),
                NonZeroDuration::from_secs(10),
            ),
            Check::Clock,
            Status::Ok,
            "within 10s of the server",
            None,
        );
    }

    #[test]
    fn clock_behind_the_server_warns() {
        assert_finding(
            &clock(
                Some(at(1_045_000)),
                at(1_000_000),
                at(1_000_000),
                NonZeroDuration::from_secs(30),
            ),
            Check::Clock,
            Status::Warn,
            "45s behind the server",
            Some("sync this machine's clock; tokens are refused beyond the skew limit"),
        );
    }

    #[test]
    fn clock_ahead_of_the_server_warns() {
        assert_finding(
            &clock(
                Some(at(955_000)),
                at(1_000_000),
                at(1_000_000),
                NonZeroDuration::from_secs(30),
            ),
            Check::Clock,
            Status::Warn,
            "45s ahead of the server",
            Some("sync this machine's clock; tokens are refused beyond the skew limit"),
        );
    }

    #[test]
    fn needs_server_skips_with_the_server_reason() {
        assert_finding(
            &needs_server(Check::Clock),
            Check::Clock,
            Status::Skip,
            "needs the server",
            None,
        );
    }

    #[test]
    fn needs_bazel_skips_with_the_bazel_reason() {
        assert_finding(
            &needs_bazel(Check::RemoteCache),
            Check::RemoteCache,
            Status::Skip,
            "needs Bazel",
            None,
        );
    }
}
