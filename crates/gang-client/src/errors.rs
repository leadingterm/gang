//! Errors of the client library.

use std::borrow::Cow;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use gang_os::PathIo;
use gang_spi::PortError;
use gang_tls::{HttpError, TransportError};
use gang_types::{Fix, NonZeroBytes, ProviderName};
use url::Url;

use crate::bazelrc::display_url;

/// The fix of a server that cannot be reached.
pub(crate) const REACH_FIX: &str = "check --server; for a farm on this machine, run gang local up";
/// The fix of a server whose certificate the client does not trust.
pub(crate) const TRUST_FIX: &str = "add --ca-pin sha256:<hex>; gang server prints it at startup";
/// The fix of a stored login that cannot be renewed.
const LOGIN_AGAIN_FIX: &str = "run gang login again";

/// Everything that can go wrong while logging in, minting a bearer token, or serving a
/// credential to Bazel.
///
/// A message names the operation and the value and never repeats its source; the action that
/// helps is [`Fix::fix`].
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// A file under the client directory or the workspace could not be read, written, or removed.
    #[error(transparent)]
    Io(#[from] PathIo),
    /// Neither `RUNNER_TEMP` nor `HOME` is set to a usable value.
    #[error("cannot locate the client directory")]
    HomeUnknown,
    /// The client options file is not valid TOML of the expected shape.
    #[error("client options file {} is invalid", path.display())]
    Options {
        /// The client options file.
        path: PathBuf,
        /// The TOML failure.
        #[source]
        source: toml::de::Error,
    },
    /// No session is stored: the client has never logged in.
    #[error("not logged in")]
    NotLoggedIn,
    /// The stored session file is not valid JSON of the expected shape.
    #[error("the stored session {} cannot be read or written", path.display())]
    Session {
        /// The session file.
        path: PathBuf,
        /// The JSON failure.
        #[source]
        source: serde_json::Error,
    },
    /// A URL could not be parsed or joined.
    #[error("invalid url")]
    Url(#[from] url::ParseError),
    /// The server URL is neither `http` nor `https`, so no Gang server can be reached at it.
    #[error("the server URL {url} must use the http or https scheme")]
    UnsupportedServerUrl {
        /// The server URL with an unsupported scheme.
        url: Url,
    },
    /// The server does not authenticate its clients, so there is no login to perform.
    #[error("the server does not require a login")]
    NoAuthentication,
    /// The server requires an authentication scheme this client version does not know.
    #[error("the server requires an authentication scheme this client does not support")]
    UnsupportedAuthentication,
    /// The credential request names a host other than the server this client is bound to.
    #[error(
        "the credential request is for host `{requested}`, but the session server is `{server}`"
    )]
    HostMismatch {
        /// The host named in the credential request.
        requested: String,
        /// The host of the session server.
        server: String,
    },
    /// The request to the server failed before a response arrived.
    #[error("cannot reach the server {server}")]
    Transport {
        /// The server's base URL, without a trailing slash.
        server: String,
        /// The HTTP client's failure.
        #[source]
        source: HttpError,
    },
    /// A request body could not be encoded as JSON.
    #[error("cannot encode the request")]
    Request(#[source] serde_json::Error),
    /// The TLS configuration could not be built.
    #[error("cannot configure TLS")]
    Tls(#[source] TransportError),
    /// The server answered with a non-success status.
    #[error("the server answered {status}: {message}")]
    Server {
        /// The HTTP status code.
        status: u16,
        /// The server's message, or the status reason when it sent none.
        message: String,
    },
    /// The server is rate limiting this client.
    #[error("the server is rate limiting this client")]
    RateLimited {
        /// How long the server asks the client to wait before it retries, when it says so.
        retry_after: Option<Duration>,
    },
    /// The server's success response is not the expected JSON.
    #[error("the server's response is not valid")]
    Body(#[source] serde_json::Error),
    /// The server's response is larger than the configured limit.
    #[error("the server's response exceeds the {limit} limit")]
    BodyTooLarge {
        /// The configured limit, `max_response_size`.
        limit: NonZeroBytes,
    },
    /// No identity provider in the server's list offers interactive login.
    #[error("no identity provider offers interactive login")]
    NoInteractiveProvider,
    /// The requested identity provider is not offered for interactive login.
    #[error("identity provider `{provider}` does not offer interactive login")]
    ProviderNotOffered {
        /// The requested provider.
        provider: ProviderName,
    },
    /// Several identity providers offer interactive login and none was chosen.
    #[error("several identity providers offer interactive login: {offered}")]
    AmbiguousProvider {
        /// The offering providers, joined with `, `.
        offered: String,
    },
    /// The login configuration the server published for a provider does not parse.
    #[error("the login configuration of `{provider}` is not usable")]
    LoginConfig {
        /// The provider whose configuration failed.
        provider: ProviderName,
        /// The JSON failure.
        #[source]
        source: serde_json::Error,
    },
    /// The identity provider failed during a login.
    #[error("identity provider failed")]
    Provider(#[source] PortError),
    /// The identity provider refused the stored login during renewal.
    #[error("the identity provider no longer accepts the stored login")]
    RenewalRejected(#[source] PortError),
    /// The stored login has no refresh token to renew with.
    #[error("the stored login has no refresh token")]
    NoRefreshToken,
    /// The browser redirect did not arrive in time.
    #[error("login timed out waiting for the browser redirect")]
    LoginTimedOut,
    /// The loopback server for the browser redirect could not run.
    #[error("cannot run the loopback login server")]
    Loopback(#[source] io::Error),
    /// The workload proof is empty.
    #[error("the workload proof is empty")]
    EmptyToken,
    /// The command that prints the workload proof could not be started.
    #[error("cannot run the proof command {command}")]
    TokenCommand {
        /// The command that failed to start.
        command: String,
        /// The spawn failure.
        #[source]
        source: io::Error,
    },
    /// The command that prints the workload proof exited unsuccessfully.
    #[error("the proof command {command} failed with {status}")]
    TokenCommandFailed {
        /// The command that failed.
        command: String,
        /// The exit status, as reported by the operating system.
        status: String,
    },
    /// The command that prints the workload proof did not finish in time.
    #[error("the proof command {command} did not finish within {seconds}s")]
    TokenCommandTimedOut {
        /// The command that timed out.
        command: String,
        /// The time allowed, in whole seconds.
        seconds: u64,
    },
    /// The command's output exceeded the configured limit.
    #[error("the proof command {command} produced too much output")]
    TokenCommandOutputTooLarge {
        /// The command whose output was too large.
        command: String,
        /// The runner's size-limit error.
        #[source]
        source: gang_os::RunError,
    },
    /// The credential helper request from Bazel is not the expected JSON.
    #[error("the credential request is not valid")]
    CredentialRequest(#[source] serde_json::Error),
}

impl ClientError {
    /// The failure of an exchange with the server at `server`.
    pub(crate) fn transport(server: &Url, source: HttpError) -> Self {
        Self::Transport {
            server: display_url(server).to_owned(),
            source,
        }
    }
}

impl Fix for ClientError {
    fn fix(&self) -> Option<Cow<'static, str>> {
        match self {
            Self::HomeUnknown => Some(Cow::Borrowed("set HOME or RUNNER_TEMP")),
            Self::Options { .. } => Some(Cow::Borrowed("fix or delete the client options file")),
            Self::NotLoggedIn => Some(Cow::Borrowed("gang login --server <URL>")),
            Self::UnsupportedServerUrl { .. } => {
                Some(Cow::Borrowed("use an http or https URL for --server"))
            }
            Self::UnsupportedAuthentication => {
                Some(Cow::Borrowed("upgrade gang to the version the server runs"))
            }
            Self::Transport { source, .. } if gang_tls::is_certificate_refused(source) => {
                Some(Cow::Borrowed(TRUST_FIX))
            }
            Self::Transport { .. } => Some(Cow::Borrowed(REACH_FIX)),
            Self::Server { status: 403, .. } => Some(Cow::Borrowed(
                "ask an operator for a role that grants the permission the action needs",
            )),
            Self::NoInteractiveProvider => Some(Cow::Borrowed(
                "run gang login --workload --provider <NAME> --token-command <COMMAND>",
            )),
            Self::AmbiguousProvider { offered } => Some(Cow::Owned(format!(
                "add {}",
                offered
                    .split(", ")
                    .map(|provider| format!("--provider {provider}"))
                    .collect::<Vec<_>>()
                    .join(", or ")
            ))),
            Self::RenewalRejected(_) | Self::NoRefreshToken => Some(Cow::Borrowed(LOGIN_AGAIN_FIX)),
            Self::LoginTimedOut => Some(Cow::Borrowed(
                "run gang login again and finish the sign-in in the browser",
            )),
            Self::TokenCommand { .. } => Some(Cow::Borrowed(
                "check that the proof command exists and is executable",
            )),
            Self::TokenCommandFailed { command, .. }
            | Self::TokenCommandTimedOut { command, .. }
            | Self::TokenCommandOutputTooLarge { command, .. } => Some(Cow::Owned(format!(
                "run {command} yourself to see its error"
            ))),
            Self::RateLimited { retry_after } => Some(match retry_after {
                Some(wait) => Cow::Owned(format!(
                    "run the command again in {} seconds",
                    wait.as_secs().max(1)
                )),
                None => Cow::Borrowed("wait a moment, then run the command again"),
            }),
            Self::Io(_)
            | Self::Session { .. }
            | Self::Url(_)
            | Self::NoAuthentication
            | Self::HostMismatch { .. }
            | Self::Request(_)
            | Self::Tls(_)
            | Self::Server { .. }
            | Self::Body(_)
            | Self::BodyTooLarge { .. }
            | Self::ProviderNotOffered { .. }
            | Self::LoginConfig { .. }
            | Self::Provider(_)
            | Self::Loopback(_)
            | Self::EmptyToken
            | Self::CredentialRequest(_) => None,
        }
    }
}

/// Everything that can go wrong while asking Bazel for its effective options.
///
/// A message names the program and never repeats its source.
#[derive(Debug, thiserror::Error)]
pub enum BazelError {
    /// Bazel could not be started.
    #[error("cannot run `{program}`")]
    Spawn {
        /// The program that failed to start.
        program: String,
        /// The spawn failure.
        #[source]
        source: io::Error,
    },
    /// Bazel did not finish within the configured time and was killed.
    #[error("`{program}` did not finish within {seconds}s")]
    TimedOut {
        /// The program that was killed.
        program: String,
        /// The time allowed, in whole seconds.
        seconds: u64,
    },
    /// Bazel finished without reporting a command line.
    #[error("`{program}` reported no command line ({status}): {detail}")]
    NoCommandLine {
        /// The program that ran.
        program: String,
        /// The exit status, as reported by the operating system.
        status: String,
        /// The first `ERROR:` line of its standard error, else its last non-empty line.
        detail: String,
    },
    /// Bazel's build events are not valid JSON lines.
    #[error("the build events of `{program}` are not valid")]
    Events {
        /// The program that wrote the events.
        program: String,
        /// The JSON failure.
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::assert_sources_not_repeated;

    const CAUSE: &str = "cause-marker";

    fn json_error() -> serde_json::Error {
        serde_json::from_str::<u8>(CAUSE).expect_err("the text is not a number")
    }

    fn io_error() -> io::Error {
        io::Error::other(CAUSE)
    }

    fn port_error() -> PortError {
        PortError::backend(CAUSE)
    }

    fn timed_out() -> HttpError {
        HttpError::TimedOut
    }

    fn server() -> Url {
        Url::parse("https://gang.example.com").expect("the address is valid")
    }

    fn provider(name: &str) -> ProviderName {
        ProviderName::try_new(name).expect("the provider name is valid")
    }

    /// One value of every variant.
    fn every_variant() -> Vec<ClientError> {
        vec![
            ClientError::Io(PathIo::new(
                "read",
                &PathBuf::from("/home/ci/session.json"),
                io_error(),
            )),
            ClientError::HomeUnknown,
            ClientError::Options {
                path: PathBuf::from("/home/ci/client.toml"),
                source: toml::from_str::<toml::Value>("=").expect_err("the text is invalid TOML"),
            },
            ClientError::NotLoggedIn,
            ClientError::Session {
                path: PathBuf::from("/home/ci/session.json"),
                source: json_error(),
            },
            ClientError::Url(url::ParseError::EmptyHost),
            ClientError::UnsupportedServerUrl {
                url: Url::parse("ftp://gang.example.com").expect("the address is valid"),
            },
            ClientError::NoAuthentication,
            ClientError::UnsupportedAuthentication,
            ClientError::HostMismatch {
                requested: "other.example.com".to_owned(),
                server: "gang.example.com".to_owned(),
            },
            ClientError::transport(&server(), timed_out()),
            ClientError::Request(json_error()),
            ClientError::Tls(TransportError::HandshakeTimeout),
            ClientError::Server {
                status: 500,
                message: "boom".to_owned(),
            },
            ClientError::RateLimited {
                retry_after: Some(Duration::from_secs(7)),
            },
            ClientError::Body(json_error()),
            ClientError::BodyTooLarge {
                limit: NonZeroBytes::kib(1),
            },
            ClientError::NoInteractiveProvider,
            ClientError::ProviderNotOffered {
                provider: provider("github"),
            },
            ClientError::AmbiguousProvider {
                offered: "google, okta".to_owned(),
            },
            ClientError::LoginConfig {
                provider: provider("google"),
                source: json_error(),
            },
            ClientError::Provider(port_error()),
            ClientError::RenewalRejected(port_error()),
            ClientError::NoRefreshToken,
            ClientError::LoginTimedOut,
            ClientError::Loopback(io_error()),
            ClientError::EmptyToken,
            ClientError::TokenCommand {
                command: "./ci/job-token".to_owned(),
                source: io_error(),
            },
            ClientError::TokenCommandFailed {
                command: "./ci/job-token".to_owned(),
                status: "exit status: 1".to_owned(),
            },
            ClientError::TokenCommandTimedOut {
                command: "./ci/job-token".to_owned(),
                seconds: 10,
            },
            ClientError::TokenCommandOutputTooLarge {
                command: "./ci/job-token".to_owned(),
                source: gang_os::RunError::StdoutTooLarge {
                    program: PathBuf::from("./ci/job-token"),
                    limit: NonZeroBytes::kib(1),
                },
            },
            ClientError::CredentialRequest(json_error()),
        ]
    }

    fn fix_of(error: &ClientError) -> Option<String> {
        error.fix().map(Cow::into_owned)
    }

    #[test]
    fn not_logged_in_names_the_login_command() {
        assert_eq!(ClientError::NotLoggedIn.to_string(), "not logged in");
        assert_eq!(
            fix_of(&ClientError::NotLoggedIn).as_deref(),
            Some("gang login --server <URL>")
        );
    }

    #[test]
    fn ambiguous_provider_offers_each_provider_in_the_fix() {
        let error = ClientError::AmbiguousProvider {
            offered: "google, okta".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "several identity providers offer interactive login: google, okta"
        );
        assert_eq!(
            fix_of(&error).as_deref(),
            Some("add --provider google, or --provider okta")
        );
    }

    #[test]
    fn a_failed_proof_command_is_run_by_hand() {
        let error = ClientError::TokenCommandFailed {
            command: "./ci/job-token".to_owned(),
            status: "exit status: 1".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "the proof command ./ci/job-token failed with exit status: 1"
        );
        assert_eq!(
            fix_of(&error).as_deref(),
            Some("run ./ci/job-token yourself to see its error")
        );
    }

    #[test]
    fn a_timed_out_proof_command_names_the_timeout_and_fix() {
        let error = ClientError::TokenCommandTimedOut {
            command: "./ci/job-token".to_owned(),
            seconds: 10,
        };

        assert_eq!(
            error.to_string(),
            "the proof command ./ci/job-token did not finish within 10s"
        );
        assert_eq!(
            fix_of(&error).as_deref(),
            Some("run ./ci/job-token yourself to see its error")
        );
    }

    #[test]
    fn only_a_forbidden_answer_points_to_an_operator() {
        let answered = |status| ClientError::Server {
            status,
            message: "forbidden".to_owned(),
        };

        assert_eq!(
            answered(403).to_string(),
            "the server answered 403: forbidden"
        );
        assert_eq!(
            fix_of(&answered(403)).as_deref(),
            Some("ask an operator for a role that grants the permission the action needs")
        );
        assert_eq!(fix_of(&answered(500)), None);
    }

    #[test]
    fn no_interactive_provider_points_to_the_workload_login() {
        assert_eq!(
            fix_of(&ClientError::NoInteractiveProvider).as_deref(),
            Some("run gang login --workload --provider <NAME> --token-command <COMMAND>")
        );
    }

    #[test]
    fn an_unreachable_server_is_named_without_a_trailing_slash() {
        let error = ClientError::transport(&server(), timed_out());

        assert_eq!(
            error.to_string(),
            "cannot reach the server https://gang.example.com"
        );
        assert_eq!(fix_of(&error).as_deref(), Some(REACH_FIX));
    }

    #[test]
    fn a_server_path_is_kept_in_the_name() {
        let base = Url::parse("https://gang.example.com/farm/").expect("the address is valid");

        assert_eq!(display_url(&base), "https://gang.example.com/farm/");
    }

    #[test]
    fn renewal_failures_ask_for_a_new_login() {
        assert_eq!(
            fix_of(&ClientError::RenewalRejected(port_error())).as_deref(),
            Some("run gang login again")
        );
        assert_eq!(
            fix_of(&ClientError::NoRefreshToken).as_deref(),
            Some("run gang login again")
        );
    }

    #[test]
    fn setup_failures_name_their_action() {
        assert_eq!(
            fix_of(&ClientError::HomeUnknown).as_deref(),
            Some("set HOME or RUNNER_TEMP")
        );
        assert_eq!(
            fix_of(&ClientError::UnsupportedAuthentication).as_deref(),
            Some("upgrade gang to the version the server runs")
        );
        assert_eq!(
            fix_of(&ClientError::LoginTimedOut).as_deref(),
            Some("run gang login again and finish the sign-in in the browser")
        );
    }

    #[test]
    fn internal_failures_carry_no_fix() {
        for error in [
            ClientError::Request(json_error()),
            ClientError::Body(json_error()),
            ClientError::BodyTooLarge {
                limit: NonZeroBytes::kib(1),
            },
            ClientError::EmptyToken,
            ClientError::NoAuthentication,
        ] {
            assert_eq!(fix_of(&error), None, "{error}");
        }
    }

    #[test]
    fn every_fix_is_one_line() {
        for error in every_variant() {
            if let Some(fix) = error.fix() {
                assert!(!fix.is_empty(), "{error}");
                assert!(!fix.contains('\n'), "{error}");
            }
        }
    }

    #[test]
    fn no_message_repeats_its_source() {
        assert_sources_not_repeated(&every_variant());
    }

    #[test]
    fn rate_limiting_asks_to_wait() {
        let told = ClientError::RateLimited {
            retry_after: Some(Duration::from_secs(7)),
        };
        let untold = ClientError::RateLimited { retry_after: None };

        assert_eq!(told.to_string(), "the server is rate limiting this client");
        assert_eq!(
            fix_of(&told).as_deref(),
            Some("run the command again in 7 seconds")
        );
        assert_eq!(
            fix_of(&untold).as_deref(),
            Some("wait a moment, then run the command again")
        );
    }

    #[test]
    fn no_bazel_message_repeats_its_source() {
        let errors = [
            BazelError::Spawn {
                program: "bazel".to_owned(),
                source: io_error(),
            },
            BazelError::TimedOut {
                program: "bazel".to_owned(),
                seconds: 1,
            },
            BazelError::NoCommandLine {
                program: "bazel".to_owned(),
                status: "exit status: 1".to_owned(),
                detail: "no command line".to_owned(),
            },
            BazelError::Events {
                program: "bazel".to_owned(),
                source: json_error(),
            },
        ];

        assert_sources_not_repeated(&errors);
    }
}
