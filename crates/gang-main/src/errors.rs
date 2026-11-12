//! Typed errors for the `gang` binary: every failure a command handler can report, with
//! lowercase messages that name the operation and the offending value.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use gang_types::announcement::ListenerRole;
use gang_types::{CaPin, ProviderName};

/// Result type shared by the command handlers.
pub(crate) type Result<T> = std::result::Result<T, MainError>;

/// Renders a timeout the way configuration writes it, such as `30s`.
fn duration_text(value: &Duration) -> String {
    gang_types::duration::display(*value).to_string()
}

/// The configuration key that sets a listener's address.
pub(crate) const fn listen_key(role: ListenerRole) -> &'static str {
    match role {
        ListenerRole::Public => "listen",
        ListenerRole::Worker => "worker_listen",
        ListenerRole::Imds => "mock_imds_listen",
    }
}

/// Any failure a `gang` command can report to the operator.
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub(crate) enum MainError {
    /// The reset trampoline was not given a program.
    #[error("a program is required")]
    ResetUsage,
    /// The reset trampoline could not clear the signal mask.
    #[error("cannot reset the signal mask")]
    ResetMask(#[source] io::Error),
    /// The reset trampoline could not execute the action.
    #[error("cannot execute the action")]
    ResetExec(#[source] io::Error),

    /// The configuration file could not be read.
    #[error("cannot read the configuration `{}`", path.display())]
    #[fix("check the --config path")]
    ReadConfig {
        /// The configuration file that could not be read.
        path: PathBuf,
        /// The underlying read failure.
        #[source]
        source: io::Error,
    },

    /// The configuration file does not parse or validate.
    #[error("invalid configuration `{}`", path.display())]
    #[fix(source)]
    ParseConfig {
        /// The configuration file that failed to parse.
        path: PathBuf,
        /// The parse or validation failure.
        #[source]
        source: gang_config::ConfigError,
    },

    /// A plugin could not be constructed from its configuration section.
    #[error("cannot build the {domain} plugin")]
    Plugin {
        /// The port domain whose plugin failed to build.
        domain: &'static str,
        /// The construction failure.
        #[source]
        source: gang_spi::PortError,
    },

    /// An identity provider's configuration section could not be serialized.
    #[error("cannot serialize the identity provider `{provider}`")]
    ProviderSection {
        /// The identity provider whose section failed to serialize.
        provider: ProviderName,
        /// The serialization failure.
        #[source]
        source: serde_json::Error,
    },

    /// A local-only command ran against a configuration that does not select local mode.
    #[error("the configuration does not select [provisioner.local]")]
    #[fix("pass a local configuration, e.g. --config configs/gang.local.toml")]
    NotLocalMode,

    /// `gang options` named a provisioner that the composed binary does not have.
    #[error("unknown provisioner `{name}`")]
    #[fix("name one of: {accepted}")]
    UnknownProvisioner {
        /// The provisioner key the operator named.
        name: String,
        /// The provisioner keys the binary accepts, comma separated.
        accepted: String,
    },

    /// A listening socket could not be bound.
    #[error("cannot listen on `{address}` for the {role} endpoint")]
    #[fix(
        "stop what holds {address}, or change {} in the configuration",
        listen_key(*role)
    )]
    Bind {
        /// The listener role whose socket could not be bound.
        role: ListenerRole,
        /// The address that could not be bound.
        address: SocketAddr,
        /// The bind failure.
        #[source]
        source: io::Error,
    },

    /// A listening socket's bound address could not be read.
    #[error("cannot read the bound address of a listener")]
    LocalAddress(#[source] io::Error),

    /// A listener's URL cannot be derived from its bound address and transport scheme.
    #[error("cannot derive a listener url")]
    ListenerUrl(#[from] gang_config::ListenerUrlError),
    /// The worker must enroll with the server, but the configuration omits its `endpoint`.
    #[error("endpoint is required to enroll the worker when worker authentication is mtls")]
    WorkerEndpointMissing,

    /// Signal handlers could not be installed.
    #[error("cannot install signal handlers")]
    Signals(#[source] io::Error),

    /// The async runtime could not be started.
    #[error("cannot start the async runtime")]
    Runtime(#[source] io::Error),

    /// Logging could not be initialized.
    #[error("cannot start logging")]
    Logging(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The path of the running executable could not be determined.
    #[error("cannot locate the gang executable")]
    CurrentExecutable(#[source] io::Error),

    /// A workload login lacked its provider or token.
    #[error("a workload login needs a provider and a token")]
    #[fix("add --provider <NAME> and --token <FILE> or --token-command <ARGV>")]
    WorkloadLoginIncomplete,

    /// A certificate revocation named neither a certificate serial nor a principal.
    #[error("name what to revoke")]
    #[fix("gang revoke --serial <HEX>, or gang revoke --principal <PROVIDER:PRINCIPAL>")]
    RevocationTargetIncomplete,

    /// The server answered with a resource name this client cannot read.
    #[error("the server returned an unreadable resource name")]
    ResourceName(#[from] gang_wire::ResourceNameError),
    /// No server was given and the client directory holds no stored login to take one from.
    #[error("no server given and no stored login")]
    #[fix("pass --server <URL>, or run gang login --server <URL>")]
    NoServer,

    /// Standard output could not be written.
    #[error("cannot write to stdout")]
    Stdout(#[source] io::Error),

    /// Standard input could not be read.
    #[error("cannot read standard input")]
    Stdin(#[source] io::Error),

    /// The CA certificate could not be written to the given path.
    #[error("cannot write the CA certificate")]
    CaCertificate(#[source] gang_os::PathIo),

    /// A task the provisioner attached to the server stopped while the server was running.
    #[error("a provisioner sidecar stopped")]
    Sidecar(#[source] gang_spi::PortError),

    /// The dashboard login's client secret could not be loaded from its source.
    #[error("cannot load the dashboard login client secret")]
    #[fix(source)]
    DashboardSecret(#[source] gang_config::SecretFileError),

    /// The task that loads the dashboard login's client secret stopped without a result.
    #[error("the dashboard login client secret loader stopped")]
    DashboardSecretTask(#[source] tokio::task::JoinError),

    /// The gang server command failed.
    #[error(transparent)]
    Server(#[from] gang_server::ServerError),

    /// The gang worker command failed.
    #[error(transparent)]
    #[fix(source)]
    Worker(#[from] gang_worker::WorkerError),

    /// The executor command failed.
    #[error(transparent)]
    Exec(#[from] gang_exec::ExecError),

    /// The `gang-join` trampoline could not join the action's scope.
    #[error(transparent)]
    Join(#[from] gang_cgroup::CgroupError),

    /// The client command failed.
    #[error(transparent)]
    #[fix(source)]
    Client(#[from] gang_client::ClientError),

    /// A local-mode command failed.
    #[error(transparent)]
    #[fix(source)]
    Local(#[from] LocalCommandError),

    /// A deploy decision refused to go on, or could not read its input.
    #[error(transparent)]
    Deploy(#[from] gang_deploy::DeployError),

    /// A file a deploy decision reads could not be read.
    #[error(transparent)]
    DeployFile(#[from] gang_os::PathIo),
}

impl From<gang_spi::BuildError> for MainError {
    fn from(error: gang_spi::BuildError) -> Self {
        Self::Plugin {
            domain: error.domain,
            source: error.source,
        }
    }
}

/// A failure of a `gang local` command: process control, the server record, or startup.
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub(crate) enum LocalCommandError {
    /// A file or directory used for local state could not be accessed.
    #[error(transparent)]
    Io(#[from] gang_os::PathIo),

    /// The server record exists but cannot be parsed.
    #[error("the server record `{}` is not valid", path.display())]
    Record {
        /// The record file that failed to parse.
        path: PathBuf,
        /// The parse failure.
        #[source]
        source: serde_json::Error,
    },

    /// The server lock is held by a running server.
    #[error("`{}` is held by a running gang server or a gang local down in progress", lock.display())]
    #[fix("gang local down")]
    AlreadyRunning {
        /// The server lock that a running server holds.
        lock: PathBuf,
    },

    /// The server lock is held but no server record exists.
    #[error("a gang server holds `{}` but has no record", lock.display())]
    #[fix("stop the gang server that holds the lock by hand")]
    UnrecordedServer {
        /// The server lock that is held without a record.
        lock: PathBuf,
    },

    /// The state directory exists but other users can open it; it was left unchanged.
    #[error("the state directory `{}` has mode `{mode:04o}`; it must be private", path.display())]
    #[fix(
        "run chmod 700 {}, or set [provisioner.local] state_dir to a private directory",
        path.display()
    )]
    StateDirNotPrivate {
        /// The state directory.
        path: PathBuf,
        /// Its permission bits.
        mode: u32,
    },

    /// The server process could not be started.
    #[error("cannot start the gang server")]
    Spawn(#[from] gang_os::SpawnError),

    /// The path of the running executable could not be determined.
    #[error("cannot locate the gang executable")]
    Executable(#[source] io::Error),

    /// The server process exited before it reported its CA pin.
    #[error("the gang server exited during startup")]
    #[fix("read {}", log.display())]
    ServerExited {
        /// The server log to inspect.
        log: PathBuf,
    },

    /// The server did not report the announcement `gang local up` waited for within the startup
    /// timeout.
    #[error(
        "the gang server printed no {awaited} announcement within {}",
        duration_text(timeout)
    )]
    #[fix("read {}; on a slow machine raise --startup-timeout", log.display())]
    StartupTimedOut {
        /// The announcement waited for: `ca-pin`, or `plaintext` under `--insecure`.
        awaited: &'static str,
        /// The configured startup timeout.
        timeout: Duration,
        /// The server log to inspect.
        log: PathBuf,
    },

    /// The server's log does not name a listener it announces before its startup line.
    #[error("the server log `{}` does not announce its {role} listener", log.display())]
    MissingListener {
        /// The listener role that is missing.
        role: ListenerRole,
        /// The server log that was read.
        log: PathBuf,
    },

    /// The server did not exit within the stop timeout after being signaled.
    #[error(
        "the gang server (pid {pid}) did not stop within {}",
        duration_text(timeout)
    )]
    #[fix("kill -9 {pid}, then run gang local down again")]
    StopTimedOut {
        /// The process id of the server.
        pid: u32,
        /// The configured stop timeout.
        timeout: Duration,
    },

    /// A signal could not be delivered to a process.
    #[error("cannot signal process {pid}")]
    Signal {
        /// The process that could not be signaled.
        pid: u32,
        /// The system call failure.
        #[source]
        source: io::Error,
    },

    /// The local provisioner could not be reached.
    #[error("cannot reach the local provisioner")]
    Provisioner(#[source] gang_spi::PortError),

    /// The configuration copy could not be rendered back to TOML.
    #[error("cannot write the configuration copy")]
    Render(#[source] gang_config::ConfigError),

    /// The server lock could not be handed to the server process.
    #[error("cannot hand the server lock to the server")]
    Stdin(#[source] io::Error),

    /// `--insecure` names a listener or endpoint that is not on loopback.
    #[error("--insecure needs loopback addresses, but `{what}` is `{value}`")]
    #[fix("set {what} to a loopback address, or drop --insecure")]
    InsecureNeedsLoopback {
        /// The configuration key that holds the address.
        what: String,
        /// The address as configured.
        value: String,
    },

    /// The configuration serves plaintext, but `--insecure` was not given.
    #[error(
        "the configuration serves plaintext; gang local up serves plaintext only with --insecure"
    )]
    #[fix("set [server] transport = \"tls\", or pass --insecure to serve plaintext on loopback")]
    PlaintextNeedsInsecure,

    /// A configured endpoint has no host to use for a plaintext endpoint.
    #[error("cannot rewrite the endpoint `{endpoint}` without a host")]
    EndpointWithoutHost {
        /// The endpoint as configured.
        endpoint: String,
    },

    /// A configured endpoint cannot be rewritten under the scheme a plaintext server serves.
    #[error("cannot rewrite the endpoint `{endpoint}` under the `{scheme}` scheme")]
    EndpointScheme {
        /// The endpoint as configured.
        endpoint: String,
        /// The scheme it was rewritten under.
        scheme: &'static str,
        /// Why the rewritten text is not a URL.
        #[source]
        source: url::ParseError,
    },

    /// The server announced a CA pin other than the one derived from the configured authority.
    #[error("the server announced `{announced}` but the configured authority has pin `{derived}`")]
    PinMismatch {
        /// The pin derived from the configured authority's public key.
        derived: CaPin,
        /// The pin the server announced on startup.
        announced: CaPin,
    },
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use gang_types::Fix as _;

    use super::*;

    const SOURCE_TEXT: &str = "zz-underlying-cause";

    fn io_cause() -> io::Error {
        io::Error::other(SOURCE_TEXT)
    }

    fn address() -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], 443))
    }

    #[test]
    fn listen_keys_match_configuration() {
        assert_eq!(listen_key(ListenerRole::Public), "listen");
        assert_eq!(listen_key(ListenerRole::Worker), "worker_listen");
        assert_eq!(listen_key(ListenerRole::Imds), "mock_imds_listen");
    }

    fn config_error() -> gang_config::ConfigError {
        let cause = toml::from_str::<toml::Value>("= 1").expect_err("the text is not toml");
        gang_config::ConfigError::Gang(cause)
    }

    fn assert_single_line(fix: &str) {
        assert!(!fix.is_empty(), "a fix is never empty");
        assert!(!fix.contains('\n'), "a fix is one line: {fix:?}");
    }

    #[test]
    fn bind_fix_names_the_key_of_the_endpoint() {
        let fix = |role| {
            MainError::Bind {
                role,
                address: address(),
                source: io_cause(),
            }
            .fix()
        };
        assert_eq!(
            fix(ListenerRole::Public).as_deref(),
            Some("stop what holds 0.0.0.0:443, or change listen in the configuration")
        );
        assert_eq!(
            fix(ListenerRole::Worker).as_deref(),
            Some("stop what holds 0.0.0.0:443, or change worker_listen in the configuration")
        );
        assert_eq!(
            fix(ListenerRole::Imds).as_deref(),
            Some("stop what holds 0.0.0.0:443, or change mock_imds_listen in the configuration")
        );
    }

    #[test]
    fn main_error_messages_and_fixes_follow_the_design() {
        let cases: Vec<(MainError, &str, &str)> = vec![
            (
                MainError::ReadConfig {
                    path: PathBuf::from("/etc/gang/gang.toml"),
                    source: io_cause(),
                },
                "`/etc/gang/gang.toml`",
                "--config",
            ),
            (MainError::NotLocalMode, "[provisioner.local]", "--config"),
            (
                MainError::UnknownProvisioner {
                    name: "gcp".to_owned(),
                    accepted: "aws, local".to_owned(),
                },
                "`gcp`",
                "aws, local",
            ),
            (
                MainError::Bind {
                    role: ListenerRole::Public,
                    address: address(),
                    source: io_cause(),
                },
                "`0.0.0.0:443`",
                "listen",
            ),
            (MainError::WorkloadLoginIncomplete, "provider", "--provider"),
            (MainError::RevocationTargetIncomplete, "revoke", "--serial"),
            (MainError::NoServer, "server", "--server"),
            (
                MainError::DashboardSecret(gang_config::SecretFileError::Empty {
                    path: PathBuf::from("/etc/gang/secret"),
                }),
                "client secret",
                "dashboard.login.client_secret_file",
            ),
        ];
        for (error, message_names, fix_names) in cases {
            let message = error.to_string();
            assert!(
                message.contains(message_names),
                "{message:?} does not name {message_names:?}"
            );
            let fix = error.fix().expect("the variant has a fix");
            assert!(
                fix.contains(fix_names),
                "{fix:?} does not name {fix_names:?}"
            );
        }
    }

    #[test]
    fn parse_config_names_the_file_and_leaves_the_cause_to_the_chain() {
        let error = MainError::ParseConfig {
            path: PathBuf::from("/etc/gang/gang.toml"),
            source: config_error(),
        };
        assert_eq!(
            error.to_string(),
            "invalid configuration `/etc/gang/gang.toml`"
        );
        assert!(error.source().is_some());
    }

    #[test]
    fn parse_config_fix_is_the_config_errors_fix() {
        let error = MainError::ParseConfig {
            path: PathBuf::from("/etc/gang/gang.toml"),
            source: config_error(),
        };
        assert_eq!(error.fix(), config_error().fix());
    }

    #[test]
    fn wrapped_errors_delegate_their_fix() {
        let client = MainError::Client(gang_client::ClientError::HomeUnknown);
        assert_eq!(client.fix(), gang_client::ClientError::HomeUnknown.fix());

        let worker_error = || gang_worker::WorkerError::SandboxProgram {
            program: PathBuf::from("bwrap"),
        };
        assert_eq!(
            MainError::Worker(worker_error()).fix(),
            worker_error().fix()
        );

        let local = MainError::Local(LocalCommandError::AlreadyRunning {
            lock: PathBuf::from("/tmp/server.lock"),
        });
        assert_eq!(local.fix().as_deref(), Some("gang local down"));
    }

    #[test]
    fn internal_failures_carry_no_fix() {
        assert_eq!(
            MainError::Sidecar(gang_spi::PortError::backend(io::Error::other("stopped"))).fix(),
            None
        );
        assert_eq!(MainError::Stdin(io_cause()).fix(), None);
        let mismatch = LocalCommandError::PinMismatch {
            derived: CaPin::new([1; 32]),
            announced: CaPin::new([2; 32]),
        };
        assert_eq!(mismatch.fix(), None);
    }

    #[test]
    fn main_error_display_never_contains_its_source() {
        let errors: Vec<MainError> = vec![
            MainError::ReadConfig {
                path: PathBuf::from("gang.toml"),
                source: io_cause(),
            },
            MainError::ParseConfig {
                path: PathBuf::from("gang.toml"),
                source: config_error(),
            },
            MainError::Plugin {
                domain: "signer",
                source: gang_spi::PortError::config(SOURCE_TEXT),
            },
            MainError::Bind {
                role: ListenerRole::Worker,
                address: address(),
                source: io_cause(),
            },
            MainError::LocalAddress(io_cause()),
            MainError::ListenerUrl(gang_config::ListenerUrlError(SOURCE_TEXT.to_owned())),
            MainError::Signals(io_cause()),
            MainError::Runtime(io_cause()),
            MainError::Logging(Box::new(io_cause())),
            MainError::CurrentExecutable(io_cause()),
            MainError::ResourceName(gang_wire::ResourceNameError::MalformedId),
            MainError::Stdout(io_cause()),
            MainError::Stdin(io_cause()),
            MainError::CaCertificate(gang_os::PathIo::new(
                "write",
                std::path::Path::new("ca.pem"),
                io_cause(),
            )),
            MainError::DashboardSecret(gang_config::SecretFileError::Empty {
                path: PathBuf::from("secret"),
            }),
        ];
        gang_testkit::assert_sources_not_repeated(&errors);
        for error in errors {
            assert!(error.source().is_some(), "the variant keeps its source");
            let message = error.to_string();
            assert!(
                message.starts_with(|first: char| first.is_ascii_lowercase()),
                "{message:?} does not start lowercase"
            );
        }
    }

    #[test]
    fn local_messages_and_fixes_follow_the_design() {
        let lock = PathBuf::from("/tmp/gang-local/server/server.lock");
        let log = PathBuf::from("/tmp/gang-local/server/server.log");
        let cases: Vec<(LocalCommandError, &str, &str)> = vec![
            (
                LocalCommandError::AlreadyRunning { lock: lock.clone() },
                "`/tmp/gang-local/server/server.lock`",
                "gang local down",
            ),
            (
                LocalCommandError::StartupTimedOut {
                    awaited: "ca-pin",
                    timeout: Duration::from_secs(30),
                    log: log.clone(),
                },
                "30s",
                "/tmp/gang-local/server/server.log",
            ),
            (
                LocalCommandError::StopTimedOut {
                    pid: 48213,
                    timeout: Duration::from_millis(1500),
                },
                "48213",
                "48213",
            ),
            (
                LocalCommandError::ServerExited { log },
                "exited",
                "/tmp/gang-local/server/server.log",
            ),
            (
                LocalCommandError::UnrecordedServer { lock },
                "`/tmp/gang-local/server/server.lock`",
                "lock",
            ),
            (
                LocalCommandError::StateDirNotPrivate {
                    path: PathBuf::from("/tmp/gang-local"),
                    mode: 0o755,
                },
                "`0755`",
                "chmod 700 /tmp/gang-local",
            ),
            (
                LocalCommandError::InsecureNeedsLoopback {
                    what: "listen".to_owned(),
                    value: "10.0.0.1:50051".to_owned(),
                },
                "`listen` is `10.0.0.1:50051`",
                "listen",
            ),
            (
                LocalCommandError::PlaintextNeedsInsecure,
                "--insecure",
                "transport",
            ),
        ];
        for (error, message_names, fix_names) in cases {
            let message = error.to_string();
            assert!(
                message.contains(message_names),
                "{message:?} does not name {message_names:?}"
            );
            let actual = error.fix().expect("the variant has a fix");
            assert!(
                actual.contains(fix_names),
                "{actual:?} does not name {fix_names:?}"
            );
            assert_single_line(&actual);
        }
    }

    #[test]
    fn endpoint_rewrite_errors_quote_the_endpoint_and_scheme() {
        let missing_host = LocalCommandError::EndpointWithoutHost {
            endpoint: "file:///tmp/socket".to_owned(),
        };
        assert_eq!(
            missing_host.to_string(),
            "cannot rewrite the endpoint `file:///tmp/socket` without a host"
        );

        let invalid_scheme = LocalCommandError::EndpointScheme {
            endpoint: "https://example.com".to_owned(),
            scheme: "http",
            source: url::ParseError::EmptyHost,
        };
        assert_eq!(
            invalid_scheme.to_string(),
            "cannot rewrite the endpoint `https://example.com` under the `http` scheme"
        );
    }

    #[test]
    fn every_main_fix_is_one_line() {
        let fixes = [
            MainError::NotLocalMode.fix(),
            MainError::WorkloadLoginIncomplete.fix(),
            MainError::RevocationTargetIncomplete.fix(),
            MainError::ReadConfig {
                path: PathBuf::from("gang.toml"),
                source: io_cause(),
            }
            .fix(),
        ];
        for fix in fixes {
            assert_single_line(&fix.expect("the variant has a fix"));
        }
    }

    #[test]
    fn local_display_never_contains_its_source() {
        let errors: Vec<LocalCommandError> = vec![
            LocalCommandError::Io(gang_os::PathIo::new(
                "read",
                &PathBuf::from("server.json"),
                io_cause(),
            )),
            LocalCommandError::Spawn(gang_os::SpawnError::Spawn {
                program: PathBuf::from("gang"),
                source: io_cause(),
            }),
            LocalCommandError::Executable(io_cause()),
            LocalCommandError::Stdin(io_cause()),
            LocalCommandError::Render(config_error()),
            LocalCommandError::Provisioner(gang_spi::PortError::config(SOURCE_TEXT)),
            LocalCommandError::Signal {
                pid: 7,
                source: io::Error::from(io::ErrorKind::PermissionDenied),
            },
        ];
        gang_testkit::assert_sources_not_repeated(&errors);
        for error in errors {
            assert!(error.source().is_some(), "the variant keeps its source");
            assert_eq!(error.fix(), None);
        }
    }
}
