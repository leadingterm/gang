use gang_types::{InstanceId, NameError};
use std::path::PathBuf;

/// Every failure the local provisioner can report. Converted into `PortError` at the port boundary.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum LocalError {
    /// A file or directory under the state dir could not be read or written.
    #[error(transparent)]
    #[failure(unavailable)]
    Io(#[from] gang_os::PathIo),
    /// A JSON state file exists but does not decode.
    #[error("invalid state file {}", .path.display())]
    #[failure(backend)]
    State {
        /// The state file that failed to decode.
        path: PathBuf,
        /// The decode error.
        #[source]
        source: serde_json::Error,
    },
    /// A signing key file exists but is not a valid PKCS#8 P-256 private key.
    #[error("invalid key file {}", .path.display())]
    #[failure(backend)]
    Key {
        /// The key file that failed to decode or encode.
        path: PathBuf,
    },
    /// The workload token signing key is not a valid P-256 key.
    #[error("workload token key is not a valid P-256 key")]
    #[failure(backend)]
    WorkloadKey,
    /// A workload token could not be signed.
    #[error("cannot sign a workload token")]
    #[failure(backend)]
    Sign(#[source] serde_json::Error),
    /// A JSON document could not be serialized, for example while building a token.
    #[error("cannot encode JSON")]
    #[failure(backend)]
    Encode(#[source] serde_json::Error),
    /// A path has to be rendered into a UTF-8 store specification.
    #[error("path {} is not valid UTF-8", .0.display())]
    #[failure(backend)]
    NonUtf8Path(PathBuf),
    /// A worker process could not be started.
    #[error(transparent)]
    #[failure(backend)]
    Spawn(#[from] gang_os::SpawnError),
    /// Sending a signal to a worker process failed for a reason other than it being gone.
    #[error("cannot stop process {pid}")]
    #[failure(unavailable)]
    Signal {
        /// The process that could not be signalled.
        pid: u32,
        /// The OS error from the signal call.
        #[source]
        source: std::io::Error,
    },
    /// The running executable, which workers are launched from, could not be located.
    #[error("cannot locate the running executable")]
    #[failure(backend)]
    CurrentExecutable(#[source] std::io::Error),
    /// No instance record exists for the requested id.
    #[error("instance {0} does not exist")]
    #[failure(not_found(Instance))]
    UnknownInstance(InstanceId),
    /// A string could not be turned into a valid instance name.
    #[error("cannot name instance")]
    #[failure(backend)]
    Name(#[from] NameError),
    /// A request to the mock IMDS server failed at the transport, body, or URI level.
    #[error("mock imds request failed")]
    #[failure(unavailable)]
    Imds(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The mock IMDS server answered with a non-success status.
    #[error("mock imds answered {status}")]
    #[failure(unavailable)]
    ImdsStatus {
        /// The HTTP status code returned by the server.
        status: u16,
    },
    /// The mock metadata service could not bind its listener, report its address, or serve.
    #[error("mock metadata service failed")]
    #[failure(unavailable)]
    MockImdsListen(#[source] std::io::Error),
    /// The process was started without the environment that marks it as a worker.
    #[error("this process is not a local worker: {variable} is not set")]
    #[failure(config)]
    NotAWorker {
        /// The environment variable that is missing.
        variable: &'static str,
    },
    /// The OS random source could not provide bytes.
    #[error("cannot read the OS random source: {0}")]
    #[failure(backend)]
    Random(String),
    /// The provisioner was already attached to a server.
    #[error("the local provisioner is already attached to a server")]
    #[failure(config)]
    AlreadyAttached,
    /// A directory the configuration names exists but is not private; it was left unchanged.
    #[error(transparent)]
    #[failure(config)]
    PrivateDir(#[from] gang_os::fs::PrivateDirError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_spi::{PortError, Subject};
    use std::error::Error as _;
    use std::path::Path;

    fn io_error(action: &'static str) -> gang_os::PathIo {
        gang_os::PathIo::new(action, Path::new("state"), std::io::Error::other("disk"))
    }

    #[test]
    fn unknown_instance_maps_to_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let id = InstanceId::try_new("local-7".to_owned())?;
        let port = PortError::from(LocalError::UnknownInstance(id.clone()));
        assert!(matches!(&port, PortError::NotFound(Subject::Instance(found)) if *found == id));
        Ok(())
    }

    #[test]
    fn transient_failures_map_to_unavailable() {
        let io = LocalError::Io(io_error("read"));
        assert!(matches!(PortError::from(io), PortError::Unavailable(_)));
        let status = LocalError::ImdsStatus { status: 503 };
        assert!(matches!(PortError::from(status), PortError::Unavailable(_)));
        let signal = LocalError::Signal {
            pid: 42,
            source: std::io::Error::other("gone"),
        };
        assert!(matches!(PortError::from(signal), PortError::Unavailable(_)));
        let listen = LocalError::MockImdsListen(std::io::Error::other("address in use"));
        assert!(matches!(PortError::from(listen), PortError::Unavailable(_)));
    }

    #[test]
    fn not_a_worker_maps_to_config() {
        let error = LocalError::NotAWorker {
            variable: "GANG_LOCAL_INSTANCE",
        };
        match PortError::from(error) {
            PortError::Config(cause) => {
                assert!(cause.to_string().contains("GANG_LOCAL_INSTANCE"));
            }
            other => panic!("expected a configuration error, got {other:?}"),
        }
    }

    #[test]
    fn already_attached_maps_to_config() {
        match PortError::from(LocalError::AlreadyAttached) {
            PortError::Config(cause) => {
                assert_eq!(
                    cause.to_string(),
                    "the local provisioner is already attached to a server"
                );
            }
            other => panic!("expected a configuration error, got {other:?}"),
        }
    }

    #[test]
    fn private_dir_refusal_maps_to_config() {
        let error = LocalError::PrivateDir(gang_os::fs::PrivateDirError::NotPrivate {
            path: PathBuf::from("state"),
            mode: 0o755,
        });
        match PortError::from(error) {
            PortError::Config(cause) => {
                assert!(cause.to_string().contains("state has mode 0755"));
            }
            other => panic!("expected a configuration error, got {other:?}"),
        }
    }

    #[test]
    fn everything_else_maps_to_backend() {
        assert!(matches!(
            PortError::from(LocalError::Random("no entropy".to_owned())),
            PortError::Backend(_)
        ));
        assert!(matches!(
            PortError::from(LocalError::Key {
                path: PathBuf::from("ca.key"),
            }),
            PortError::Backend(_)
        ));
        assert!(matches!(
            PortError::from(LocalError::WorkloadKey),
            PortError::Backend(_)
        ));
        assert!(matches!(
            PortError::from(LocalError::Sign(serde_json::Error::io(
                std::io::Error::other("encoding failed")
            ))),
            PortError::Backend(_)
        ));
        assert!(matches!(
            PortError::from(LocalError::CurrentExecutable(std::io::Error::other(
                "no path"
            ))),
            PortError::Backend(_)
        ));
    }

    #[test]
    fn io_error_is_transparent() {
        let error = LocalError::Io(io_error("read"));
        assert!(error.source().is_some());
        assert_eq!(error.to_string(), "cannot read state");
    }
}
