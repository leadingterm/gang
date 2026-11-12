//! Errors of the worker library.

use std::path::PathBuf;

use gang_clock::Timestamp;
use gang_spi::PortError;
use gang_types::{Bytes, FleetName, Isa};

use crate::credentials::EnrollError;
use crate::transport::ServerCaError;

/// Why the worker cannot run or stopped running.
///
/// A message names the operation and the value and never repeats its source; the action that
/// helps is the `Fix` trait.
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub enum WorkerError {
    /// No fleet of the configuration has the name of this instance's fleet.
    #[error("no fleet named {0} is configured")]
    #[fix("set this instance's fleet to one declared in the configuration")]
    NoFleet(FleetName),
    /// The instance's fleet is configured for a different instruction set than the node runs.
    #[error("fleet `{fleet}` is configured for {configured} but this node runs {actual}")]
    #[fix("run the worker on a node of the fleet's instruction set")]
    IsaMismatch {
        /// The fleet of the instance.
        fleet: FleetName,
        /// The instruction set the fleet is configured for.
        configured: Isa,
        /// The instruction set the node runs on.
        actual: Isa,
    },
    /// The scratch volume leaves the worker's actions less disk than its fleet declares.
    #[error(
        "fleet `{fleet}` declares disk_per_instance {declared}, but this worker's scratch volume leaves its actions {measured}"
    )]
    #[fix(
        "lower disk_per_instance of fleet `{}` to at most {}, or give its instances a larger scratch volume",
        fleet,
        measured
    )]
    FleetDiskUnavailable {
        /// The fleet of the instance.
        fleet: FleetName,
        /// The fleet's declared `disk_per_instance`.
        declared: Bytes,
        /// The disk the scratch volume leaves the actions: its free space at mount plus the
        /// worker's own fast content store, less the fast content store's budget.
        measured: Bytes,
    },
    /// A provisioner, cache, or limits port failed.
    #[error("a provisioner, cache, or limits port failed")]
    Port(#[from] PortError),
    /// Enrollment with the certificate authority failed.
    #[error("cannot enroll with the certificate authority")]
    Enroll(#[from] EnrollError),
    /// The CA certificate of a server-only transport cannot be obtained.
    #[error(transparent)]
    #[fix(source)]
    ServerCa(#[from] ServerCaError),
    /// The certificate authority issued credentials that are already expired.
    #[error("the certificate authority issued a certificate that has already expired")]
    ExpiredCertificate {
        /// The expiration time reported by the authority.
        not_after: Timestamp,
    },
    /// The sandbox configuration cannot be rendered for the worker.
    #[error("invalid sandbox file: {0}")]
    Config(String),
    /// A sandbox program does not resolve to an executable file.
    #[error("sandbox program `{}` was not found as an executable file; searched the worker's PATH", program.display())]
    #[fix("install {} in the worker image, or set program in its [sandbox.<plugin>] section", program.display())]
    SandboxProgram {
        /// The program as configured.
        program: PathBuf,
    },
    /// The scheduler stopped the worker with an error.
    #[error("the worker stopped")]
    Scheduler(#[source] PortError),
    /// A filesystem operation on the scratch volume failed.
    #[error(transparent)]
    Io(#[from] gang_os::PathIo),
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::path::Path;

    use gang_os::PathIo;

    use super::*;

    const CAUSE: &str = "cause-marker";

    fn fleet(name: &str) -> FleetName {
        FleetName::try_new(name).expect("the fleet name is valid")
    }

    /// One value of every variant.
    fn every_variant() -> Vec<WorkerError> {
        vec![
            WorkerError::NoFleet(fleet("linux-arm64")),
            WorkerError::IsaMismatch {
                fleet: fleet("linux-arm64"),
                configured: Isa::Aarch64,
                actual: Isa::X86_64,
            },
            WorkerError::FleetDiskUnavailable {
                fleet: fleet("linux-arm64"),
                declared: Bytes::gib(10),
                measured: Bytes::gib(5),
            },
            WorkerError::Port(PortError::backend(CAUSE)),
            WorkerError::Enroll(EnrollError::new(io::Error::other(CAUSE))),
            WorkerError::ServerCa(ServerCaError::Missing),
            WorkerError::ServerCa(ServerCaError::Fetch(Box::new(io::Error::other(CAUSE)))),
            WorkerError::ExpiredCertificate {
                not_after: Timestamp::from_unix_millis(0),
            },
            WorkerError::Config("missing field".to_owned()),
            WorkerError::SandboxProgram {
                program: PathBuf::from("bwrap"),
            },
            WorkerError::Scheduler(PortError::backend(CAUSE)),
            WorkerError::Io(PathIo::new(
                "write file",
                Path::new("/x"),
                io::Error::other(CAUSE),
            )),
        ]
    }

    fn fix_of(error: &WorkerError) -> Option<String> {
        gang_types::Fix::fix(error).map(std::borrow::Cow::into_owned)
    }

    #[test]
    fn a_missing_server_ca_names_the_pin() {
        let error = WorkerError::ServerCa(ServerCaError::Missing);
        assert_eq!(
            error.to_string(),
            "the server advertised no CA certificate although ca_pin is set"
        );
        assert_eq!(
            fix_of(&error).as_deref(),
            Some("check server.ca_pin and the server's TLS setup")
        );
    }

    #[test]
    fn a_missing_sandbox_program_names_the_program() {
        let error = WorkerError::SandboxProgram {
            program: PathBuf::from("bwrap"),
        };

        assert_eq!(
            error.to_string(),
            "sandbox program `bwrap` was not found as an executable file; searched the worker's PATH"
        );
        assert_eq!(
            fix_of(&error).as_deref(),
            Some(
                "install bwrap in the worker image, or set program in its [sandbox.<plugin>] section"
            )
        );
    }

    #[test]
    fn a_fleet_mismatch_names_the_action() {
        assert_eq!(
            fix_of(&WorkerError::NoFleet(fleet("linux-arm64"))).as_deref(),
            Some("set this instance's fleet to one declared in the configuration")
        );
        assert_eq!(
            fix_of(&WorkerError::IsaMismatch {
                fleet: fleet("linux-arm64"),
                configured: Isa::Aarch64,
                actual: Isa::X86_64,
            })
            .as_deref(),
            Some("run the worker on a node of the fleet's instruction set")
        );
    }

    #[test]
    fn backend_failures_carry_no_fix() {
        for error in [
            WorkerError::Port(PortError::backend(CAUSE)),
            WorkerError::ServerCa(ServerCaError::Fetch(Box::new(io::Error::other(CAUSE)))),
            WorkerError::Scheduler(PortError::backend(CAUSE)),
            WorkerError::Config("missing field".to_owned()),
        ] {
            assert_eq!(fix_of(&error), None, "{error}");
        }
    }

    #[test]
    fn every_fix_is_one_line() {
        for error in every_variant() {
            if let Some(fix) = fix_of(&error) {
                assert!(!fix.is_empty(), "{error}");
                assert!(!fix.contains('\n'), "{error}");
            }
        }
    }

    #[test]
    fn no_message_repeats_its_source() {
        for error in every_variant() {
            if let Some(source) = error.source() {
                let cause = source.to_string();
                assert!(!cause.is_empty(), "{error}");
                assert!(!error.to_string().contains(&cause), "{error}: {cause}");
            }
        }
    }
}
