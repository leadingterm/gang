use gang_aws_sdk::CallError;
use gang_os::PathIo;
use gang_spi::PortFailure;
use gang_types::FleetName;
use std::path::PathBuf;

/// Every failure the AWS adapters can report. Converted into `PortError` at the port boundary.
#[derive(Debug, thiserror::Error, PortFailure)]
pub enum AwsError {
    /// An AWS SDK call failed.
    #[error(transparent)]
    #[failure(transparent)]
    Call(#[from] CallError),
    /// The permission to change the fleet has expired.
    #[error(transparent)]
    #[failure(transparent)]
    LeaseLapsed(#[from] gang_spi::LeaseLapsed),
    /// The configured auto scaling group does not exist.
    #[error("auto scaling group {0} does not exist")]
    #[failure(config)]
    MissingGroup(FleetName),
    /// An AWS response lacked a field the adapter needs, or carried an unexpected value.
    #[error("{operation} returned no {what}")]
    MalformedResponse {
        /// The AWS operation whose response was incomplete.
        operation: &'static str,
        /// The response content that is missing or unexpected.
        what: &'static str,
    },
    /// A configuration value cannot be used.
    #[error("invalid {field}: {reason}")]
    #[failure(config)]
    Config {
        /// The configuration field that is invalid.
        field: &'static str,
        /// Why the value is invalid.
        reason: String,
    },
    /// The KMS key cannot be used to sign certificates.
    #[error("kms key {key_id} cannot be used as the certificate authority key: {reason}")]
    #[failure(config)]
    UnsuitableKey {
        /// The key id, key ARN or alias that was configured.
        key_id: String,
        /// Which property of the key rules it out.
        reason: &'static str,
    },
    /// The scratch volume is not usable.
    #[error(transparent)]
    Scratch(#[from] ScratchError),
}

impl AwsError {
    /// A response that lacks the named content.
    pub(crate) fn malformed(operation: &'static str, what: &'static str) -> Self {
        Self::MalformedResponse { operation, what }
    }
}

/// Why the scratch volume cannot be adopted. All messages are lowercase.
#[derive(Debug, thiserror::Error)]
pub enum ScratchError {
    /// The configured path is not a mount point, so no volume has been prepared there.
    #[error("{} is not a mount point; the scratch volume has not been prepared", .path.display())]
    NotMounted {
        /// The configured mount point.
        path: PathBuf,
    },
    /// The mounted filesystem is not xfs.
    #[error("{} is not an xfs filesystem", .path.display())]
    NotXfs {
        /// The configured mount point.
        path: PathBuf,
    },
    /// The filesystem cannot clone file data by reference.
    #[error("the filesystem at {} does not support reflinks", .path.display())]
    NoReflink {
        /// The configured mount point.
        path: PathBuf,
    },
    /// The filesystem could not be inspected.
    #[error("cannot inspect {}: {source}", .path.display())]
    Probe {
        /// The configured mount point.
        path: PathBuf,
        /// The operating system error from the probe.
        source: std::io::Error,
    },
    /// The space available on the scratch volume could not be read.
    #[error("cannot read the scratch volume's size")]
    Space(#[from] PathIo),
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_spi::PortError;

    #[test]
    fn transient_maps_to_unavailable() {
        assert!(matches!(
            PortError::from(AwsError::from(CallError::Transient {
                operation: "Sign",
                source: Box::new(std::io::Error::other("reset")),
            })),
            PortError::Unavailable(_)
        ));
    }

    #[test]
    fn refused_maps_to_backend() {
        let refused = AwsError::from(CallError::Refused {
            operation: "Sign",
            source: Box::new(std::io::Error::other("denied")),
        });
        assert!(matches!(PortError::from(refused), PortError::Backend(_)));
    }

    #[test]
    fn malformed_response_maps_to_backend() {
        let malformed = AwsError::MalformedResponse {
            operation: "GetWebIdentityToken",
            what: "token",
        };
        assert!(matches!(PortError::from(malformed), PortError::Backend(_)));
    }

    #[test]
    fn scratch_maps_to_backend() {
        let scratch = AwsError::Scratch(ScratchError::NotXfs {
            path: PathBuf::from("/scratch"),
        });
        assert!(matches!(PortError::from(scratch), PortError::Backend(_)));
    }

    #[test]
    fn missing_group_maps_to_config_naming_the_group() -> Result<(), Box<dyn std::error::Error>> {
        let group = FleetName::try_new("gang-fleet")?;
        let port = PortError::from(AwsError::MissingGroup(group));
        assert!(matches!(
            &port,
            PortError::Config(cause) if cause.to_string().contains("gang-fleet")
        ));
        Ok(())
    }

    #[test]
    fn config_maps_to_config_naming_the_field() {
        let config = AwsError::Config {
            field: "region",
            reason: "must not be empty".to_owned(),
        };
        assert!(matches!(
            &PortError::from(config),
            PortError::Config(cause) if cause.to_string().contains("region")
        ));
    }

    #[test]
    fn unsuitable_key_maps_to_config_naming_the_key() {
        let key = AwsError::UnsuitableKey {
            key_id: "alias/signing".to_owned(),
            reason: "key usage is not SIGN_VERIFY",
        };
        assert!(matches!(
            &PortError::from(key),
            PortError::Config(cause) if cause.to_string().contains("alias/signing")
        ));
    }

    #[test]
    fn malformed_names_operation_and_content() {
        let malformed = AwsError::malformed("GetWebIdentityToken", "token");
        assert!(matches!(
            malformed,
            AwsError::MalformedResponse {
                operation: "GetWebIdentityToken",
                what: "token",
            }
        ));
        let text = malformed.to_string();
        assert!(text.contains("GetWebIdentityToken"));
        assert!(text.contains("token"));
    }
}
