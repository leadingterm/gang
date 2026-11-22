//! Restart-stable controller candidate identity for ECS tasks and standalone EC2.

use std::ffi::OsStr;

use gang_clock::Timestamp;
use gang_spi::{ControllerHostIdentity, ControllerIdentityPort, PortError};
use gang_types::InstanceId;

use crate::errors::AwsError;
use crate::plugin::Aws;

const CONTROLLER_IDENTITY: &str = "GANG_CONTROLLER_IDENTITY";
const CONTROLLER_LAUNCHED_AT: &str = "GANG_CONTROLLER_LAUNCHED_AT";

fn task_identity(
    identity: Option<&OsStr>,
    launched_at: Option<&OsStr>,
) -> Result<Option<ControllerHostIdentity>, PortError> {
    let (identity, launched_at) = match (identity, launched_at) {
        (None, None) => return Ok(None),
        (Some(identity), Some(launched_at)) => (identity, launched_at),
        _ => {
            return Err(AwsError::Config {
                field: CONTROLLER_IDENTITY,
                reason: "task identity and launch time must both be set".to_owned(),
            }
            .into());
        }
    };
    let invalid_identity = || AwsError::Config {
        field: CONTROLLER_IDENTITY,
        reason: "must be a 64-character hexadecimal task identity".to_owned(),
    };
    let identity = identity.to_str().ok_or_else(invalid_identity)?;
    if identity.len() != 64 || !identity.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_identity().into());
    }
    let instance = InstanceId::try_new(identity).map_err(|_| invalid_identity())?;
    let invalid_time = || AwsError::Config {
        field: CONTROLLER_LAUNCHED_AT,
        reason: "must be a valid RFC 3339 timestamp".to_owned(),
    };
    let time = launched_at.to_str().ok_or_else(invalid_time)?;
    let at = Timestamp::from_rfc3339(time).map_err(|_| invalid_time())?;
    Ok(Some(ControllerHostIdentity::Launched { at, instance }))
}

impl ControllerIdentityPort for Aws {
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError> {
        let identity = std::env::var_os(CONTROLLER_IDENTITY);
        let launched_at = std::env::var_os(CONTROLLER_LAUNCHED_AT);
        if let Some(identity) = task_identity(identity.as_deref(), launched_at.as_deref())? {
            return Ok(identity);
        }
        Ok(self.runtime().await?.metadata.incarnation().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_spi::CandidatePriority;

    #[test]
    fn task_identity_is_stable_and_newer_than_old_asg() -> Result<(), Box<dyn std::error::Error>> {
        let digest = "0123456789abcdef".repeat(4);
        let time = OsStr::new("2026-11-20T12:00:00Z");
        let identity =
            task_identity(Some(digest.as_ref()), Some(time))?.ok_or("missing identity")?;
        assert_eq!(
            identity,
            task_identity(Some(digest.as_ref()), Some(time))?.ok_or("missing identity")?
        );
        let ControllerHostIdentity::Launched { at, instance } = identity else {
            return Err("task identity must use launch priority".into());
        };
        let old_at = Timestamp::from_rfc3339("2026-11-19T12:00:00Z")?;
        let old_instance = InstanceId::try_new("i-old")?;
        assert!(
            CandidatePriority::launched(at, &instance)
                > CandidatePriority::launched(old_at, &old_instance)
        );
        Ok(())
    }

    #[test]
    fn incomplete_and_invalid_task_identity_is_rejected() {
        let digest = "a".repeat(64);
        let time = OsStr::new("2026-11-20T12:00:00Z");
        assert!(matches!(task_identity(None, None), Ok(None)));
        assert!(task_identity(Some(digest.as_ref()), None).is_err());
        assert!(task_identity(None, Some(time)).is_err());
        assert!(task_identity(Some(OsStr::new("abc")), Some(time)).is_err());
        assert!(task_identity(Some("g".repeat(64).as_ref()), Some(time)).is_err());
        assert!(task_identity(Some(digest.as_ref()), Some(OsStr::new("invalid"))).is_err());
    }
}
