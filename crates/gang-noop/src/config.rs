//! The `[sandbox.noop]` section.

use std::path::PathBuf;

use gang_spi::SandboxPrograms;
use gang_types::{AbsolutePathBuf, ActionEnvironment};
use serde::{Deserialize, Serialize};

/// The `[sandbox.noop]` section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoopConfig {
    /// The operator-asserted host environment identity.
    pub environment: ActionEnvironment,
    /// The absolute directory under which each action's `TMPDIR` is created. When absent, the
    /// system temporary directory is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_dir: Option<AbsolutePathBuf>,
}

impl SandboxPrograms for NoopConfig {
    fn supports_capability(&self, _capability: gang_types::Capability) -> bool {
        false
    }

    fn try_for_each_program<E>(
        &mut self,
        _visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }

    fn try_for_each_root<E>(
        &mut self,
        _visit: &mut impl FnMut(&mut AbsolutePathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::path::PathBuf;

    use gang_spi::SandboxPrograms;
    use gang_types::AbsolutePathBuf;

    use super::NoopConfig;

    #[test]
    fn requires_environment_and_defaults_scratch_dir() -> Result<(), Box<dyn std::error::Error>> {
        assert!(toml::from_str::<NoopConfig>("").is_err());
        let config: NoopConfig = toml::from_str("environment = \"host:test\"")?;
        assert_eq!(config.environment.as_str(), "host:test");
        assert_eq!(config.scratch_dir, None);
        Ok(())
    }

    #[test]
    fn rejects_invalid_environment() {
        assert!(toml::from_str::<NoopConfig>("environment = \"host:\"").is_err());
    }

    #[test]
    fn parses_scratch_dir() -> Result<(), Box<dyn std::error::Error>> {
        let config: NoopConfig =
            toml::from_str("environment = \"host:test\"\nscratch_dir = \"/var/scratch\"")?;

        assert_eq!(
            config.scratch_dir,
            Some(AbsolutePathBuf::try_new("/var/scratch")?)
        );
        Ok(())
    }

    #[test]
    fn rejects_relative_scratch_dir() {
        assert!(
            toml::from_str::<NoopConfig>("environment = \"host:test\"\nscratch_dir = \"relative\"")
                .is_err()
        );
    }

    #[test]
    fn omits_absent_scratch_dir() -> Result<(), Box<dyn std::error::Error>> {
        let config: NoopConfig = toml::from_str("environment = \"host:test\"")?;
        let rendered = toml::to_string(&config)?;
        assert!(!rendered.contains("scratch_dir"));
        Ok(())
    }

    #[test]
    fn round_trips_through_toml_with_scratch_dir() -> Result<(), Box<dyn std::error::Error>> {
        let config = NoopConfig {
            environment: "host:test".parse()?,
            scratch_dir: Some(AbsolutePathBuf::try_new("/var/scratch")?),
        };
        let rendered = toml::to_string(&config)?;
        let parsed: NoopConfig = toml::from_str(&rendered)?;

        assert_eq!(parsed, config);
        Ok(())
    }

    #[test]
    fn rejects_unknown_keys() {
        let rejected = toml::from_str::<NoopConfig>("environment = \"host:test\"\nextra = true");

        assert!(rejected.is_err());
    }

    #[test]
    fn noop_cannot_grant_network() -> Result<(), Box<dyn std::error::Error>> {
        let config: NoopConfig = toml::from_str("environment = 'host:test'")?;
        assert!(!config.supports_capability(gang_types::Capability::Network));
        Ok(())
    }

    #[test]
    fn visits_no_programs() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = NoopConfig {
            environment: "host:test".parse()?,
            scratch_dir: Some(AbsolutePathBuf::try_new("/var/scratch")?),
        };
        let mut visits = 0;

        config.try_for_each_program(&mut |_: &mut PathBuf| -> Result<(), Infallible> {
            visits += 1;
            Ok(())
        })?;

        assert_eq!(visits, 0);
        Ok(())
    }
}
