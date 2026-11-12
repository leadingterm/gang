//! The `[sandbox.noop]` section.

use std::path::PathBuf;

use gang_spi::SandboxPrograms;
use gang_types::AbsolutePathBuf;
use serde::{Deserialize, Serialize};

/// The `[sandbox.noop]` section.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoopConfig {
    /// The absolute directory under which each action's `TMPDIR` is created. When absent, the
    /// system temporary directory is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_dir: Option<AbsolutePathBuf>,
}

impl SandboxPrograms for NoopConfig {
    fn try_for_each_program<E>(
        &mut self,
        _visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
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
    fn parses_empty_section_with_default_scratch_dir() {
        let config: NoopConfig = toml::from_str("").expect("an empty noop section parses");

        assert_eq!(config.scratch_dir, None);
    }

    #[test]
    fn parses_scratch_dir() {
        let config: NoopConfig = toml::from_str("scratch_dir = \"/var/scratch\"")
            .expect("a noop section with a scratch directory parses");

        assert_eq!(
            config.scratch_dir,
            Some(AbsolutePathBuf::try_new("/var/scratch").expect("absolute"))
        );
    }

    #[test]
    fn rejects_relative_scratch_dir() {
        assert!(toml::from_str::<NoopConfig>("scratch_dir = \"relative\"").is_err());
    }

    #[test]
    fn omits_absent_scratch_dir() {
        let rendered = toml::to_string(&NoopConfig::default()).expect("serializes");
        assert!(!rendered.contains("scratch_dir"));
    }

    #[test]
    fn round_trips_through_toml_with_scratch_dir() {
        let config = NoopConfig {
            scratch_dir: Some(AbsolutePathBuf::try_new("/var/scratch").expect("absolute")),
        };
        let rendered = toml::to_string(&config).expect("the section renders");
        let parsed: NoopConfig = toml::from_str(&rendered).expect("the rendered section parses");

        assert_eq!(parsed, config);
    }

    #[test]
    fn rejects_unknown_keys() {
        let rejected = toml::from_str::<NoopConfig>("extra = true");

        assert!(rejected.is_err());
    }

    #[test]
    fn visits_no_programs() {
        let mut config = NoopConfig {
            scratch_dir: Some(AbsolutePathBuf::try_new("/var/scratch").expect("absolute")),
        };
        let mut visits = 0;

        config
            .try_for_each_program(&mut |_: &mut PathBuf| -> Result<(), Infallible> {
                visits += 1;
                Ok(())
            })
            .expect("visiting no programs cannot fail");

        assert_eq!(visits, 0);
    }
}
