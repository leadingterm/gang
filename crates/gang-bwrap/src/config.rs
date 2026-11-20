//! The `[sandbox.bwrap]` section of `gang.toml`.

use std::path::PathBuf;

use gang_spi::SandboxPrograms;
use gang_types::AbsolutePathBuf;
use serde::{Deserialize, Serialize};

/// Configuration of the bubblewrap sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BwrapConfig {
    /// Absolute path to the rootfs envelope containing `root` and `identity`.
    pub root: AbsolutePathBuf,
    /// Whether an action requesting network access may share the host network.
    pub permit_network_property: bool,
    /// The bubblewrap program, resolved through the worker's PATH. Default: `bwrap`.
    #[serde(default = "default_program")]
    pub program: PathBuf,
}

fn default_program() -> PathBuf {
    PathBuf::from("bwrap")
}

impl SandboxPrograms for BwrapConfig {
    fn try_for_each_program<E>(
        &mut self,
        visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        visit(&mut self.program)
    }

    fn try_for_each_root<E>(
        &mut self,
        visit: &mut impl FnMut(&mut AbsolutePathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        visit(&mut self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::BwrapConfig;
    use gang_spi::SandboxPrograms;
    use gang_types::AbsolutePathBuf;

    #[test]
    fn root_visitor_updates_serialized_configuration() {
        let mut config: BwrapConfig =
            toml::from_str("root = '/rootfs'\npermit_network_property = false").expect("config");
        config
            .try_for_each_root(&mut |root| -> Result<(), std::convert::Infallible> {
                *root = AbsolutePathBuf::rooted("/pinned/rootfs");
                Ok(())
            })
            .expect("visitor");
        let serialized = toml::to_string(&config).expect("serialize");
        let parsed: BwrapConfig = toml::from_str(&serialized).expect("parse");
        assert_eq!(
            parsed.root.as_path(),
            std::path::Path::new("/pinned/rootfs")
        );
    }

    #[test]
    fn requires_absolute_root_and_rejects_legacy_keys() {
        assert!(toml::from_str::<BwrapConfig>("permit_network_property = false").is_err());
        assert!(
            toml::from_str::<BwrapConfig>("root = 'relative'\npermit_network_property = false")
                .is_err()
        );
        for old in [
            "readonly_bind_paths = []",
            "readonly_bind_mounts = {}",
            "tmp_path = '/tmp'",
        ] {
            let input = format!("root = '/rootfs'\npermit_network_property = false\n{old}");
            assert!(toml::from_str::<BwrapConfig>(&input).is_err(), "{old}");
        }
        let config: BwrapConfig =
            toml::from_str("root = '/rootfs'\npermit_network_property = false")
                .expect("valid config");
        assert_eq!(config.root.as_path(), std::path::Path::new("/rootfs"));
        assert_eq!(config.program, std::path::Path::new("bwrap"));
    }
}
