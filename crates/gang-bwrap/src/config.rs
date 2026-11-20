//! The `[sandbox.bwrap]` section of `gang.toml`.

use std::path::PathBuf;

use gang_spi::SandboxPrograms;
use gang_types::{AbsolutePathBuf, Capability};
use serde::{Deserialize, Serialize};

/// Configuration of the bubblewrap sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BwrapConfig {
    /// Absolute path to the rootfs envelope containing `root` and `identity`.
    pub root: AbsolutePathBuf,
    /// Programs needed for isolated public egress, or none when egress is unavailable.
    #[serde(default)]
    pub egress: Option<EgressPrograms>,
    /// The bubblewrap program, resolved through the worker's PATH. Default: `bwrap`.
    #[serde(default = "default_program")]
    pub program: PathBuf,
}

/// Host programs used to configure a public-egress network namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EgressPrograms {
    /// User-mode network stack.
    pub slirp4netns: PathBuf,
    /// Firewall configuration program.
    pub nft: PathBuf,
    /// Namespace entry program.
    pub nsenter: PathBuf,
    /// Host interface inventory program.
    pub ip: PathBuf,
}

impl Default for EgressPrograms {
    fn default() -> Self {
        Self {
            slirp4netns: PathBuf::from("slirp4netns"),
            nft: PathBuf::from("nft"),
            nsenter: PathBuf::from("nsenter"),
            ip: PathBuf::from("ip"),
        }
    }
}

/// The default `bwrap` program, resolved through the worker's `PATH`.
fn default_program() -> PathBuf {
    PathBuf::from("bwrap")
}

impl SandboxPrograms for BwrapConfig {
    fn supports_capability(&self, capability: Capability) -> bool {
        matches!(capability, Capability::Network) && self.egress.is_some()
    }

    fn try_for_each_program<E>(
        &mut self,
        visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        visit(&mut self.program)?;
        if let Some(egress) = &mut self.egress {
            visit(&mut egress.slirp4netns)?;
            visit(&mut egress.nft)?;
            visit(&mut egress.nsenter)?;
            visit(&mut egress.ip)?;
        }
        Ok(())
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
    use std::convert::Infallible;
    use std::path::PathBuf;

    use super::{BwrapConfig, EgressPrograms};
    use gang_spi::SandboxPrograms;
    use gang_types::{AbsolutePathBuf, Capability};

    #[test]
    fn egress_defaults_only_when_present() {
        let without: BwrapConfig = toml::from_str("root = '/rootfs'").unwrap();
        let with: BwrapConfig = toml::from_str("root = '/rootfs'\n[egress]").unwrap();
        assert!(!without.supports_capability(Capability::Network));
        assert!(with.supports_capability(Capability::Network));
        assert_eq!(with.egress, Some(EgressPrograms::default()));
        assert!(toml::from_str::<BwrapConfig>("root = '/rootfs'\n[egress]\nunknown = 1").is_err());
        assert!(
            toml::from_str::<BwrapConfig>("root = '/rootfs'\npermit_network_property = true")
                .is_err()
        );
    }

    #[test]
    fn round_trips_egress_configuration() {
        let config = BwrapConfig {
            root: AbsolutePathBuf::rooted("/rootfs"),
            egress: Some(EgressPrograms::default()),
            program: PathBuf::from("/opt/bwrap"),
        };
        let rendered = toml::to_string(&config).expect("config renders");
        let parsed: BwrapConfig = toml::from_str(&rendered).expect("rendered config parses");
        assert_eq!(parsed, config);
    }

    #[test]
    fn visits_and_rewrites_bwrap_program() {
        let mut config: BwrapConfig = toml::from_str("root = '/rootfs'").expect("config");
        let mut visited = Vec::new();
        config
            .try_for_each_program(&mut |program| -> Result<(), Infallible> {
                visited.push(program.clone());
                *program = PathBuf::from("/resolved/bwrap");
                Ok(())
            })
            .expect("visitor");
        assert_eq!(visited, [PathBuf::from("bwrap")]);
        assert_eq!(config.program, PathBuf::from("/resolved/bwrap"));
    }

    #[test]
    fn root_visitor_updates_serialized_configuration() {
        let mut config: BwrapConfig = toml::from_str("root = '/rootfs'").expect("config");
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
        assert!(toml::from_str::<BwrapConfig>("").is_err());
        assert!(toml::from_str::<BwrapConfig>("root = 'relative'").is_err());
        for old in [
            "readonly_bind_paths = []",
            "readonly_bind_mounts = {}",
            "tmp_path = '/tmp'",
            "permit_network_property = false",
        ] {
            let input = format!("root = '/rootfs'\n{old}");
            assert!(toml::from_str::<BwrapConfig>(&input).is_err(), "{old}");
        }
        let config: BwrapConfig = toml::from_str("root = '/rootfs'").expect("valid config");
        assert_eq!(config.root.as_path(), std::path::Path::new("/rootfs"));
        assert_eq!(config.program, std::path::Path::new("bwrap"));
    }

    #[test]
    fn visits_and_rewrites_every_egress_helper() {
        let mut config: BwrapConfig = toml::from_str("root = '/rootfs'\n[egress]").unwrap();
        let mut visited = Vec::new();
        config
            .try_for_each_program(&mut |program| -> Result<(), Infallible> {
                visited.push(program.clone());
                *program = PathBuf::from("/resolved").join(&*program);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            visited,
            ["bwrap", "slirp4netns", "nft", "nsenter", "ip"].map(PathBuf::from)
        );
        let egress = config.egress.unwrap();
        assert_eq!(egress.nft, PathBuf::from("/resolved/nft"));
        assert_eq!(egress.ip, PathBuf::from("/resolved/ip"));
    }
}
