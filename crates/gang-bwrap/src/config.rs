//! The `[sandbox.bwrap]` section of `gang.toml`.

use std::path::PathBuf;

use gang_spi::SandboxPrograms;
use gang_types::AbsolutePathBuf;
use serde::{Deserialize, Serialize};

/// Configuration of the bubblewrap sandbox. `readonly_bind_paths` and `permit_network_property`
/// are required; `program` and `tmp_path` default as documented on their fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BwrapConfig {
    /// Host paths made visible read-only inside the sandbox at the same location. Each must be
    /// absolute; a relative path is rejected when the configuration is parsed.
    pub readonly_bind_paths: Vec<AbsolutePathBuf>,
    /// Whether an action that requests network access is given the host network. When `false`,
    /// the network request is ignored and the action runs without network.
    pub permit_network_property: bool,
    /// The `bwrap` program. A relative name is resolved by `gang worker` through the worker's
    /// `PATH` when it renders the sandbox file, never by `gang exec`. Default: `bwrap`.
    #[serde(default = "default_program")]
    pub program: PathBuf,
    /// The absolute path mounted as a fresh, empty tmpfs inside the sandbox.
    /// A relative path is rejected when the configuration is parsed. Default: `/tmp`.
    #[serde(default = "default_tmp_path")]
    pub tmp_path: AbsolutePathBuf,
}

/// The default `bwrap` program, resolved through the worker's `PATH`.
fn default_program() -> PathBuf {
    PathBuf::from("bwrap")
}

/// The default tmpfs mount point inside the sandbox.
fn default_tmp_path() -> AbsolutePathBuf {
    AbsolutePathBuf::rooted("/tmp")
}

impl SandboxPrograms for BwrapConfig {
    fn try_for_each_program<E>(
        &mut self,
        visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        visit(&mut self.program)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::path::PathBuf;

    use gang_spi::SandboxPrograms;
    use gang_types::AbsolutePathBuf;

    use super::BwrapConfig;

    const COMPLETE: &str = r#"
readonly_bind_paths = ["/usr", "/etc/ssl"]
permit_network_property = true
"#;

    fn abs(path: &str) -> AbsolutePathBuf {
        AbsolutePathBuf::try_new(path).expect("test path is absolute")
    }

    #[test]
    fn parses_required_keys_with_default_program_and_tmp_path() {
        let config: BwrapConfig = toml::from_str(COMPLETE).expect("complete section parses");

        assert_eq!(
            config,
            BwrapConfig {
                readonly_bind_paths: vec![abs("/usr"), abs("/etc/ssl")],
                permit_network_property: true,
                program: PathBuf::from("bwrap"),
                tmp_path: abs("/tmp"),
            }
        );
    }

    #[test]
    fn parses_program_and_tmp_path() {
        let config: BwrapConfig = toml::from_str(&format!(
            "{COMPLETE}program = \"/opt/bwrap/bin/bwrap\"\ntmp_path = \"/scratch/tmp\"\n"
        ))
        .expect("section with program and tmp_path parses");

        assert_eq!(config.program, PathBuf::from("/opt/bwrap/bin/bwrap"));
        assert_eq!(config.tmp_path, abs("/scratch/tmp"));
    }

    #[test]
    fn round_trips_through_toml() {
        let config = BwrapConfig {
            readonly_bind_paths: vec![abs("/usr")],
            permit_network_property: false,
            program: PathBuf::from("/opt/bwrap/bin/bwrap"),
            tmp_path: abs("/scratch/tmp"),
        };

        let rendered = toml::to_string(&config).expect("config renders");
        let parsed: BwrapConfig = toml::from_str(&rendered).expect("rendered config parses");

        assert_eq!(parsed, config);
    }

    #[test]
    fn rejects_unknown_key() {
        let rejected = toml::from_str::<BwrapConfig>(&format!("{COMPLETE}unexpected = 1\n"));

        let error = rejected.expect_err("unknown key is rejected");
        assert!(
            error.message().contains("unexpected"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn rejects_missing_readonly_bind_paths() {
        let rejected = toml::from_str::<BwrapConfig>("permit_network_property = false\n");

        let error = rejected.expect_err("missing key is rejected");
        assert!(
            error.message().contains("readonly_bind_paths"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn rejects_missing_permit_network_property() {
        let rejected = toml::from_str::<BwrapConfig>("readonly_bind_paths = []\n");

        let error = rejected.expect_err("missing key is rejected");
        assert!(
            error.message().contains("permit_network_property"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn rejects_a_relative_bind_path() {
        let rejected = toml::from_str::<BwrapConfig>(
            "readonly_bind_paths = [\"relative\"]\npermit_network_property = false\n",
        );

        rejected.expect_err("relative bind path is rejected");
    }

    #[test]
    fn rejects_a_relative_tmp_path() {
        let rejected =
            toml::from_str::<BwrapConfig>(&format!("{COMPLETE}tmp_path = \"relative\"\n"));

        rejected.expect_err("relative tmp path is rejected");
    }

    #[test]
    fn visits_exactly_its_program_and_observes_a_rewrite() {
        let mut config = BwrapConfig {
            readonly_bind_paths: vec![abs("/usr")],
            permit_network_property: false,
            program: PathBuf::from("bwrap"),
            tmp_path: abs("/tmp"),
        };
        let mut visited = Vec::new();

        config
            .try_for_each_program(&mut |program: &mut PathBuf| -> Result<(), Infallible> {
                visited.push(program.clone());
                *program = PathBuf::from("/usr/bin/bwrap");
                Ok(())
            })
            .expect("visiting a program cannot fail");

        assert_eq!(visited, [PathBuf::from("bwrap")]);
        assert_eq!(config.program, PathBuf::from("/usr/bin/bwrap"));
    }
}
