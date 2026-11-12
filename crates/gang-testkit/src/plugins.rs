//! The plugin sets the tests compose, and the standard `gang.toml` text both of them accept.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;

use gang_config::{FleetOptionsError, FleetOptionsTable, PluginSet, SandboxSelection};
use gang_types::{AuthenticationSettings, SchedulerSettings};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// An opaque plugin section: any TOML table, round-tripped unchanged. The test plugin sets use it
/// for every section whose contents the tests never read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Opaque(pub toml::Table);

impl AuthenticationSettings for Opaque {}

impl SchedulerSettings for Opaque {}

gang_types::string_enum! {
    /// Names one sandbox plugin by its section key.
    pub enum SandboxKey("sandbox key") {
        /// The `noop` sandbox, which runs no program.
        Noop = "noop",
        /// The `bwrap` sandbox, which runs actions under bubblewrap.
        Bwrap = "bwrap",
        /// The `boxed` sandbox, which runs the program its section names.
        Boxed = "boxed",
    }
}

/// The `[sandbox.boxed]` section: the program the boxed sandbox runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoxedSection {
    /// The program the boxed sandbox runs when its section names none, `fake-sandbox`.
    #[serde(default = "default_program")]
    pub program: PathBuf,
}

/// The program the boxed sandbox runs when its configuration names none.
fn default_program() -> PathBuf {
    PathBuf::from("fake-sandbox")
}

/// The `[sandbox.<plugin>]` sections of the test compositions.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    /// The `[sandbox.noop]` section, when configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noop: Option<Opaque>,
    /// The `[sandbox.bwrap]` section, when configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bwrap: Option<Opaque>,
    /// The `[sandbox.boxed]` section, when configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boxed: Option<BoxedSection>,
}

impl SandboxSelection for SandboxConfig {
    type Key = SandboxKey;

    fn is_configured(&self, key: SandboxKey) -> bool {
        match key {
            SandboxKey::Noop => self.noop.is_some(),
            SandboxKey::Bwrap => self.bwrap.is_some(),
            SandboxKey::Boxed => self.boxed.is_some(),
        }
    }

    fn try_for_each_program<E>(
        &mut self,
        key: SandboxKey,
        mut visit: impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        match key {
            SandboxKey::Noop | SandboxKey::Bwrap => Ok(()),
            SandboxKey::Boxed => match &mut self.boxed {
                Some(section) => visit(&mut section.program),
                None => Ok(()),
            },
        }
    }
}

/// The `[limits.<plugin>]` section of the test composition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestLimits {
    /// The most processes an action may start.
    pub max_pids: u32,
}

/// Test plugin sections, with configurable limits and typed sandbox configuration.
#[derive(Debug)]
pub struct TestPluginSet<L = TestLimits>(PhantomData<L>);

impl<L: DeserializeOwned + Serialize + Debug + Clone> PluginSet for TestPluginSet<L> {
    type Provisioner = Opaque;
    type Cache = Opaque;
    type Signer = Opaque;
    type Scheduler = Opaque;
    type Database = Opaque;
    type SandboxKey = SandboxKey;
    type Sandbox = SandboxConfig;
    type IdentityProvider = Opaque;
    type ClientAuthentication = Opaque;
    type WorkerAuthentication = Opaque;
    type Limits = L;
    type FleetOptions = Opaque;

    fn fleet_options(
        _provisioner: &Opaque,
        table: FleetOptionsTable,
    ) -> Result<Opaque, FleetOptionsError> {
        Ok(Opaque(table))
    }

    fn option_properties(_options: &Opaque) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

/// The test plugin set with typed limits.
pub type TestPlugins = TestPluginSet<TestLimits>;

/// The test plugin set with opaque limits.
pub type OpaquePlugins = TestPluginSet<Opaque>;

/// The standard test configuration, valid for both plugin sets: one x86-64 fleet running the
/// `noop` sandbox, opaque `test` plugin sections, plaintext authentication stubs and a plaintext
/// server, so it needs no TLS material.
pub const STANDARD_GANG_TOML: &str = r#"poll_interval = "500ms"
client_action_timeout = "1m"
default_isa = "x86-64"
listen = "127.0.0.1:9443"
worker_listen = "127.0.0.1:9445"
endpoint = "https://gang.example.com:9443"

[[fleets]]
name = "example-fleet"
isa = "x86-64"
sandbox = "noop"
min_instances = 0
max_instances = 4
vcpus_per_instance = 4
memory_per_instance = "8GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 4
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "30s"

[provisioner.test]

[cache.test]

[signer.test]

[scheduler.test]

[database.test]

[sandbox.noop]

[authentication.clients.none]

[authentication.workers.none]

[server]
transport = "plaintext"
"#;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use gang_config::{GangConfig, SandboxSelection};

    use super::{
        BoxedSection, Opaque, OpaquePlugins, STANDARD_GANG_TOML, SandboxConfig, SandboxKey,
        TestLimits, TestPlugins,
    };
    use crate::TestResult;

    #[test]
    fn sandbox_keys_keep_their_wire_names() {
        for (key, name) in [
            (SandboxKey::Noop, "noop"),
            (SandboxKey::Bwrap, "bwrap"),
            (SandboxKey::Boxed, "boxed"),
        ] {
            let json = format!("\"{name}\"");
            assert_eq!(key.to_string(), name);
            assert_eq!(serde_json::to_string(&key).expect("serialize key"), json);
            assert_eq!(
                serde_json::from_str::<SandboxKey>(&json).expect("parse key"),
                key
            );
        }
    }

    #[test]
    fn standard_config_parses_for_both_plugin_sets() -> TestResult {
        let opaque = STANDARD_GANG_TOML.parse::<GangConfig<OpaquePlugins>>()?;
        assert_eq!(opaque.fleets.len(), 1);
        let policy = opaque
            .fleets
            .policies()
            .first()
            .ok_or("the standard config has no fleet")?;
        assert_eq!(policy.name.to_string(), "example-fleet");

        let typed = STANDARD_GANG_TOML.parse::<GangConfig<TestPlugins>>()?;
        assert_eq!(typed.fleets.len(), 1);
        let policy = typed
            .fleets
            .policies()
            .first()
            .ok_or("the standard config has no fleet")?;
        assert_eq!(policy.name.to_string(), "example-fleet");
        Ok(())
    }

    #[test]
    fn opaque_limits_section_is_a_table() -> TestResult {
        let text = [STANDARD_GANG_TOML, "\n[limits.cgroup]\nmax_pids = 1\n"].concat();
        let config = text.parse::<GangConfig<OpaquePlugins>>()?;
        assert!(config.limits.is_some());
        Ok(())
    }

    #[test]
    fn test_limits_section_parses_and_rejects_unknown_keys() -> TestResult {
        let valid = [STANDARD_GANG_TOML, "\n[limits]\nmax_pids = 5\n"].concat();
        let config = valid.parse::<GangConfig<TestPlugins>>()?;
        assert_eq!(config.limits, Some(TestLimits { max_pids: 5 }));

        let unknown = [STANDARD_GANG_TOML, "\n[limits]\nmax_pids = 5\nbogus = 1\n"].concat();
        assert!(unknown.parse::<GangConfig<TestPlugins>>().is_err());
        Ok(())
    }

    #[test]
    fn is_configured_follows_the_present_sections() {
        let config = SandboxConfig {
            noop: Some(Opaque::default()),
            bwrap: None,
            boxed: None,
        };
        assert!(config.is_configured(SandboxKey::Noop));
        assert!(!config.is_configured(SandboxKey::Bwrap));
        assert!(!config.is_configured(SandboxKey::Boxed));
    }

    #[test]
    fn boxed_program_is_visited_once_and_can_be_rewritten() -> TestResult {
        let mut config = SandboxConfig {
            boxed: Some(BoxedSection {
                program: PathBuf::from("old"),
            }),
            ..SandboxConfig::default()
        };
        let mut visits = 0;
        config.try_for_each_program(SandboxKey::Boxed, |program| {
            visits += 1;
            *program = PathBuf::from("new");
            Ok::<(), String>(())
        })?;
        assert_eq!(visits, 1);
        let program = config
            .boxed
            .as_ref()
            .map(|section| section.program.as_path());
        assert_eq!(program, Some(Path::new("new")));
        Ok(())
    }

    #[test]
    fn noop_bwrap_and_absent_boxed_visit_nothing() -> TestResult {
        let mut config = SandboxConfig {
            noop: Some(Opaque::default()),
            bwrap: Some(Opaque::default()),
            boxed: None,
        };
        let mut visits = 0;
        for key in [SandboxKey::Noop, SandboxKey::Bwrap, SandboxKey::Boxed] {
            config.try_for_each_program(key, |_| {
                visits += 1;
                Ok::<(), String>(())
            })?;
        }
        assert_eq!(visits, 0);
        Ok(())
    }

    #[test]
    fn visitor_error_is_returned() {
        let mut config = SandboxConfig {
            boxed: Some(BoxedSection {
                program: PathBuf::from("a"),
            }),
            ..SandboxConfig::default()
        };
        let result = config.try_for_each_program(SandboxKey::Boxed, |_| Err::<(), &str>("stop"));
        assert_eq!(result, Err("stop"));
    }

    #[test]
    fn sandbox_key_display_and_serde_names_agree() -> TestResult {
        for (key, text) in [
            (SandboxKey::Noop, "noop"),
            (SandboxKey::Bwrap, "bwrap"),
            (SandboxKey::Boxed, "boxed"),
        ] {
            assert_eq!(key.to_string(), text);
            assert_eq!(serde_json::to_string(&key)?, format!("\"{text}\""));
            assert_eq!(
                serde_json::from_str::<SandboxKey>(&format!("\"{text}\""))?,
                key
            );
        }
        assert_eq!(
            "missing"
                .parse::<SandboxKey>()
                .expect_err("not a key")
                .to_string(),
            "unknown sandbox key `missing`, expected one of noop, bwrap, boxed"
        );
        Ok(())
    }

    #[test]
    fn boxed_section_defaults_its_program() -> TestResult {
        let section: BoxedSection = toml::from_str("")?;
        assert_eq!(section.program, PathBuf::from("fake-sandbox"));
        Ok(())
    }
}
