//! The plugin configuration types of one composition.

use std::fmt::{Debug, Display};
use std::path::PathBuf;

use gang_types::{AuthenticationSettings, FleetName, SchedulerSettings};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The `[fleets.options]` table of one fleet: the provisioner's options as TOML.
pub type FleetOptionsTable = toml::Table;

/// Why a fleet's options table does not match its provisioner plugin's options type.
pub type FleetOptionsError = toml::de::Error;

/// The plugin configuration types of one composition; implemented by `gang_spi::compose!`.
/// Every section type is serializable and cloneable.
pub trait PluginSet {
    /// The `[provisioner.<plugin>]` section: exactly one plugin.
    type Provisioner: DeserializeOwned + Serialize + Debug + Clone;
    /// The `[cache.<plugin>]` section: exactly one plugin.
    type Cache: DeserializeOwned + Serialize + Debug + Clone;
    /// The `[signer.<plugin>]` section: exactly one plugin.
    type Signer: DeserializeOwned + Serialize + Debug + Clone;
    /// The `[scheduler.<plugin>]` section: exactly one plugin; the host reads the worker endpoint
    /// through `SchedulerSettings`.
    type Scheduler: DeserializeOwned + Serialize + Debug + Clone + SchedulerSettings;
    /// The `[database.<plugin>]` section: exactly one plugin.
    type Database: DeserializeOwned + Serialize + Debug + Clone;
    /// Names one sandbox plugin by its section key; `Display` prints the key.
    type SandboxKey: DeserializeOwned + Serialize + Debug + Display + Copy;
    /// The `[sandbox.<plugin>]` sections: any selection of the composed sandboxes.
    type Sandbox: DeserializeOwned
        + Serialize
        + Debug
        + Clone
        + SandboxSelection<Key = Self::SandboxKey>;
    /// The plugin of one `[identity.providers.<name>.<plugin>]` section.
    type IdentityProvider: DeserializeOwned + Serialize + Debug + Clone;
    /// The `[authentication.clients.<plugin>]` section: exactly one plugin that authenticates clients.
    type ClientAuthentication: DeserializeOwned + Serialize + Debug + Clone + AuthenticationSettings;
    /// The `[authentication.workers.<plugin>]` section: exactly one plugin that authenticates workers.
    type WorkerAuthentication: DeserializeOwned + Serialize + Debug + Clone + AuthenticationSettings;
    /// The `[limits.<plugin>]` section: at most one plugin.
    type Limits: DeserializeOwned + Serialize + Debug + Clone;
    /// The typed `[fleets.options]` table of one fleet.
    type FleetOptions: Debug;

    /// Deserializes a fleet's options table into the options type of the selected provisioner
    /// plugin.
    ///
    /// # Errors
    ///
    /// [`FleetOptionsError`] when the table does not match that options type, or when the options type rejects the combination of options (`FleetOptions::validate` in composed plugin sets).
    fn fleet_options(
        provisioner: &Self::Provisioner,
        table: FleetOptionsTable,
    ) -> Result<Self::FleetOptions, FleetOptionsError>;

    /// Checks rules spanning every fleet after each fleet's options have parsed, using the
    /// selected provisioner's `FleetOptions::validate_fleets` in composed plugin sets.
    ///
    /// # Errors
    ///
    /// The provisioner's rejection text.
    fn validate_fleets(fleets: &[(&FleetName, &Self::FleetOptions)]) -> Result<(), String> {
        let _ = fleets;
        Ok(())
    }

    /// The option keys and values of a fleet, as platform properties its workers register.
    fn option_properties(options: &Self::FleetOptions) -> Vec<(&'static str, String)>;
}

/// Which sandboxes a configuration activates.
pub trait SandboxSelection {
    /// Names one sandbox plugin by its section key.
    type Key: Copy;

    /// Whether the `[sandbox.<plugin>]` section named by `key` is present.
    fn is_configured(&self, key: Self::Key) -> bool;

    /// Calls `visit` on every program path of the `[sandbox.<plugin>]` section `key` names
    /// (nothing when that section is absent or names no program), and stops at the first error.
    ///
    /// # Errors
    ///
    /// The first error `visit` returns.
    fn try_for_each_program<E>(
        &mut self,
        key: Self::Key,
        visit: impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E>;
}

/// Deserializes a fleet's options table into `T`, then runs `validate` on the result.
///
/// # Errors
///
/// [`FleetOptionsError`] when the table does not match `T`, or when `validate` rejects the
/// deserialized options; the rejection message becomes the error's text. `validate` is not
/// called when the table does not match `T`.
pub fn validated_fleet_options<T: DeserializeOwned>(
    table: FleetOptionsTable,
    validate: impl FnOnce(&T) -> Result<(), String>,
) -> Result<T, FleetOptionsError> {
    let options: T = table.try_into()?;
    validate(&options).map_err(<FleetOptionsError as serde::de::Error>::custom)?;
    Ok(options)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use serde::Deserialize;

    use super::{FleetOptionsTable, validated_fleet_options};

    #[derive(Debug, PartialEq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Options {
        count: u32,
    }

    fn table(text: &str) -> FleetOptionsTable {
        text.parse().expect("test table is valid TOML")
    }

    #[test]
    fn valid_table_passes_validation() {
        let options =
            validated_fleet_options::<Options>(table("count = 3"), |_| Ok(())).expect("valid");
        assert_eq!(options, Options { count: 3 });
    }

    #[test]
    fn unknown_field_is_rejected_with_the_serde_error() {
        let error = validated_fleet_options::<Options>(table("count = 3\nextra = 1"), |_| Ok(()))
            .expect_err("unknown field");
        assert!(
            error.to_string().contains("unknown field `extra`"),
            "{error}"
        );
    }

    #[test]
    fn rejection_message_is_the_error_text() {
        let error = validated_fleet_options::<Options>(table("count = 0"), |options| {
            if options.count == 0 {
                Err("count must be positive".to_owned())
            } else {
                Ok(())
            }
        })
        .expect_err("validate rejects");
        assert!(
            error.to_string().contains("count must be positive"),
            "{error}"
        );
    }

    #[test]
    fn validate_is_not_called_when_deserialization_fails() {
        let called = Cell::new(false);
        let result = validated_fleet_options::<Options>(table("count = \"many\""), |_| {
            called.set(true);
            Ok(())
        });
        assert!(result.is_err());
        assert!(!called.get());
    }
}
