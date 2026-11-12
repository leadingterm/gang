//! The options a provisioner accepts in `[fleets.options]`.

use std::fmt::Debug;

use gang_types::FleetName;
use serde::de::DeserializeOwned;

/// One option a provisioner accepts: its key, what it means, and whether a fleet must set it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionSpec {
    /// The key in `[fleets.options]`; for an option that is not launch-only, also the platform
    /// property actions request it with.
    pub key: &'static str,
    /// What the option does and which values it accepts: the field's doc comment.
    pub doc: &'static str,
    /// Whether a fleet must set the option.
    pub required: bool,
    /// Whether the option only shapes how instances are launched: it is no platform property,
    /// and actions cannot request it.
    pub launch: bool,
}

/// The typed `[fleets.options]` table of one provisioner.
///
/// Derive it on a struct that also derives `Deserialize` with `deny_unknown_fields`: serde
/// validates the table when the configuration is loaded, and the derive lists the vocabulary
/// and renders the platform properties. Every field is one option; a field of type `Option<T>`
/// is optional. A field marked `#[option(launch)]` shapes only how instances are launched: its
/// type needs `Deserialize` alone, it is listed with `launch: true`, and `properties()` skips
/// it. Any other field's value type implements `Display`. The struct may name a
/// `#[fleet_options(validate = <path>)]` function, which checks the rules that span fields.
/// A `#[fleet_options(validate_fleets = <path>)]` function checks rules spanning its fleets.
pub trait FleetOptions: DeserializeOwned + Debug + Send + Sync + 'static {
    /// The accepted options, in field order.
    const OPTIONS: &'static [OptionSpec];

    /// The options as platform properties, in field order; unset optional options are omitted.
    /// Called once, when the configuration is loaded.
    fn properties(&self) -> Vec<(&'static str, String)>;

    /// Checks the rules that span fields; called once, when the configuration is loaded. The
    /// error text names what is wrong.
    ///
    /// # Errors
    ///
    /// The text naming the rule the options break.
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    /// Checks rules spanning this provisioner's fleets, in declaration order, after every fleet's
    /// own options pass [`FleetOptions::validate`]. The error names the rule and affected fleets.
    ///
    /// # Errors
    ///
    /// The text naming the rule and the fleets that break it.
    fn validate_fleets(fleets: &[(&FleetName, &Self)]) -> Result<(), String>
    where
        Self: Sized,
    {
        let _ = fleets;
        Ok(())
    }
}
