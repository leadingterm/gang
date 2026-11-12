//! Behaviour of `#[derive(FleetOptions)]` on a `[fleets.options]` struct.

use std::fmt;

use gang_spi::{FleetOptions, OptionSpec};
use gang_types::FleetName;
use serde::Deserialize;

/// A machine tier, rendered through `Display` as a platform property value.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Tier {
    Small,
    Large,
}

impl fmt::Display for Tier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Small => "small",
            Self::Large => "large",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
struct Sample {
    /// The machine image,
    /// as a path.
    image: String,
    /// The machine tier.
    tier: Tier,
    /// The zone to start in.
    zone: Option<String>,
    /// The disk size in gigabytes.
    disk_gb: Option<u32>,
}

#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// A nested table that is launch-only: it is no platform property and it is not `Display`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Nested {
    /// Whether the instance starts warm.
    warm: bool,
}

/// Options with one launch-only field.
#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
struct Launched {
    /// The machine image.
    image: String,
    /// How the instance is launched.
    #[option(launch)]
    nested: Option<Nested>,
}

/// Options whose cross-field rule is checked by `Checked::check`.
#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
#[fleet_options(validate = Checked::check)]
struct Checked {
    /// The zone to start in.
    zone: Option<String>,
    /// The disk size in gigabytes.
    disk_gb: Option<u32>,
}

impl Checked {
    /// Rejects a disk size without a zone.
    fn check(&self) -> Result<(), String> {
        if self.disk_gb.is_some() && self.zone.is_none() {
            return Err("disk_gb requires zone".to_owned());
        }
        Ok(())
    }
}

/// Options whose fleet-wide rule requires distinct zones.
#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
#[fleet_options(validate_fleets = UniqueZone::check_fleets)]
struct UniqueZone {
    /// The fleet's zone.
    zone: String,
}

impl UniqueZone {
    /// Rejects fleets that use the same zone.
    fn check_fleets(fleets: &[(&FleetName, &Self)]) -> Result<(), String> {
        for (index, (name, options)) in fleets.iter().enumerate() {
            for (other_name, other) in &fleets[index + 1..] {
                if options.zone == other.zone {
                    return Err(format!("fleets {name} and {other_name} share a zone"));
                }
            }
        }
        Ok(())
    }
}

#[test]
fn options_list_keys_in_field_order_with_docs_and_requiredness() {
    assert_eq!(
        Sample::OPTIONS,
        &[
            OptionSpec {
                key: "image",
                doc: "The machine image, as a path.",
                required: true,
                launch: false,
            },
            OptionSpec {
                key: "tier",
                doc: "The machine tier.",
                required: true,
                launch: false,
            },
            OptionSpec {
                key: "zone",
                doc: "The zone to start in.",
                required: false,
                launch: false,
            },
            OptionSpec {
                key: "disk_gb",
                doc: "The disk size in gigabytes.",
                required: false,
                launch: false,
            },
        ]
    );
}

#[test]
fn properties_render_required_always_and_optional_only_when_set() {
    let unset = Sample {
        image: "ami-1".to_owned(),
        tier: Tier::Small,
        zone: None,
        disk_gb: None,
    };
    assert_eq!(
        unset.properties(),
        vec![("image", "ami-1".to_owned()), ("tier", "small".to_owned())]
    );

    let set = Sample {
        image: "ami-2".to_owned(),
        tier: Tier::Large,
        zone: Some("eu-1".to_owned()),
        disk_gb: Some(20),
    };
    assert_eq!(
        set.properties(),
        vec![
            ("image", "ami-2".to_owned()),
            ("tier", "large".to_owned()),
            ("zone", "eu-1".to_owned()),
            ("disk_gb", "20".to_owned()),
        ]
    );
}

#[test]
fn empty_struct_has_no_options_and_no_properties() {
    assert!(Empty::OPTIONS.is_empty());
    assert!(Empty {}.properties().is_empty());
}

#[test]
fn unknown_key_is_rejected_naming_accepted_keys() {
    let error = toml::from_str::<Sample>(
        r#"
image = "ami-1"
tier = "small"
bogus = 1
"#,
    )
    .expect_err("unknown key must be rejected");
    let message = error.to_string();
    assert!(message.contains("bogus"), "{message}");
    for key in ["image", "tier", "zone", "disk_gb"] {
        assert!(message.contains(key), "{message}");
    }
}

#[test]
fn missing_required_key_is_an_error() {
    let error = toml::from_str::<Sample>(
        r#"
tier = "small"
"#,
    )
    .expect_err("missing required key must be rejected");
    assert!(error.to_string().contains("image"), "{error}");
}

#[test]
fn launch_option_is_listed_as_launch_and_not_a_property() {
    assert_eq!(
        Launched::OPTIONS,
        &[
            OptionSpec {
                key: "image",
                doc: "The machine image.",
                required: true,
                launch: false,
            },
            OptionSpec {
                key: "nested",
                doc: "How the instance is launched.",
                required: false,
                launch: true,
            },
        ]
    );
}

#[test]
fn launch_option_is_omitted_from_properties_whether_set_or_unset() {
    let unset = Launched {
        image: "ami-1".to_owned(),
        nested: None,
    };
    assert_eq!(unset.properties(), vec![("image", "ami-1".to_owned())]);

    let set = Launched {
        image: "ami-2".to_owned(),
        nested: Some(Nested { warm: true }),
    };
    assert_eq!(set.properties(), vec![("image", "ami-2".to_owned())]);
}

#[test]
fn launch_option_still_deserializes_as_an_optional_table() {
    let parsed = toml::from_str::<Launched>(
        r#"
image = "ami-1"

[nested]
warm = true
"#,
    )
    .expect("launch option must deserialize");
    assert!(matches!(parsed.nested, Some(Nested { warm: true })));

    let bare = toml::from_str::<Launched>("image = \"ami-1\"").expect("launch option is optional");
    assert!(bare.nested.is_none());

    let error = toml::from_str::<Launched>("nested.warm = true")
        .expect_err("required option must still be required");
    assert!(error.to_string().contains("image"), "{error}");
}

#[test]
fn validate_returns_the_check_error_for_the_rejected_combination() {
    let rejected = Checked {
        zone: None,
        disk_gb: Some(20),
    };
    assert_eq!(rejected.validate(), Err("disk_gb requires zone".to_owned()));
}

#[test]
fn validate_accepts_every_other_combination() {
    let zoned = Checked {
        zone: Some("eu-1".to_owned()),
        disk_gb: Some(20),
    };
    assert_eq!(zoned.validate(), Ok(()));

    let zone_only = Checked {
        zone: Some("eu-1".to_owned()),
        disk_gb: None,
    };
    assert_eq!(zone_only.validate(), Ok(()));

    let neither = Checked {
        zone: None,
        disk_gb: None,
    };
    assert_eq!(neither.validate(), Ok(()));
}

#[test]
fn struct_without_validate_attribute_validates_ok() {
    let sample = Sample {
        image: "ami-1".to_owned(),
        tier: Tier::Small,
        zone: None,
        disk_gb: Some(20),
    };
    assert_eq!(sample.validate(), Ok(()));
    assert_eq!(Empty {}.validate(), Ok(()));
}

#[test]
fn validate_fleets_calls_the_derived_hook() -> Result<(), Box<dyn std::error::Error>> {
    let first = FleetName::try_new("first")?;
    let second = FleetName::try_new("second")?;
    let east = UniqueZone {
        zone: "east".to_owned(),
    };
    let also_east = UniqueZone {
        zone: "east".to_owned(),
    };
    let west = UniqueZone {
        zone: "west".to_owned(),
    };
    assert_eq!(
        UniqueZone::validate_fleets(&[(&first, &east), (&second, &also_east)]),
        Err("fleets first and second share a zone".to_owned())
    );
    assert_eq!(
        UniqueZone::validate_fleets(&[(&first, &east), (&second, &west)]),
        Ok(())
    );
    assert_eq!(Empty::validate_fleets(&[]), Ok(()));
    Ok(())
}
