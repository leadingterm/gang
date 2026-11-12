//! Fleets: the instances a provisioner runs for one fleet, and the lifetime of their workers.

use crate::{FleetName, Isa, Resources};

crate::string_enum! {
    /// Whether the workers a provisioner starts serve many actions or exactly one.
    pub enum WorkerLifetime("worker lifetime") {
        /// The worker serves many actions and may keep its volumes between them.
        LongLived = "long_lived",
        /// The worker serves exactly one action and then exits.
        SingleUse = "single_use",
    }
}

/// A fleet as a provisioner sees it: its name, instruction set, instance size and the
/// provisioner's typed options.
#[derive(Debug, PartialEq, Eq)]
pub struct Fleet<'a, O> {
    /// The fleet's name.
    pub name: &'a FleetName,
    /// The instruction set the fleet's workers run.
    pub isa: Isa,
    /// The resources of one instance of the fleet.
    pub instance: Resources,
    /// The provisioner's typed options for the fleet.
    pub options: &'a O,
}

impl<O> Clone for Fleet<'_, O> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<O> Copy for Fleet<'_, O> {}

impl<'a, O> Fleet<'a, O> {
    /// Returns the fleet with `options` in place of its own options, keeping the name, ISA and
    /// instance size.
    #[must_use]
    pub fn with_options<P>(self, options: &'a P) -> Fleet<'a, P> {
        Fleet {
            name: self.name,
            isa: self.isa,
            instance: self.instance,
            options,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::Bytes;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Settings {
        lifetime: WorkerLifetime,
    }

    #[test]
    fn worker_lifetime_names_and_parse_errors_are_consistent() {
        for (value, text) in [
            (WorkerLifetime::LongLived, "long_lived"),
            (WorkerLifetime::SingleUse, "single_use"),
        ] {
            assert_eq!(value.to_string(), text);
            assert_eq!(text.parse::<WorkerLifetime>(), Ok(value));
        }
        assert_eq!(
            "other"
                .parse::<WorkerLifetime>()
                .expect_err("not a worker lifetime")
                .to_string(),
            "unknown worker lifetime `other`, expected one of long_lived, single_use"
        );
    }

    #[test]
    fn worker_lifetime_parses_snake_case_field() {
        let settings: Settings = toml::from_str("lifetime = \"single_use\"\n").unwrap();
        assert_eq!(
            settings,
            Settings {
                lifetime: WorkerLifetime::SingleUse,
            }
        );
    }

    #[test]
    fn worker_lifetime_round_trips_through_toml() {
        let settings = Settings {
            lifetime: WorkerLifetime::LongLived,
        };
        let text = toml::to_string(&settings).unwrap();
        assert_eq!(toml::from_str::<Settings>(&text).unwrap(), settings);
    }

    #[test]
    fn worker_lifetime_rejects_kebab_case() {
        assert!(toml::from_str::<Settings>("lifetime = \"single-use\"\n").is_err());
    }

    #[test]
    fn fleet_is_copy_even_when_options_are_not_clone() {
        #[derive(Debug, PartialEq)]
        struct Opaque(u32);

        let name = FleetName::try_new("gang-x86").unwrap();
        let options = Opaque(1);
        let fleet = Fleet {
            name: &name,
            isa: Isa::X86_64,
            instance: Resources {
                vcpus: 8,
                memory: Bytes::new(16 << 30),
                disk: Bytes::new(100 << 30),
            },
            options: &options,
        };
        let copied = fleet;
        let cloned = Clone::clone(&fleet);
        assert_eq!(copied, fleet);
        assert_eq!(cloned, fleet);
    }
}
