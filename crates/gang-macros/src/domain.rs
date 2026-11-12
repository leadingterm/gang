//! The plugin domains `compose!` knows, and the ports each provides.

/// How many plugins of a domain a configuration may activate.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    /// Exactly one section: an externally tagged enum.
    ExactlyOne,
    /// Zero or one section: the same externally tagged enum, in an optional configuration field.
    ZeroOrOne,
    /// Any selection of sections: one optional field per plugin.
    AnyOf,
}

/// An associated type a port's trait declares, and what its dispatch enum delegates.
pub struct Associated {
    /// The associated type's name in the port's trait.
    pub name: &'static str,
    /// The `#[settings]`-style traits the dispatch enum implements by forwarding each method to
    /// the selected plugin's value; `compose!` calls each trait's `__gang_delegate_<Trait>` macro.
    pub delegated: &'static [&'static str],
}

/// A port a domain's plugins implement, with the associated types its trait declares.
pub struct PortSpec {
    /// The trait's name in `gang_spi`.
    pub name: &'static str,
    /// The associated types of the trait, in declaration order: `compose!` generates one
    /// dispatch enum per type, named after the domain and the type.
    pub associated: &'static [Associated],
}

impl PortSpec {
    const fn plain(name: &'static str) -> Self {
        Self {
            name,
            associated: &[],
        }
    }
}

/// The settings an authentication domain's configuration reports: whether it requires TLS and
/// which identity providers it names. The trait carries `#[settings]`, which records its methods.
pub const AUTHENTICATION_SETTINGS: &str = "AuthenticationSettings";

/// The worker endpoint a scheduler's configuration names or derives. The trait carries
/// `#[settings]`, which records its methods.
pub const SCHEDULER_SETTINGS: &str = "SchedulerSettings";

/// One plugin domain.
pub struct Domain {
    /// The key in the `compose!` invocation.
    pub key: &'static str,
    /// The associated type of `gang_config::PluginSet` carrying the domain's configuration.
    pub associated_type: &'static str,
    /// How many of the domain's plugins a configuration activates.
    pub cardinality: Cardinality,
    /// Whether the generated configuration implements `gang_config::SandboxSelection`.
    pub sandbox_selection: bool,
    /// The ports every plugin of the domain implements.
    pub ports: &'static [PortSpec],
    /// The settings traits (declared with `#[settings]` in `gang_types`) the domain's
    /// configuration enum implements by delegating to the selected plugin's settings: the enum
    /// reports what the host reads and sets without knowing the plugin.
    pub config_traits: &'static [&'static str],
}

/// The associated type that carries a provisioner's typed `[fleets.options]`: the domain that has
/// it also fills the `PluginSet` items that parse and render the options.
pub const FLEET_OPTIONS: &str = "FleetOptions";

/// The associated type that carries an action's limits scope. The domain that has it also fills
/// the scope's `ActionScope` launcher impl on the generated dispatch enum.
pub const LIMITS_SCOPE: &str = "Scope";

/// The trait the limits scope's dispatch enum implements by delegation: its launcher.
pub const ACTION_SCOPE: &str = "ActionScope";

/// Every domain, in the order of `gang_config::PluginSet`.
pub const DOMAINS: [Domain; 10] = [
    Domain {
        key: "provisioner",
        associated_type: "Provisioner",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[
            PortSpec {
                name: "ProvisionerPort",
                associated: &[Associated {
                    name: FLEET_OPTIONS,
                    delegated: &[],
                }],
            },
            PortSpec::plain("NodeLifecyclePort"),
            PortSpec::plain("ScratchVolumePort"),
            PortSpec::plain("WorkloadCredentialPort"),
        ],
        config_traits: &[],
    },
    Domain {
        key: "cache",
        associated_type: "Cache",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[PortSpec::plain("CacheStorePort")],
        config_traits: &[],
    },
    Domain {
        key: "signer",
        associated_type: "Signer",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[PortSpec::plain("CertificateSignerPort")],
        config_traits: &[],
    },
    Domain {
        key: "scheduler",
        associated_type: "Scheduler",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[PortSpec::plain("SchedulerPort")],
        config_traits: &[SCHEDULER_SETTINGS],
    },
    Domain {
        key: "database",
        associated_type: "Database",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[PortSpec::plain("StateRepositoryPort")],
        config_traits: &[],
    },
    Domain {
        key: "sandbox",
        associated_type: "Sandbox",
        cardinality: Cardinality::AnyOf,
        sandbox_selection: true,
        ports: &[PortSpec::plain("ActionIsolationPort")],
        config_traits: &[],
    },
    Domain {
        key: "identity_provider",
        associated_type: "IdentityProvider",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[
            PortSpec::plain("ProofVerifierPort"),
            PortSpec::plain("LoginPort"),
        ],
        config_traits: &[],
    },
    Domain {
        key: "client_authentication",
        associated_type: "ClientAuthentication",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[
            PortSpec::plain("AuthenticationPort"),
            PortSpec::plain("CredentialLifecyclePort"),
        ],
        config_traits: &[AUTHENTICATION_SETTINGS],
    },
    Domain {
        key: "worker_authentication",
        associated_type: "WorkerAuthentication",
        cardinality: Cardinality::ExactlyOne,
        sandbox_selection: false,
        ports: &[
            PortSpec::plain("AuthenticationPort"),
            PortSpec::plain("CredentialLifecyclePort"),
        ],
        config_traits: &[AUTHENTICATION_SETTINGS],
    },
    Domain {
        key: "limits",
        associated_type: "Limits",
        cardinality: Cardinality::ZeroOrOne,
        sandbox_selection: false,
        ports: &[PortSpec {
            name: "ActionLimitsPort",
            associated: &[Associated {
                name: LIMITS_SCOPE,
                delegated: &[ACTION_SCOPE],
            }],
        }],
        config_traits: &[],
    },
];

impl Domain {
    /// The associated types of the domain's ports, each once, in port order, with the port's
    /// trait that declares it.
    pub fn associated_types(&self) -> impl Iterator<Item = (&'static str, &'static Associated)> {
        self.ports.iter().flat_map(|port| {
            port.associated
                .iter()
                .map(move |associated| (port.name, associated))
        })
    }

    /// Whether a port of the domain declares the typed fleet options.
    pub fn has_fleet_options(&self) -> bool {
        self.associated_types()
            .any(|(_, associated)| associated.name == FLEET_OPTIONS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_provisioner_domain_has_fleet_options() {
        let keys: Vec<&str> = DOMAINS
            .iter()
            .filter(|domain| domain.has_fleet_options())
            .map(|domain| domain.key)
            .collect();
        assert_eq!(keys, ["provisioner"]);
    }

    #[test]
    fn associated_types_in_the_table_are_fleet_options_and_limits_scope() {
        let associated: Vec<(&str, &str)> = DOMAINS
            .iter()
            .flat_map(Domain::associated_types)
            .map(|(port, associated)| (port, associated.name))
            .collect();
        assert_eq!(
            associated,
            [
                ("ProvisionerPort", FLEET_OPTIONS),
                ("ActionLimitsPort", LIMITS_SCOPE)
            ]
        );
    }

    #[test]
    fn only_the_limits_scope_delegates_its_traits() {
        let delegated: Vec<(&str, &[&str])> = DOMAINS
            .iter()
            .flat_map(Domain::associated_types)
            .map(|(_, associated)| (associated.name, associated.delegated))
            .collect();
        assert_eq!(
            delegated,
            [
                (FLEET_OPTIONS, &[][..]),
                (LIMITS_SCOPE, &[ACTION_SCOPE][..])
            ]
        );
    }

    #[test]
    fn domain_without_associated_types_has_no_fleet_options() {
        let cache: Vec<&str> = DOMAINS
            .iter()
            .filter(|domain| domain.key == "cache")
            .filter(|domain| domain.associated_types().next().is_none())
            .filter(|domain| !domain.has_fleet_options())
            .map(|domain| domain.key)
            .collect();
        assert_eq!(cache, ["cache"]);
    }

    #[test]
    fn authentication_settings_are_on_the_authentication_domains_only() {
        let keys: Vec<&str> = DOMAINS
            .iter()
            .filter(|domain| domain.config_traits.contains(&AUTHENTICATION_SETTINGS))
            .map(|domain| domain.key)
            .collect();
        assert_eq!(keys, ["client_authentication", "worker_authentication"]);
    }

    #[test]
    fn scheduler_settings_are_on_the_scheduler_domain_only() {
        let keys: Vec<&str> = DOMAINS
            .iter()
            .filter(|domain| domain.config_traits.contains(&SCHEDULER_SETTINGS))
            .map(|domain| domain.key)
            .collect();
        assert_eq!(keys, ["scheduler"]);
    }
}
