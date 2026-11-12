//! Errors of `gang.toml` parsing and validation.

use std::net::SocketAddr;

use gang_types::{FleetName, Isa, MAX_FLEETS, ProviderName, RoleName, Url};

/// A configuration file that cannot be used.
///
/// A message names the setting and the value and never repeats its source; the action that
/// helps is [`gang_types::Fix::fix`].
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub enum ConfigError {
    /// The `gang.toml` text is not valid TOML or does not match the configuration schema;
    /// unknown backends and unknown fields are reported here with the accepted keys.
    #[error("invalid gang.toml")]
    Gang(#[source] toml::de::Error),
    /// A configuration could not be rendered as TOML.
    #[error("cannot render configuration")]
    Render(#[from] toml::ser::Error),
    /// `default_isa` matches no fleet's instruction set.
    #[error("default_isa `{0}` matches no fleet")]
    #[fix("set default_isa to the instruction set of a declared fleet")]
    DefaultIsaWithoutFleet(Isa),
    /// The `[fleets.options]` table does not match the options of the fleet's provisioner.
    #[error("invalid options of fleet `{fleet}`")]
    FleetOptions {
        /// The fleet whose options are invalid.
        fleet: FleetName,
        /// The serde error, which lists the accepted keys for unknown fields.
        #[source]
        source: Box<toml::de::Error>,
    },
    /// Two `[[fleets]]` entries share a name.
    #[error("fleet `{0}` is declared twice")]
    #[fix("give every [[fleets]] entry its own name")]
    DuplicateFleet(FleetName),
    /// More fleets are declared than the platform supports.
    #[error("{count} fleets declared, at most {} are supported", MAX_FLEETS)]
    #[fix("declare at most {} fleets", MAX_FLEETS)]
    TooManyFleets {
        /// The number of declared fleets.
        count: usize,
    },
    /// A fleet allows fewer instances at its maximum than at its minimum.
    #[error(
        "fleet `{fleet}` has min_instances {min_instances} above max_instances {max_instances}"
    )]
    #[fix("set max_instances at or above min_instances")]
    FleetBounds {
        /// The fleet's name.
        fleet: FleetName,
        /// The configured floor.
        min_instances: u32,
        /// The configured ceiling.
        max_instances: u32,
    },
    /// A fleet names a sandbox that has no `[sandbox.<plugin>]` section.
    #[error("fleet `{fleet}` uses sandbox `{key}` that has no [sandbox.{key}] section")]
    #[fix("add the [sandbox.<plugin>] section, or name a configured sandbox in the fleet")]
    FleetSandboxNotConfigured {
        /// The fleet's name.
        fleet: FleetName,
        /// The sandbox the fleet names.
        key: String,
    },
    /// A provisioner rejects a combination of fleets.
    #[error("{0}")]
    #[fix("rename or reconfigure the fleets the message names")]
    FleetSet(String),
    /// `[authentication]` names an identity provider that has no `[identity.providers.<name>]`
    /// section.
    #[error("authentication names provider `{0}` that has no [identity.providers.{0}] section")]
    #[fix("add the [identity.providers.<name>] section, or name a configured provider")]
    AuthenticationProviderNotConfigured(ProviderName),
    /// An identity provider uses the reserved name `none`, which anonymous callers use.
    #[error("identity provider name `{0}` is reserved")]
    #[fix("rename the provider; none is reserved")]
    ReservedProviderName(ProviderName),
    /// `transport = "plaintext"` with a plugin that needs TLS, such as `mtls`.
    #[error("an authentication plugin requires server.transport = \"tls\"")]
    #[fix("set server.transport = \"tls\"")]
    AuthenticationRequiresTls,
    /// A scheduler worker endpoint has no host.
    #[error("worker endpoint `{0}` has no host")]
    #[fix(
        "set the scheduler worker endpoint to a URL with a host, such as grpcs://gang.example.com:9090"
    )]
    WorkerEndpointWithoutHost(Url),
    /// `transport = "tls"` with a scheduler worker endpoint that is not a `grpcs` URL.
    #[error("worker endpoint `{0}` must use grpcs when server.transport is \"tls\"")]
    #[fix("set the scheduler worker endpoint to a grpcs URL")]
    WorkerEndpointNotGrpcs(Url),
    /// `transport = "plaintext"` with a worker endpoint that is not a `grpc` or `grpcs` URL.
    #[error("worker endpoint `{0}` must use grpc or grpcs when server.transport is \"plaintext\"")]
    #[fix("set the scheduler worker endpoint to a grpc or grpcs URL")]
    WorkerEndpointNotGrpc(Url),
    /// `transport = "tls"` with an `endpoint` that is not an https URL.
    #[error("endpoint `{0}` must be an https URL when server.transport is \"tls\"")]
    #[fix("set endpoint to an https URL")]
    EndpointNotHttps(Url),
    /// `transport = "plaintext"` with an `endpoint` that is not an http or https URL.
    #[error("endpoint `{0}` must be an http or https URL when server.transport is \"plaintext\"")]
    #[fix("set endpoint to an http or https URL")]
    EndpointNotHttp(Url),
    /// `endpoint` is omitted but `listen` is not a loopback address, so no public endpoint can be
    /// derived for clients to dial.
    #[error("endpoint is required when listen `{listen}` is not a loopback address")]
    #[fix("set endpoint to the address clients dial")]
    EndpointRequired {
        /// The configured public listen address.
        listen: SocketAddr,
    },
    /// The scheduler worker endpoint is omitted but `worker_listen` is not loopback.
    #[error("{key} is required when worker_listen `{listen}` is not a loopback address")]
    #[fix("set {key} to the address workers dial")]
    WorkerEndpointRequired {
        /// The worker endpoint field within the scheduler plugin section.
        key: &'static str,
        /// The configured worker listen address.
        listen: SocketAddr,
    },
    /// `server.roles` declares a role name twice.
    #[error("role `{0}` is declared twice")]
    #[fix("give every role in server.roles its own name")]
    DuplicateRole(RoleName),
    /// A role grants no permissions.
    #[error("role `{0}` grants no permissions")]
    #[fix("grant the role at least one permission")]
    RoleWithoutPermissions(RoleName),
    /// A role lists no principals.
    #[error("role `{0}` has no principals")]
    #[fix("list at least one principal for the role")]
    RoleWithoutPrincipals(RoleName),
    /// A role's principal names a provider that has no `[identity.providers.<name>]` section
    /// and is not the reserved `none`.
    #[error(
        "role `{role}` names provider `{provider}` that has no [identity.providers.{provider}] section"
    )]
    #[fix("add the [identity.providers.<name>] section, or name a configured provider")]
    RoleProviderNotConfigured {
        /// The role whose principal names the provider.
        role: RoleName,
        /// The provider that is not configured.
        provider: ProviderName,
    },
    /// `dashboard.actions_stale_after` is not above `dashboard.detail_interval`.
    #[error("dashboard.actions_stale_after must exceed dashboard.detail_interval")]
    #[fix("raise dashboard.actions_stale_after above dashboard.detail_interval")]
    StaleThresholdNotAboveRefresh,
    /// `dashboard.login.provider` names no `[identity.providers]` section, or is the reserved
    /// `none`.
    #[error(
        "dashboard.login.provider `{0}` must name an [identity.providers] section and not `none`"
    )]
    #[fix("name a configured provider other than none in dashboard.login.provider")]
    DashboardLoginProviderNotConfigured(ProviderName),
}

/// A `ca_pin` value that is neither `signer` nor a `sha256:` pin.
#[derive(Debug, thiserror::Error)]
#[error("expected `signer` or sha256:<64 lowercase hex>")]
pub struct CaPinSettingError;

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::error::Error;

    use super::*;

    use gang_types::Fix as _;

    const CAUSE: &str = "cause-marker";

    struct Unserializable;

    impl serde::Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom(CAUSE))
        }
    }

    fn syntax_error() -> toml::de::Error {
        toml::from_str::<toml::Table>("a = ").expect_err("the text is not TOML")
    }

    fn fleet() -> FleetName {
        FleetName::try_new("linux-x86-64").expect("the fleet name is valid")
    }

    fn provider() -> ProviderName {
        ProviderName::try_new("google").expect("the provider name is valid")
    }

    fn role() -> RoleName {
        RoleName::try_new("administrator").expect("the role name is valid")
    }

    fn endpoint() -> Url {
        Url::parse("ftp://gang.example.com").expect("the address is valid")
    }

    /// One value of every variant.
    fn every_variant() -> Vec<ConfigError> {
        vec![
            ConfigError::Gang(syntax_error()),
            ConfigError::Render(
                toml::to_string(&Unserializable).expect_err("the value cannot be serialized"),
            ),
            ConfigError::DefaultIsaWithoutFleet(Isa::Aarch64),
            ConfigError::FleetOptions {
                fleet: fleet(),
                source: Box::new(syntax_error()),
            },
            ConfigError::DuplicateFleet(fleet()),
            ConfigError::TooManyFleets {
                count: MAX_FLEETS + 1,
            },
            ConfigError::FleetBounds {
                fleet: fleet(),
                min_instances: 6,
                max_instances: 4,
            },
            ConfigError::FleetSandboxNotConfigured {
                fleet: fleet(),
                key: "bwrap".to_owned(),
            },
            ConfigError::FleetSet("incompatible fleets".to_owned()),
            ConfigError::AuthenticationProviderNotConfigured(provider()),
            ConfigError::ReservedProviderName(provider()),
            ConfigError::AuthenticationRequiresTls,
            ConfigError::WorkerEndpointWithoutHost(endpoint()),
            ConfigError::WorkerEndpointNotGrpcs(endpoint()),
            ConfigError::WorkerEndpointNotGrpc(endpoint()),
            ConfigError::EndpointNotHttps(endpoint()),
            ConfigError::EndpointNotHttp(endpoint()),
            ConfigError::EndpointRequired {
                listen: "0.0.0.0:443".parse().expect("the address is valid"),
            },
            ConfigError::WorkerEndpointRequired {
                key: "worker_endpoint",
                listen: "0.0.0.0:443".parse().expect("the address is valid"),
            },
            ConfigError::DuplicateRole(role()),
            ConfigError::RoleWithoutPermissions(role()),
            ConfigError::RoleWithoutPrincipals(role()),
            ConfigError::RoleProviderNotConfigured {
                role: role(),
                provider: provider(),
            },
            ConfigError::StaleThresholdNotAboveRefresh,
            ConfigError::DashboardLoginProviderNotConfigured(provider()),
        ]
    }

    fn fix_of(error: &ConfigError) -> Option<String> {
        error.fix().map(Cow::into_owned)
    }

    #[test]
    fn bounds_and_limits_name_the_value_to_change() {
        let fix = fix_of(&ConfigError::FleetBounds {
            fleet: fleet(),
            min_instances: 6,
            max_instances: 4,
        })
        .expect("the variant has a fix");
        assert!(fix.contains("max_instances"), "{fix}");
        let fix = fix_of(&ConfigError::TooManyFleets { count: 65 }).expect("the variant has a fix");
        assert!(fix.contains("declare at most 64"), "{fix}");
        let fix =
            fix_of(&ConfigError::StaleThresholdNotAboveRefresh).expect("the variant has a fix");
        assert!(fix.contains("dashboard.actions_stale_after"), "{fix}");
    }

    #[test]
    fn fleet_set_error_preserves_message_and_names_the_fix() {
        let error = ConfigError::FleetSet("fleets a and b conflict".to_owned());
        assert_eq!(error.to_string(), "fleets a and b conflict");
        assert_eq!(
            fix_of(&error).as_deref(),
            Some("rename or reconfigure the fleets the message names")
        );
    }

    #[test]
    fn missing_sections_say_which_section_to_add() {
        let fix = fix_of(&ConfigError::AuthenticationProviderNotConfigured(provider()))
            .expect("the variant has a fix");
        assert!(fix.contains("[identity.providers.<name>]"), "{fix}");
    }

    #[test]
    fn transport_settings_name_the_setting_to_set() {
        let fix = fix_of(&ConfigError::AuthenticationRequiresTls).expect("the variant has a fix");
        assert!(fix.contains("server.transport"), "{fix}");
        let fix =
            fix_of(&ConfigError::EndpointNotHttps(endpoint())).expect("the variant has a fix");
        assert!(fix.contains("endpoint"), "{fix}");
    }

    #[test]
    fn parse_failures_carry_no_fix_because_their_cause_lists_what_is_accepted() {
        assert_eq!(fix_of(&ConfigError::Gang(syntax_error())), None);
        assert_eq!(
            fix_of(&ConfigError::FleetOptions {
                fleet: fleet(),
                source: Box::new(syntax_error()),
            }),
            None
        );
    }

    #[test]
    fn every_fix_is_one_line() {
        for error in every_variant() {
            if let Some(fix) = error.fix() {
                assert!(!fix.is_empty(), "{error}");
                assert!(!fix.contains('\n'), "{error}");
            }
        }
    }

    #[test]
    fn no_message_repeats_its_source() {
        for error in every_variant() {
            if let Some(source) = error.source() {
                let cause = source.to_string();
                assert!(!cause.is_empty(), "{error}");
                assert!(!error.to_string().contains(&cause), "{error}: {cause}");
            }
        }
    }
}
