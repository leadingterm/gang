//! Errors of `gang.toml` parsing and validation.

use std::net::SocketAddr;
use std::time::Duration;

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
    /// The removed top-level scheduler table must be migrated to the current configuration.
    #[error(
        "the top-level scheduler table is replaced by top-level worker_endpoint and [actions], [server], and [worker]"
    )]
    #[fix(
        "move worker_endpoint to the top level; memory, disk and cold-start settings to [actions.memory], [actions.disk] and [actions.cold_start]; policy, retries, timeout and kill_grace to [actions] (actions.timeout and actions.kill_grace); HTTP limits to [server.http2]; worker liveness to server.worker_timeout; file limits to server.max_open_files and worker.max_open_files; transfer, connect, cleanup and keepalive settings to [worker]; remove scheduler_name"
    )]
    RemovedScheduler(#[source] toml::de::Error),
    /// A configuration could not be rendered as TOML.
    #[error("cannot render configuration")]
    Render(#[from] toml::ser::Error),
    /// A fleet's action ceilings are invalid.
    #[error("invalid action policy for fleet `{fleet}`")]
    FleetActionPolicy {
        /// Fleet with the invalid policy.
        fleet: FleetName,
        /// The invalid ceiling.
        #[source]
        source: gang_types::ActionPolicyError,
    },
    /// A selected sandbox cannot provide a granted capability.
    #[error("fleet `{fleet}` sandbox `{key}` does not support capability `{capability}`")]
    UnsupportedCapability {
        /// Fleet granting the capability.
        fleet: FleetName,
        /// Selected sandbox.
        key: String,
        /// Unsupported capability.
        capability: gang_types::Capability,
    },
    /// Network access requires a public egress policy.
    #[error("network capability requires execution.egress")]
    MissingEgress,
    /// A public egress policy is invalid.
    #[error("invalid execution.egress policy")]
    InvalidEgress(#[source] gang_types::EgressPolicyError),
    /// A configured action resource setting does not fit a fleet.
    #[error("{key} {value} bytes must be {relation} fleet `{fleet}` capacity {capacity} bytes")]
    #[fix("adjust the action setting or the named fleet's capacity")]
    ActionResourceDoesNotFit {
        /// The action configuration key.
        key: &'static str,
        /// The configured value in bytes.
        value: u64,
        /// Required relationship to capacity.
        relation: &'static str,
        /// The affected fleet.
        fleet: FleetName,
        /// The declared capacity in bytes.
        capacity: u64,
    },
    /// No fleet can fit the configured cold-start reservation.
    #[error(
        "actions.cold_start cpu_count {cpu_count}, memory {memory_kb} KiB and disk {disk_kb} KiB fit no fleet"
    )]
    #[fix("lower the cold-start reservation or configure a fleet that fits it")]
    ColdStartFitsNoFleet {
        /// Reserved CPUs.
        cpu_count: u32,
        /// Reserved memory in KiB.
        memory_kb: u64,
        /// Reserved disk in KiB.
        disk_kb: u64,
    },
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
    /// A scheduled minimum that does not raise the fleet's floor or exceeds its ceiling.
    #[error(
        "fleet `{fleet}` has a scheduled minimum of {instances}, which must be above min_instances {min_instances} and at most max_instances {max_instances}"
    )]
    #[fix(
        "set each [[fleets.scheduled_minimum]] instances above min_instances and at most max_instances"
    )]
    ScheduledMinimumBounds {
        /// The fleet's name.
        fleet: FleetName,
        /// The invalid scheduled count.
        instances: u32,
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
    /// `endpoint_trust = "system"` under `tls` with a `ca_pin` and a worker plugin that does not
    /// enroll: the worker reads the CA from `GET /v1/server`, which then advertises none.
    #[error(
        "server.endpoint_trust = \"system\" advertises no CA, which a pinned worker without enrollment needs"
    )]
    #[fix(
        "set server.endpoint_trust = \"ca\", or enroll workers through an authentication plugin that issues certificates"
    )]
    PinnedWorkersNeedAdvertisedCa,
    /// A worker endpoint has no host.
    #[error("worker_endpoint `{0}` has no host")]
    #[fix("set worker_endpoint to a URL with a host, such as grpcs://gang.example.com:9090")]
    WorkerEndpointWithoutHost(Url),
    /// `transport = "tls"` with a worker endpoint that is not a `grpcs` URL.
    #[error("worker_endpoint `{0}` must use grpcs when server.transport is \"tls\"")]
    #[fix("set worker_endpoint to a grpcs URL")]
    WorkerEndpointNotGrpcs(Url),
    /// `transport = "plaintext"` with a worker endpoint that is not a `grpc` or `grpcs` URL.
    #[error("worker_endpoint `{0}` must use grpc or grpcs when server.transport is \"plaintext\"")]
    #[fix("set worker_endpoint to a grpc or grpcs URL")]
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
    /// PROXY is enabled for an optional listener that is not configured.
    #[error("server.proxy_protocol.{listener} requires {listener}_listen")]
    #[fix("configure {listener}_listen or set server.proxy_protocol.{listener} to off")]
    ProxyProtocolWithoutListener {
        /// Optional listener name.
        listener: &'static str,
    },
    /// Two listeners cannot bind overlapping non-ephemeral addresses.
    #[error("{first} and {second} have conflicting listen addresses at `{listen}`")]
    #[fix("configure distinct listener addresses")]
    ListenerCollision {
        /// First listener TOML key.
        first: &'static str,
        /// Second listener TOML key.
        second: &'static str,
        /// Conflicting address.
        listen: SocketAddr,
    },
    /// Leadership renewal leaves insufficient time for a mutation and clock skew.
    #[error(
        "server.leadership.renew_interval {renew_interval:?} plus call_timeout {call_timeout:?} and clock_skew {clock_skew:?} must be below ttl {ttl:?}"
    )]
    #[fix(
        "set server.leadership.renew_interval plus call_timeout and clock_skew below server.leadership.ttl"
    )]
    LeadershipRenewalTooLate {
        /// Lease lifetime.
        ttl: Duration,
        /// Renewal interval.
        renew_interval: Duration,
        /// Minimum remaining validity for a mutation.
        call_timeout: Duration,
        /// Clock skew allowance.
        clock_skew: Duration,
    },
    /// Bazel drops a frozen controller after the keepalive timeout; its reconnect must find the
    /// new leader healthy.
    #[error(
        "server.leadership ttl plus clock_skew and renew_interval ({takeover:?}) plus the load balancer's healthy window ({healthy_window:?}) must be below the client keepalive timeout {keepalive_timeout:?}"
    )]
    #[fix("lower server.leadership.ttl, clock_skew or renew_interval")]
    LeadershipOutlastsKeepalive {
        /// Maximum leader takeover time.
        takeover: Duration,
        /// Time for the load balancer to mark the new leader healthy.
        healthy_window: Duration,
        /// Client keepalive timeout.
        keepalive_timeout: Duration,
    },
    /// The worker keepalive interval cannot reach the controller before worker eviction.
    #[error(
        "worker.connect_timeout {connect_timeout:?} must be shorter than twice server.worker_timeout {worker_timeout:?}, or the scheduler evicts workers between keepalives"
    )]
    #[fix("set worker.connect_timeout below twice server.worker_timeout")]
    WorkerConnectTimeoutTooLong {
        /// Worker connection timeout.
        connect_timeout: Duration,
        /// Controller worker eviction timeout.
        worker_timeout: Duration,
    },
    /// Scheduler hand-off is not strictly below a fleet's boot grace.
    #[error("{key} {shutdown_grace:?} must be below fleets `{fleet}` boot_grace {boot_grace:?}")]
    #[fix("set {key} below fleets boot_grace")]
    ShutdownGraceNotBelowBootGrace {
        /// Full TOML key.
        key: &'static str,
        /// Controller hand-off duration.
        shutdown_grace: Duration,
        /// Fleet whose boot grace is too short.
        fleet: FleetName,
        /// Fleet boot grace.
        boot_grace: Duration,
    },
    /// The worker endpoint is omitted but `worker_listen` is not loopback.
    #[error("{key} is required when worker_listen `{listen}` is not a loopback address")]
    #[fix("set {key} to the address workers dial")]
    WorkerEndpointRequired {
        /// The worker endpoint TOML key.
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
    /// The dashboard history window is not between one and 1440 whole steps.
    #[error(
        "dashboard.history_window must be 1 to 1440 whole dashboard.history_steps of whole seconds"
    )]
    #[fix(
        "set dashboard.history_step to whole seconds and dashboard.history_window to a multiple of it, at most 1440 steps"
    )]
    HistoryWindowNotWholeSteps,
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
            ConfigError::RemovedScheduler(syntax_error()),
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
            ConfigError::ScheduledMinimumBounds {
                fleet: fleet(),
                instances: 5,
                min_instances: 0,
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
            ConfigError::PinnedWorkersNeedAdvertisedCa,
            ConfigError::WorkerEndpointWithoutHost(endpoint()),
            ConfigError::WorkerEndpointNotGrpcs(endpoint()),
            ConfigError::WorkerEndpointNotGrpc(endpoint()),
            ConfigError::EndpointNotHttps(endpoint()),
            ConfigError::EndpointNotHttp(endpoint()),
            ConfigError::EndpointRequired {
                listen: "0.0.0.0:443".parse().expect("the address is valid"),
            },
            ConfigError::ListenerCollision {
                first: "listen",
                second: "worker_listen",
                listen: "127.0.0.1:443".parse().expect("the address is valid"),
            },
            ConfigError::LeadershipRenewalTooLate {
                ttl: Duration::from_secs(10),
                renew_interval: Duration::from_secs(8),
                call_timeout: Duration::from_secs(2),
                clock_skew: Duration::from_secs(2),
            },
            ConfigError::LeadershipOutlastsKeepalive {
                takeover: Duration::from_secs(25),
                healthy_window: Duration::from_secs(10),
                keepalive_timeout: Duration::from_secs(30),
            },
            ConfigError::WorkerConnectTimeoutTooLong {
                connect_timeout: Duration::from_secs(20),
                worker_timeout: Duration::from_secs(10),
            },
            ConfigError::ShutdownGraceNotBelowBootGrace {
                key: "server.http2.shutdown_grace",
                shutdown_grace: Duration::from_secs(30),
                fleet: fleet(),
                boot_grace: Duration::from_secs(30),
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
            ConfigError::HistoryWindowNotWholeSteps,
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
        let fix = fix_of(&ConfigError::HistoryWindowNotWholeSteps).expect("the variant has a fix");
        assert_eq!(
            fix,
            "set dashboard.history_step to whole seconds and dashboard.history_window to a multiple of it, at most 1440 steps"
        );
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
