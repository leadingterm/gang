//! Tests of the `gang.toml` template: placement, variants, identity, and the typed accessors,
//! run against the local-mode configuration of the specification.

use std::fmt;
use std::path::PathBuf;

use gang_config::{
    Document, FleetOptionsError, FleetOptionsTable, GangConfig, InputLinks, PluginSet, RawFleet,
    SandboxSelection,
};
use gang_e2e::{
    AUDIENCE, ActionLimits, ActionTools, ConfigTemplate, ControlKnobs, E2eError, EgressTools,
    ExecutionKnobs, FleetLayout, IdentityKnobs, LARGE_FLEET, LIMIT_CPU_PERIOD_MICROS,
    LIMIT_MAX_PIDS, NOOP_FLEET, ORGANIZATION, ORGANIZATION_CLAIM, PermitNetwork, Placement,
    SMALL_FLEET, Sandbox, ServerKnobs, SoftMemory, Transport, Variant,
};
use gang_main::plugins::{
    CacheConfig, ClientAuthenticationConfig, DatabaseConfig, IdentityProviderConfig, LimitsConfig,
    Plugins, ProvisionerConfig, SandboxKey, SignerConfig, WorkerAuthenticationConfig,
};
use gang_types::{
    AbsolutePathBuf, AuthenticationSettings, Bytes, CaPin, Capability, NonZeroDuration,
    PermissionSet, ProviderName, PublicEgressPolicy, Url,
};
use toml::Table;

/// The checked-in local-mode configuration supplied in the test's runfiles.
fn base_config() -> gang_e2e::Result<String> {
    let path = std::env::var("GANG_E2E_BASE_CONFIG")
        .map_err(|error| E2eError::system(format!("reading GANG_E2E_BASE_CONFIG: {error}")))?;
    std::fs::read_to_string(&path)
        .map_err(E2eError::io(format!("reading the base config at {path}")))
}

/// The loopback listen address of `port`.
fn listen(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// The rendered template parsed as the real, unresolved configuration.
fn typed_rendered(template: &ConfigTemplate) -> gang_e2e::Result<Document<Plugins>> {
    Document::<Plugins>::parse(&template.render()?)
        .map_err(|error| E2eError::Template(error.to_string()))
}

/// The fixture with one sandbox section removed.
fn without_sandbox(key: SandboxKey) -> gang_e2e::Result<String> {
    let mut document = Document::<Plugins>::parse(&base_config()?)
        .map_err(|error| E2eError::Template(gang_types::error_chain(&error).to_string()))?;
    match key {
        SandboxKey::Bwrap => document.sandbox.bwrap = None,
        SandboxKey::Noop => document.sandbox.noop = None,
    }
    document
        .to_toml()
        .map_err(|error| E2eError::Template(error.to_string()))
}

/// The fixture, placed at a fresh placement over `transport`.
fn placed(transport: Transport) -> gang_e2e::Result<(Placement, ConfigTemplate)> {
    let placement = Placement::create(None)?;
    let template = ConfigTemplate::parse(&base_config()?)?.place(&placement, transport)?;
    Ok((placement, template))
}

/// The worker launcher argv the tests pass to `vary`.
fn launcher() -> Vec<String> {
    ["systemd-run", "--user", "--scope", "--"]
        .map(String::from)
        .to_vec()
}

const ROOTFS_IDENTITY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

struct TestTools {
    tools: ActionTools,
    rootfs: tempfile::TempDir,
}

impl std::ops::Deref for TestTools {
    type Target = ActionTools;

    fn deref(&self) -> &Self::Target {
        &self.tools
    }
}

impl std::ops::DerefMut for TestTools {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tools
    }
}

/// A rootfs envelope for configuration rendering; its files are not executed.
fn tools() -> gang_e2e::Result<TestTools> {
    let rootfs = tempfile::tempdir().map_err(E2eError::io("creating rootfs envelope"))?;
    let root = rootfs.path().join("root");
    std::fs::create_dir_all(root.join("etc")).map_err(E2eError::io("creating rootfs tree"))?;
    std::fs::create_dir_all(root.join("usr/bin")).map_err(E2eError::io("creating rootfs bin"))?;
    std::os::unix::fs::symlink("usr/bin", root.join("bin"))
        .map_err(E2eError::io("linking merged bin"))?;
    std::fs::write(root.join("usr/bin/bash"), "").map_err(E2eError::io("writing fixture shell"))?;
    std::fs::write(
        root.join("etc/passwd"),
        "builder:x:1000:1000:builder:/home/builder:/bin/bash\n",
    )
    .map_err(E2eError::io("writing rootfs passwd"))?;
    std::fs::write(root.join("etc/group"), "builder:x:1000:\n")
        .map_err(E2eError::io("writing rootfs group"))?;
    std::fs::write(
        rootfs.path().join("identity"),
        format!("{ROOTFS_IDENTITY}\n"),
    )
    .map_err(E2eError::io("writing rootfs identity"))?;
    Ok(TestTools {
        tools: ActionTools {
            shell: PathBuf::from("/nix/store/aaa-bash/bin/bash"),
            bwrap: Some(PathBuf::from("/nix/store/ccc-bubblewrap/bin/bwrap")),
            path: "/nix/store/aaa-bash/bin:/nix/store/bbb-coreutils/bin".to_owned(),
            glibc_tool: None,
            action_rootfs: Some(rootfs.path().to_path_buf()),
            egress: Some(EgressTools {
                slirp4netns: "/nix/store/egress/bin/slirp4netns".into(),
                nft: "/nix/store/egress/bin/nft".into(),
                nsenter: "/nix/store/egress/bin/nsenter".into(),
                ip: "/nix/store/egress/bin/ip".into(),
                unshare: "/nix/store/egress/bin/unshare".into(),
                probe: "/nix/store/egress/bin/egress_probe".into(),
                peer: "/nix/store/egress/bin/egress_peer".into(),
                policy: Some(PublicEgressPolicy {
                    protected_networks: vec![
                        "10.0.0.0/8"
                            .parse()
                            .map_err(|error| E2eError::system(format!("{error}")))?,
                    ]
                    .into_boxed_slice(),
                    dns_servers: vec![
                        "1.1.1.1"
                            .parse()
                            .map_err(|error| E2eError::system(format!("{error}")))?,
                    ]
                    .into_boxed_slice(),
                    setup_timeout: NonZeroDuration::from_secs(10),
                }),
                targets: None,
                repository_cache: None,
            }),
        },
        rootfs,
    })
}

#[test]
fn placed_base_omits_announced_endpoints_and_parses_as_toml() -> gang_e2e::Result<()> {
    for transport in [Transport::Plain, Transport::Tls] {
        let (_, template) = placed(transport)?;
        let doc = typed_rendered(&template)?;
        assert!(doc.endpoint.is_none());
        assert!(doc.worker_endpoint.is_none());
    }
    Ok(())
}

#[test]
fn placement_preserves_dynamodb_configuration() -> gang_e2e::Result<()> {
    let mut table: Table = toml::from_str(&base_config()?).expect("base is TOML");
    let dynamodb: Table =
        toml::from_str("[database.dynamodb]\ntable = \"gang-test\"\nregion = \"us-east-1\"\n")
            .expect("database fixture is TOML");
    table.insert(
        "database".to_owned(),
        dynamodb.get("database").expect("database fixture").clone(),
    );
    let text = toml::to_string(&table).expect("base renders");
    let original =
        Document::<Plugins>::parse(&text).map_err(|error| E2eError::Template(error.to_string()))?;
    let placement = Placement::create(None)?;
    let placed = ConfigTemplate::parse(&text)?.place(&placement, Transport::Tls)?;
    let rendered = typed_rendered(&placed)?;
    let (DatabaseConfig::Dynamodb(rendered), DatabaseConfig::Dynamodb(expected)) =
        (&rendered.database, &original.database)
    else {
        panic!("placement must keep the dynamodb database section");
    };
    assert_eq!(rendered, expected);
    Ok(())
}

#[test]
fn place_gives_every_local_fleet_the_harness_fleet_disk() -> gang_e2e::Result<()> {
    let placement = Placement::create(None)?;
    let template = ConfigTemplate::parse(&base_config()?)?
        .place(&placement, Transport::Tls)?
        .vary(
            &placement_variant(Sandbox::Bwrap),
            &launcher(),
            &*(tools()?),
        )?;
    let doc = typed_rendered(&template)?;
    assert_eq!(doc.fleets.len(), 3);
    for fleet in &doc.fleets {
        assert_eq!(fleet.disk_per_instance, gang_types::NonZeroBytes::gib(8));
    }
    Ok(())
}

#[test]
fn place_points_listeners_paths_and_endpoints_at_the_placement() -> gang_e2e::Result<()> {
    let (placement, template) = placed(Transport::Tls)?;
    let doc = typed_rendered(&template)?;
    assert_eq!(doc.listen.to_string(), listen(placement.public_port()));
    assert_eq!(
        doc.worker_listen.to_string(),
        listen(placement.worker_port())
    );
    let ProvisionerConfig::Local(local) = &doc.provisioner else {
        panic!("local provisioner expected");
    };
    let imds = listen(placement.imds_port());
    assert_eq!(local.mock_imds_listen.get().to_string(), imds);
    assert_eq!(local.state_dir.as_path(), placement.state_dir());
    let CacheConfig::Filesystem(cache) = &doc.cache else {
        panic!("filesystem cache expected");
    };
    assert_eq!(cache.path.as_path(), placement.cache_dir());
    let SignerConfig::File(signer) = &doc.signer else {
        panic!("file signer expected");
    };
    assert_eq!(signer.key_file.as_path(), placement.ca_key());
    let DatabaseConfig::Sqlite(database) = &doc.database else {
        return Err(E2eError::Template("sqlite database expected".to_owned()));
    };
    assert_eq!(database.path, placement.database());
    let noop = doc.sandbox.noop.as_ref().expect("noop sandbox expected");
    assert_eq!(
        noop.scratch_dir
            .as_ref()
            .expect("scratch directory")
            .as_path(),
        placement.noop_scratch()
    );
    assert!(
        doc.endpoint.is_none(),
        "the server announces its public endpoint"
    );
    assert!(doc.worker_endpoint.is_none());
    assert!(!template.render()?.contains("grpc://"));
    assert_eq!(doc.fleets[0].name.as_str(), "local-worker-x86-64");
    assert_eq!(doc.fleets[0].max_instances.get(), 4);
    let (_, provider) = doc
        .identity
        .providers
        .iter()
        .find(|(name, _)| name.as_str() == "local-workers")
        .expect("local-workers provider expected");
    let IdentityProviderConfig::Oidc(oidc) = provider;
    assert_eq!(
        oidc.issuer_url.as_url().as_str().trim_end_matches('/'),
        format!("http://{imds}")
    );
    Ok(())
}

#[test]
fn place_plain_drops_identity_and_announces_its_endpoints() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Plain)?;
    let doc = typed_rendered(&template)?;
    assert!(matches!(
        doc.server.transport,
        gang_config::Transport::Plaintext
    ));
    assert!(doc.server.ca_pin.is_none());
    assert!(doc.identity.providers.is_empty());
    assert!(matches!(
        doc.authentication.clients,
        ClientAuthenticationConfig::None(_)
    ));
    assert!(matches!(
        doc.authentication.workers,
        WorkerAuthenticationConfig::None(_)
    ));
    assert!(
        doc.endpoint.is_none(),
        "the server announces its public endpoint"
    );
    assert!(doc.worker_endpoint.is_none());
    Ok(())
}

#[test]
fn place_plain_binds_the_anonymous_caller_to_the_administrator_role() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Plain)?;
    let doc = typed_rendered(&template)?;
    let roles = &doc.server.roles;
    assert_eq!(roles.len(), 1);
    assert_eq!(roles[0].name.as_str(), "administrator");
    assert_eq!(roles[0].permissions, PermissionSet::all());
    assert_eq!(roles[0].principals.len(), 1);
    assert_eq!(roles[0].principals[0].provider.as_str(), "none");
    assert_eq!(roles[0].principals[0].principal.as_str(), "anonymous");
    Ok(())
}

#[test]
fn place_plain_grants_the_anonymous_caller_every_permission() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Plain)?;
    let text = template.render()?;
    let config = match text.parse::<GangConfig<OpaquePlugins>>() {
        Ok(config) => config,
        Err(error) => panic!("the plaintext config does not parse as a gang.toml: {error}"),
    };
    let roles = &config.server.roles;
    assert_eq!(roles.len(), 1);
    assert_eq!(roles[0].name.as_str(), "administrator");
    assert_eq!(roles[0].permissions, PermissionSet::all());
    assert_eq!(roles[0].principals.len(), 1);
    assert_eq!(roles[0].principals[0].provider.as_str(), "none");
    assert_eq!(roles[0].principals[0].principal.as_str(), "anonymous");
    Ok(())
}

#[test]
fn place_plain_needs_no_identity_provider() -> gang_e2e::Result<()> {
    let placement = Placement::create(None)?;
    let mut document = Document::<Plugins>::parse(&base_config()?)
        .map_err(|error| E2eError::Template(gang_types::error_chain(&error).to_string()))?;
    document.identity.providers.clear();
    let text = document
        .to_toml()
        .map_err(|error| E2eError::Template(error.to_string()))?;
    ConfigTemplate::parse(&text)?.place(&placement, Transport::Plain)?;
    Ok(())
}

#[test]
fn place_tls_keeps_identity_and_announces_its_worker_endpoint() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Tls)?;
    let doc = typed_rendered(&template)?;
    assert!(!doc.identity.providers.is_empty());
    assert!(doc.worker_endpoint.is_none());
    Ok(())
}

#[test]
fn place_action_settings_parse_as_domain_config() -> gang_e2e::Result<()> {
    for transport in [Transport::Plain, Transport::Tls] {
        let (_placement, template) = placed(transport)?;
        let doc = typed_rendered(&template)?;
        assert!(doc.actions.cold_start.memory.get().get() > 0);
    }
    Ok(())
}

#[test]
fn cold_actions_fit_one_placed_worker() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Tls)?;
    let template = template.vary(
        &placement_variant(Sandbox::Bwrap),
        &launcher(),
        &*(tools()?),
    )?;
    let doc = typed_rendered(&template)?;
    let cold_memory = doc.actions.cold_start.memory.get().get();
    let cold_disk = doc.actions.cold_start.disk.get().get();
    let fleet = &doc.fleets[0];
    assert!(
        cold_memory.saturating_mul(4) <= fleet.memory_per_instance.as_u64(),
        "four cold actions of {cold_memory} bytes of memory do not fit one worker of {}",
        fleet.memory_per_instance
    );
    assert!(
        cold_disk.saturating_mul(3) <= fleet.disk_per_instance.as_u64(),
        "three cold actions of {cold_disk} bytes of disk do not fit one worker of {}",
        fleet.disk_per_instance
    );
    Ok(())
}

/// The document of the fixture varied by `variant`.
fn varied_document(variant: &Variant) -> gang_e2e::Result<Document<Plugins>> {
    typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        variant,
        &launcher(),
        &*(tools()?),
    )?)
}

/// The action policy of the fixture varied by `variant`.
fn varied_actions(variant: &Variant) -> gang_e2e::Result<gang_config::ActionsConfig> {
    Ok(varied_document(variant)?.actions)
}

#[test]
fn burst_queue_fits_one_http2_connection() -> gang_e2e::Result<()> {
    for (control, expected) in [
        (ControlKnobs::NONE, 100),
        (ControlKnobs::QUEUED_BEHIND_SHORT_WORK, 100),
        (ControlKnobs::BURST_FROM_ZERO, 160),
        (ControlKnobs::COLD_BURST_FROM_ZERO, 160),
    ] {
        let variant = Variant {
            control,
            ..Variant::baseline(Sandbox::Noop)
        };
        let doc = varied_document(&variant)?;
        assert_eq!(doc.server.http2.max_concurrent_streams.get(), expected);
    }
    Ok(())
}

#[test]
fn leader_failover_preset_renders_only_the_service_prior() -> gang_e2e::Result<()> {
    let baseline = varied_document(&Variant::baseline(Sandbox::Noop))?;
    let leader = varied_document(&Variant {
        control: ControlKnobs::LEADER_FAILOVER,
        ..Variant::baseline(Sandbox::Noop)
    })?;
    assert_eq!(
        ControlKnobs::LEADER_FAILOVER,
        ControlKnobs {
            unknown_action_duration: Some(std::time::Duration::from_mins(1)),
            ..ControlKnobs::NONE
        }
    );
    assert_eq!(
        baseline.scaling.unknown_action_duration.get(),
        std::time::Duration::from_secs(10)
    );
    assert_eq!(
        leader.scaling.unknown_action_duration.get(),
        std::time::Duration::from_mins(1)
    );
    let mut leader_scaling = leader.scaling;
    leader_scaling.unknown_action_duration = baseline.scaling.unknown_action_duration;
    assert_eq!(leader_scaling, baseline.scaling);
    assert_eq!(leader.poll_interval, baseline.poll_interval);
    assert_eq!(leader.client_action_timeout, baseline.client_action_timeout);
    assert_eq!(leader.fleets.len(), baseline.fleets.len());
    for (fleet, before) in leader.fleets.iter().zip(&baseline.fleets) {
        assert_eq!(fleet.min_instances, before.min_instances);
        assert_eq!(fleet.max_instances, before.max_instances);
        assert_eq!(fleet.vcpus_per_instance, before.vcpus_per_instance);
        assert_eq!(fleet.idle_timeout, before.idle_timeout);
        assert_eq!(fleet.boot_grace, before.boot_grace);
        assert_eq!(fleet.max_drain_wait, before.max_drain_wait);
    }
    Ok(())
}

#[test]
fn rate_scaling_presets_render_typed_knobs() -> gang_e2e::Result<()> {
    let backlog = varied_document(&Variant {
        control: ControlKnobs::BACKLOG_GROWS_FLEET,
        ..Variant::baseline(Sandbox::Noop)
    })?;
    assert_eq!(
        backlog.scaling.unknown_action_duration.get(),
        std::time::Duration::from_mins(10),
    );
    for (control, idle) in [
        (
            ControlKnobs::BURST_FROM_ZERO,
            std::time::Duration::from_secs(5),
        ),
        (
            ControlKnobs::COLD_BURST_FROM_ZERO,
            std::time::Duration::from_mins(10),
        ),
    ] {
        let burst = varied_document(&Variant {
            control,
            ..Variant::baseline(Sandbox::Noop)
        })?;
        assert_eq!(burst.scaling, gang_config::ScalingConfig::default());
        assert_eq!(burst.fleets[0].idle_timeout, idle);
    }
    for control in [
        ControlKnobs::SUSTAINED_STREAM,
        ControlKnobs::TWO_SOURCES,
        ControlKnobs::UTILIZATION_SCALE_IN,
    ] {
        let document = varied_document(&Variant {
            control,
            ..Variant::baseline(Sandbox::Noop)
        })?;
        assert_eq!(document.scaling.payback_percent.get(), 50);
        assert_eq!(
            document.scaling.load_half_life.get(),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            document.scaling.source_idle.get(),
            std::time::Duration::from_secs(4)
        );
        assert_eq!(
            document.scaling.readiness_prior.get(),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            document.scaling.scale_in_dwell.get(),
            std::time::Duration::from_secs(4)
        );
        assert_eq!(document.scaling.max_cordons, 1);
        assert_eq!(
            document.scaling.unknown_action_duration.get(),
            control
                .unknown_action_duration
                .expect("preset service prior")
        );
        assert_eq!(
            document.fleets[0].idle_timeout,
            control.idle_timeout.expect("preset idle timeout")
        );
        assert_eq!(
            document.fleets[0].vcpus_per_instance.get(),
            control.vcpus_per_instance.expect("preset vcpus")
        );
    }
    Ok(())
}

#[test]
fn database_outage_preset_justifies_follower_work_before_partition_heals() -> gang_e2e::Result<()> {
    let document = varied_document(&Variant {
        control: ControlKnobs::DATABASE_OUTAGE,
        ..Variant::baseline(Sandbox::Noop)
    })?;
    assert_eq!(document.scaling.payback_percent.get(), 50);
    assert_eq!(
        document.scaling.unknown_action_duration.get(),
        std::time::Duration::from_mins(10),
    );
    assert_eq!(
        document.dashboard.detail_interval.get(),
        std::time::Duration::from_secs(1),
    );
    Ok(())
}

#[test]
fn short_boot_grace_bounds_shutdown_and_preserves_shorter_setting() -> gang_e2e::Result<()> {
    let variant = Variant {
        control: ControlKnobs::ORPHAN,
        ..Variant::baseline(Sandbox::Noop)
    };
    for (suffix, expected) in [
        ("", std::time::Duration::from_secs(5)),
        (
            "\n[server.http2]\nshutdown_grace = \"3s\"\n",
            std::time::Duration::from_secs(3),
        ),
    ] {
        let base = format!("{}{suffix}", base_config()?);
        let text = ConfigTemplate::parse(&base)?
            .vary(&variant, &launcher(), &*(tools()?))?
            .render()?;
        let config = text
            .parse::<GangConfig<Plugins>>()
            .map_err(|error| E2eError::Template(error.to_string()))?;
        assert_eq!(config.server.http2.shutdown_grace, expected);
        assert!(
            config
                .fleets
                .policies()
                .iter()
                .all(|fleet| config.server.http2.shutdown_grace < fleet.boot_grace)
        );
    }
    Ok(())
}

#[test]
fn vary_soft_memory_off_disables_soft_memory_enforcement() -> gang_e2e::Result<()> {
    let variant = Variant {
        soft_memory: SoftMemory::Off,
        ..Variant::baseline(Sandbox::Noop)
    };
    let actions = varied_actions(&variant)?;
    assert_ne!(
        actions.memory.enforcement,
        gang_config::MemoryEnforcement::Soft
    );
    Ok(())
}

#[test]
fn vary_soft_memory_enforced_opts_into_soft_memory() -> gang_e2e::Result<()> {
    let variant = Variant {
        soft_memory: SoftMemory::Enforced,
        ..Variant::baseline(Sandbox::Noop)
    };
    let actions = varied_actions(&variant)?;
    assert_eq!(
        actions.memory.enforcement,
        gang_config::MemoryEnforcement::Soft
    );
    Ok(())
}

#[test]
fn disk_soft_enforcement_is_opted_into_for_the_disk_scenario() -> gang_e2e::Result<()> {
    let baseline = Variant::baseline(Sandbox::Noop);
    let actions = varied_actions(&baseline)?;
    assert_eq!(
        actions.disk.enforcement,
        gang_config::DiskEnforcement::Pressure
    );

    let variant = Variant {
        soft_disk: true,
        ..baseline
    };
    let actions = varied_actions(&variant)?;
    assert_eq!(actions.disk.enforcement, gang_config::DiskEnforcement::Soft);
    assert_eq!(actions.disk.min_reservation.as_units(), 1024);
    Ok(())
}

#[test]
fn place_fails_naming_a_missing_section() -> gang_e2e::Result<()> {
    let text = base_config()?.replace("[cache.filesystem]", "[cache.other]");
    let error = ConfigTemplate::parse(&text).expect_err("an unknown cache plugin fails");
    assert!(
        error.to_string().contains("other"),
        "the error names the plugin: {}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn bwrap_uses_the_envelope_and_its_identity() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let tools = tools()?;
    let doc =
        typed_rendered(&template.vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)?)?;
    let bwrap = doc.sandbox.bwrap.expect("bwrap sandbox");
    assert_eq!(
        bwrap.program,
        PathBuf::from("/nix/store/ccc-bubblewrap/bin/bwrap")
    );
    assert_eq!(
        bwrap.root.as_path(),
        std::fs::canonicalize(tools.rootfs.path())
            .map_err(E2eError::io("canonicalizing rootfs envelope"))?
    );
    assert_eq!(doc.fleets[0].action_environment.as_str(), ROOTFS_IDENTITY);
    Ok(())
}

#[test]
fn bwrap_requires_a_rootfs_identity() -> gang_e2e::Result<()> {
    let mut tools = tools()?;
    tools.action_rootfs = None;
    let error = ConfigTemplate::parse(&base_config()?)?
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)
        .expect_err("bwrap requires an envelope");
    assert!(error.to_string().contains("GANG_E2E_ACTION_ROOTFS"));

    tools.action_rootfs = Some(tools.rootfs.path().to_path_buf());
    std::fs::write(tools.rootfs.path().join("identity"), "invalid\n")
        .map_err(E2eError::io("writing invalid identity"))?;
    ConfigTemplate::parse(&base_config()?)?
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)
        .expect_err("bwrap rejects an invalid identity");
    Ok(())
}

#[test]
fn bwrap_reads_identity_from_the_selected_envelope() -> gang_e2e::Result<()> {
    let tools = tools()?;
    let identity = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    std::fs::write(
        tools.rootfs.path().join("identity"),
        format!("{identity}\n"),
    )
    .map_err(E2eError::io("writing identity"))?;
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &Variant::baseline(Sandbox::Bwrap),
        &launcher(),
        &tools,
    )?)?;
    assert_eq!(doc.fleets[0].action_environment.as_str(), identity);
    Ok(())
}

#[test]
fn bwrap_requires_a_program() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let mut tools = tools()?;
    tools.bwrap = None;
    let error = template
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)
        .expect_err("bwrap requires its program");
    assert_eq!(
        error.to_string(),
        "system under test: the bwrap binary is unset: run the harness inside `nix develop`, or set `bwrap` in the settings"
    );
    Ok(())
}

#[test]
fn oversized_fleet_renders_disk_and_relaunch_backoff() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let variant = Variant {
        execution: ExecutionKnobs::OVERSIZED_FLEET,
        ..Variant::baseline(Sandbox::Noop)
    };
    let doc = typed_rendered(&template.vary(&variant, &launcher(), &*(tools()?))?)?;
    assert_eq!(
        doc.fleets[0].disk_per_instance.get(),
        "1024TiB".parse::<Bytes>().expect("fleet disk is valid")
    );
    let ProvisionerConfig::Local(local) = doc.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(
        local.relaunch_backoff.get(),
        std::time::Duration::from_secs(1)
    );
    Ok(())
}

#[test]
fn vary_bwrap_denied_breaks_input_links() -> gang_e2e::Result<()> {
    let variant = Variant {
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &variant,
        &launcher(),
        &*(tools()?),
    )?)?;
    assert_eq!(doc.fleets[0].sandbox, SandboxKey::Bwrap);
    assert!(!doc.fleets[0].actions.grants.contains(&Capability::Network));
    assert!(doc.execution.egress.is_none());
    assert!(doc.sandbox.bwrap.expect("bwrap sandbox").egress.is_none());
    assert_eq!(doc.execution.input_links, InputLinks::Break);
    Ok(())
}

#[test]
fn vary_bwrap_permitted_keeps_input_links() -> gang_e2e::Result<()> {
    let variant = Variant {
        permit_network: PermitNetwork::Permitted,
        input_links: InputLinks::Keep,
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &variant,
        &launcher(),
        &*(tools()?),
    )?)?;
    assert!(doc.fleets[0].actions.grants.contains(&Capability::Network));
    assert!(doc.execution.egress.is_some());
    let egress = doc
        .sandbox
        .bwrap
        .expect("bwrap sandbox")
        .egress
        .expect("egress helpers");
    assert_eq!(egress.nft, PathBuf::from("/nix/store/egress/bin/nft"));
    assert_eq!(doc.execution.input_links, InputLinks::Keep);
    Ok(())
}

#[test]
fn network_grant_requires_tools_and_policy() -> gang_e2e::Result<()> {
    let variant = Variant {
        permit_network: PermitNetwork::Permitted,
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let mut missing_tools = tools()?;
    missing_tools.egress = None;
    assert!(
        ConfigTemplate::parse(&base_config()?)?
            .vary(&variant, &launcher(), &missing_tools)
            .is_err()
    );
    let mut missing_policy = tools()?;
    missing_policy.egress.as_mut().expect("egress tools").policy = None;
    assert!(
        ConfigTemplate::parse(&base_config()?)?
            .vary(&variant, &launcher(), &missing_policy)
            .is_err()
    );
    let mut relative_tool = tools()?;
    relative_tool.egress.as_mut().expect("egress tools").nft = "nft".into();
    assert!(
        ConfigTemplate::parse(&base_config()?)?
            .vary(&variant, &launcher(), &relative_tool)
            .is_err()
    );
    Ok(())
}

#[test]
fn vary_noop_selects_the_noop_sandbox() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let mut tools = tools()?;
    tools.action_rootfs = None;
    let doc =
        typed_rendered(&template.vary(&Variant::baseline(Sandbox::Noop), &launcher(), &tools)?)?;
    assert_eq!(doc.fleets[0].sandbox, SandboxKey::Noop);
    assert_eq!(doc.fleets[0].action_environment.as_str(), "host:e2e");
    assert_eq!(doc.execution.input_links, InputLinks::Break);
    assert_eq!(
        doc.sandbox.noop.expect("noop sandbox").environment.as_str(),
        "host:e2e"
    );
    Ok(())
}

#[test]
fn vary_bwrap_without_its_section_fails_naming_it() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&without_sandbox(SandboxKey::Bwrap)?)?;
    let error = template
        .vary(
            &Variant::baseline(Sandbox::Bwrap),
            &launcher(),
            &*(tools()?),
        )
        .expect_err("a bwrap variant without [sandbox.bwrap] fails");
    assert!(
        error.to_string().contains("sandbox.bwrap"),
        "the error names the section: {}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn vary_noop_without_its_section_fails_naming_it() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&without_sandbox(SandboxKey::Noop)?)?;
    let error = template
        .vary(&Variant::baseline(Sandbox::Noop), &launcher(), &*(tools()?))
        .expect_err("a noop variant without [sandbox.noop] fails");
    assert!(
        error.to_string().contains("sandbox.noop"),
        "the error names the section: {}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn vary_cgroup_adds_the_action_limits() -> gang_e2e::Result<()> {
    let variant = Variant {
        limits: ActionLimits::Cgroup,
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &variant,
        &launcher(),
        &*(tools()?),
    )?)?;
    let Some(LimitsConfig::Cgroup(cgroup)) = doc.limits else {
        panic!("cgroup limits expected");
    };
    assert_eq!(cgroup.delegation, gang_cgroup::Delegation::Trusted);
    assert_eq!(cgroup.max_pids.get(), LIMIT_MAX_PIDS);
    assert_eq!(
        cgroup.cpu_period.get(),
        std::time::Duration::from_micros(LIMIT_CPU_PERIOD_MICROS)
    );
    Ok(())
}

#[test]
fn vary_absent_removes_a_limits_table_of_the_base() -> gang_e2e::Result<()> {
    let text = format!(
        "{}\n[limits.cgroup]\ndelegation = \"trusted\"\nmax_pids = 1\n",
        base_config()?
    );
    let base = ConfigTemplate::parse(&text)?;
    assert!(typed_rendered(&base)?.limits.is_some());
    let doc = typed_rendered(&base.vary(
        &Variant::baseline(Sandbox::Bwrap),
        &launcher(),
        &*(tools()?),
    )?)?;
    assert!(doc.limits.is_none());
    Ok(())
}

/// The fixture placed over TLS with the e2e identity provider added for `issuer`.
fn identified(issuer: &Url) -> gang_e2e::Result<(Placement, Document<Plugins>)> {
    let (placement, template) = placed(Transport::Tls)?;
    Ok((
        placement,
        typed_rendered(&template.with_identity(issuer, IdentityKnobs::NONE)?)?,
    ))
}

/// Finds an OIDC provider in the typed document by its configured name.
fn oidc<'a>(
    document: &'a Document<Plugins>,
    name: &str,
) -> gang_e2e::Result<&'a gang_oidc::OidcConfig> {
    let (_, provider) = document
        .identity
        .providers
        .iter()
        .find(|(key, _)| key.as_str() == name)
        .ok_or_else(|| E2eError::Template(format!("missing identity provider {name}")))?;
    let IdentityProviderConfig::Oidc(config) = provider;
    Ok(config)
}

#[test]
fn with_identity_adds_the_e2e_provider() -> gang_e2e::Result<()> {
    let issuer = Url::parse("https://issuer.example.test/").expect("a valid url");
    let (placement, doc) = identified(&issuer)?;

    assert_eq!(doc.identity.providers.len(), 2);
    let workers = oidc(&doc, "local-workers")?;
    let e2e = oidc(&doc, "e2e")?;
    let WorkerAuthenticationConfig::Mtls(mtls) = &doc.authentication.workers else {
        panic!("mtls authentication expected");
    };
    assert_eq!(
        mtls.certificate_ttl.get(),
        std::time::Duration::from_hours(24)
    );
    let imds = format!("http://{}", listen(placement.imds_port()));
    assert_eq!(
        workers.issuer_url.as_url().as_str().trim_end_matches('/'),
        imds
    );
    assert!(workers.required_claims.is_empty());
    let ClientAuthenticationConfig::Token(token) = &doc.authentication.clients else {
        panic!("token authentication expected");
    };
    let providers: Vec<&str> = token.providers().iter().map(ProviderName::as_str).collect();
    assert_eq!(providers, ["e2e"]);
    assert_eq!(e2e.issuer_url.as_url().as_str(), issuer.as_str());
    assert_eq!(e2e.audience.as_str(), AUDIENCE);
    assert_eq!(
        e2e.required_claims
            .get(ORGANIZATION_CLAIM)
            .and_then(|value| value.as_str()),
        Some(ORGANIZATION)
    );
    let gang_config::Transport::Tls { trust_domain } = &doc.server.transport else {
        panic!("tls transport expected");
    };
    assert_eq!(trust_domain.as_str(), "gang.local");
    Ok(())
}

#[test]
fn with_identity_keeps_existing_e2e_oidc_keys() -> gang_e2e::Result<()> {
    let text = format!(
        concat!(
            "{}\n[identity.providers.e2e.oidc]\n",
            "issuer_url = \"https://old.example/\"\n",
            "audience = \"old\"\nprincipal_claim = \"sub\"\nadditional_audiences = [\"x\"]\n",
        ),
        base_config()?
    );
    let placement = Placement::create(None)?;
    let issuer = Url::parse("https://issuer.example.test/").expect("a valid url");
    let doc = typed_rendered(
        &ConfigTemplate::parse(&text)?
            .place(&placement, Transport::Tls)?
            .with_identity(&issuer, IdentityKnobs::NONE)?,
    )?;

    let provider = oidc(&doc, "e2e")?;
    assert_eq!(provider.additional_audiences.len(), 1);
    assert_eq!(provider.additional_audiences[0].as_str(), "x");
    assert_eq!(provider.issuer_url.as_url().as_str(), issuer.as_str());
    assert_eq!(provider.audience.as_str(), AUDIENCE);
    Ok(())
}

#[test]
fn with_identity_grants_the_administrator_roles() -> gang_e2e::Result<()> {
    let issuer = Url::parse("https://issuer.example.test/").expect("a valid url");
    let (_placement, doc) = identified(&issuer)?;

    let roles = &doc.server.roles;
    assert_eq!(roles.len(), 1);
    assert_eq!(roles[0].name.as_str(), "administrator");
    assert_eq!(roles[0].permissions, PermissionSet::all());
    assert_eq!(roles[0].principals.len(), 1);
    assert_eq!(roles[0].principals[0].provider.as_str(), "e2e");
    assert_eq!(roles[0].principals[0].principal.as_str(), "administrator");
    Ok(())
}

#[test]
fn with_identity_keeps_the_other_server_keys() -> gang_e2e::Result<()> {
    let text = base_config()?.replace("[server]\n", "[server]\ncertificate_ttl = \"10d\"\n");
    let placement = Placement::create(None)?;
    let issuer = Url::parse("https://issuer.example.test/").expect("a valid url");
    let doc = typed_rendered(
        &ConfigTemplate::parse(&text)?
            .place(&placement, Transport::Tls)?
            .with_identity(&issuer, IdentityKnobs::NONE)?,
    )?;
    assert_eq!(
        doc.server.certificate_ttl.get(),
        std::time::Duration::from_hours(240)
    );
    assert!(!doc.server.roles.is_empty());
    Ok(())
}

#[test]
fn with_identity_grants_the_administrator_every_permission() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Tls)?;
    let issuer = Url::parse("https://issuer.example.test/").expect("a valid url");
    let text = template
        .with_identity(&issuer, IdentityKnobs::NONE)?
        .render()?;
    let config = match text.parse::<GangConfig<OpaquePlugins>>() {
        Ok(config) => config,
        Err(error) => panic!("the identified config does not parse as a gang.toml: {error}"),
    };
    let roles = &config.server.roles;
    assert_eq!(roles.len(), 1);
    let role = &roles[0];
    assert_eq!(role.name.as_str(), "administrator");
    assert_eq!(role.permissions, PermissionSet::all());
    assert_eq!(role.principals.len(), 1);
    assert_eq!(role.principals[0].provider.as_str(), "e2e");
    assert_eq!(role.principals[0].principal.as_str(), "administrator");
    Ok(())
}

#[test]
fn place_fails_naming_the_local_workers_oidc_table() -> gang_e2e::Result<()> {
    let text = base_config()?.replace(
        "[identity.providers.local-workers.oidc]",
        "[identity.providers.local-workers.other]",
    );
    let error =
        ConfigTemplate::parse(&text).expect_err("an unknown identity provider plugin fails");
    assert!(
        error.to_string().contains("other"),
        "the error names the plugin: {}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn with_ca_pin_sets_the_server_pin() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Tls)?;
    let pin = CaPin::new([0x42; 32]);
    let doc = typed_rendered(&template.with_ca_pin(&pin)?)?;
    assert_eq!(
        doc.server.ca_pin,
        Some(gang_config::CaPinSetting::Pinned(pin))
    );
    Ok(())
}

#[test]
fn with_ca_pin_fails_without_a_server_table() -> gang_e2e::Result<()> {
    let mut table: Table = toml::from_str(&base_config()?).expect("base is TOML");
    table.remove("server");
    let text = toml::to_string(&table).expect("base renders");
    let error = ConfigTemplate::parse(&text).expect_err("a configuration without [server] fails");
    assert!(
        error.to_string().contains("server"),
        "the error names the section: {}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn signer_key_file_is_the_placed_ca_key() -> gang_e2e::Result<()> {
    let (placement, template) = placed(Transport::Tls)?;
    assert_eq!(template.signer_key_file()?, placement.ca_key());
    Ok(())
}

#[test]
fn signer_key_file_fails_naming_a_missing_key() -> gang_e2e::Result<()> {
    let text = base_config()?.replace("key_file = \"/tmp/gang-local/ca.key\"", "");
    let error = ConfigTemplate::parse(&text).expect_err("a missing signer key fails");
    assert!(
        error.to_string().contains("key_file"),
        "the error names the key: {}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn vary_cgroup_writes_the_worker_launcher_in_order() -> gang_e2e::Result<()> {
    let variant = Variant {
        limits: ActionLimits::Cgroup,
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &variant,
        &launcher(),
        &*(tools()?),
    )?)?;
    let ProvisionerConfig::Local(local) = doc.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(local.worker_launcher.argv(), launcher());
    Ok(())
}

#[test]
fn launch_failure_preset_sets_missing_launcher_and_short_timeout() -> gang_e2e::Result<()> {
    let variant = Variant {
        control: ControlKnobs::LAUNCH_FAILURE,
        ..Variant::baseline(Sandbox::Noop)
    };
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &variant,
        &launcher(),
        &*(tools()?),
    )?)?;
    assert_eq!(
        doc.client_action_timeout.get(),
        std::time::Duration::from_secs(15)
    );
    assert_eq!(doc.poll_interval.get(), std::time::Duration::from_secs(1));
    assert_eq!(
        doc.fleets[0].idle_timeout,
        std::time::Duration::from_secs(5)
    );
    let ProvisionerConfig::Local(local) = doc.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(
        local.worker_launcher.argv(),
        ["gang-e2e-nonexistent-worker-launcher"]
    );
    Ok(())
}

#[test]
fn fleets_preset_justifies_overflow_without_changing_placement_or_polling() -> gang_e2e::Result<()>
{
    let base = base_config()?;
    for sandbox in [Sandbox::Noop, Sandbox::Bwrap] {
        let variant = placement_variant(sandbox);
        let untuned = typed_rendered(&ConfigTemplate::parse(&base)?.vary(
            &variant,
            &launcher(),
            &*(tools()?),
        )?)?;
        let tuned = typed_rendered(&ConfigTemplate::parse(&base)?.vary(
            &Variant {
                control: ControlKnobs::FLEETS,
                ..variant
            },
            &launcher(),
            &*(tools()?),
        )?)?;
        assert_eq!(tuned.poll_interval, untuned.poll_interval);
        assert_eq!(
            tuned.poll_interval.get(),
            std::time::Duration::from_millis(500)
        );
        assert_eq!(tuned.client_action_timeout, untuned.client_action_timeout);
        assert_eq!(tuned.fleets.len(), 3);
        assert_eq!(tuned.scaling.payback_percent.get(), 50);
        assert_eq!(
            tuned.scaling.unknown_action_duration.get(),
            std::time::Duration::from_mins(10),
        );
        for (fleet, before) in tuned.fleets.iter().zip(&untuned.fleets) {
            assert_eq!(fleet.name, before.name);
            assert_eq!(fleet.sandbox, before.sandbox);
            assert_eq!(fleet.min_instances, before.min_instances);
            assert_eq!(fleet.max_instances, before.max_instances);
            assert_eq!(fleet.idle_timeout, std::time::Duration::from_secs(2));
            assert_eq!(fleet.max_drain_wait, before.max_drain_wait);
            assert_eq!(fleet.boot_grace, before.boot_grace);
        }
        let ProvisionerConfig::Local(local) = tuned.provisioner else {
            panic!("local provisioner expected");
        };
        assert_eq!(
            local.terminate_grace.get(),
            std::time::Duration::from_secs(1)
        );
    }
    Ok(())
}

#[test]
fn drain_cancel_bounds_only_its_drain_and_termination_waits() -> gang_e2e::Result<()> {
    let base = base_config()?;
    let variant = placement_variant(Sandbox::Noop);
    let tuned = typed_rendered(&ConfigTemplate::parse(&base)?.vary(
        &Variant {
            control: ControlKnobs::DRAIN_CANCEL,
            ..variant
        },
        &launcher(),
        &*(tools()?),
    )?)?;
    assert_eq!(tuned.poll_interval.get(), std::time::Duration::from_secs(2));
    assert_eq!(
        tuned.dashboard.detail_interval.get(),
        std::time::Duration::from_secs(1)
    );
    for fleet in &tuned.fleets {
        assert_eq!(fleet.idle_timeout, std::time::Duration::from_secs(5));
        assert_eq!(fleet.max_drain_wait, std::time::Duration::from_secs(10));
        assert_eq!(fleet.min_instances, 0);
        assert_eq!(fleet.max_instances.get(), 1);
    }
    let untuned = typed_rendered(&ConfigTemplate::parse(&base)?.vary(
        &variant,
        &launcher(),
        &*(tools()?),
    )?)?;
    assert_eq!(tuned.client_action_timeout, untuned.client_action_timeout);
    for fleet in &untuned.fleets {
        assert_eq!(fleet.max_drain_wait, std::time::Duration::from_mins(1));
    }
    let ProvisionerConfig::Local(local) = tuned.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(
        local.terminate_grace.get(),
        std::time::Duration::from_secs(1)
    );
    let ProvisionerConfig::Local(local) = untuned.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(
        local.terminate_grace.get(),
        std::time::Duration::from_secs(10)
    );
    Ok(())
}

#[test]
fn scheduled_minimum_preset_applies_to_every_fleet() -> gang_e2e::Result<()> {
    let variant = Variant {
        control: ControlKnobs::SCHEDULED_MINIMUM,
        fleets: FleetLayout::Placement,
        ..Variant::baseline(Sandbox::Noop)
    };
    let template =
        ConfigTemplate::parse(&base_config()?)?.vary(&variant, &launcher(), &*(tools()?))?;
    let text = template.render()?;
    let document: toml::Table = text
        .parse()
        .map_err(|error: toml::de::Error| E2eError::Template(error.to_string()))?;
    let fleets = document["fleets"].as_array().expect("fleets");
    assert_eq!(fleets.len(), 3);
    for fleet in fleets {
        assert_eq!(fleet["min_instances"].as_integer(), Some(0));
        assert_eq!(fleet["max_instances"].as_integer(), Some(1));
        let windows = fleet["scheduled_minimum"].as_array().expect("windows");
        assert_eq!(windows.len(), 1);
        let window = &windows[0];
        assert_eq!(window["instances"].as_integer(), Some(1));
        assert_eq!(
            window["days"]
                .as_array()
                .expect("days")
                .iter()
                .filter_map(toml::Value::as_str)
                .collect::<Vec<_>>(),
            ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        );
        assert_eq!(window["start"].as_str(), Some("00:00"));
        assert_eq!(window["end"].as_str(), Some("24:00"));
        assert_eq!(window["time_zone"].as_str(), Some("Asia/Kathmandu"));
    }
    Ok(())
}

#[test]
fn scheduled_minimum_zero_is_a_template_error() -> gang_e2e::Result<()> {
    let variant = Variant {
        control: ControlKnobs {
            scheduled_minimum: Some(0),
            ..ControlKnobs::NONE
        },
        ..Variant::baseline(Sandbox::Noop)
    };
    let result = ConfigTemplate::parse(&base_config()?)?.vary(&variant, &launcher(), &*(tools()?));
    assert!(matches!(result, Err(E2eError::Template(_))));
    Ok(())
}

#[test]
fn vary_absent_removes_the_worker_launcher_of_the_base() -> gang_e2e::Result<()> {
    let text = base_config()?.replace(
        "[provisioner.local]",
        "[provisioner.local]\nworker_launcher = [\"systemd-run\"]",
    );
    let base = ConfigTemplate::parse(&text)?;
    let before = typed_rendered(&base)?;
    let ProvisionerConfig::Local(local) = before.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(local.worker_launcher.argv().len(), 1);
    let doc = typed_rendered(&base.vary(
        &Variant::baseline(Sandbox::Bwrap),
        &launcher(),
        &*(tools()?),
    )?)?;
    let ProvisionerConfig::Local(local) = doc.provisioner else {
        panic!("local provisioner expected");
    };
    assert!(local.worker_launcher.argv().is_empty());
    Ok(())
}

/// The variant of the placement layout over `sandbox`.
fn placement_variant(sandbox: Sandbox) -> Variant {
    Variant {
        fleets: FleetLayout::Placement,
        ..Variant::baseline(sandbox)
    }
}

/// The `[[fleets]]` of a rendered placement template of the fixture under `sandbox`.
fn placement_fleets(sandbox: Sandbox) -> gang_e2e::Result<Vec<RawFleet<SandboxKey>>> {
    let template = ConfigTemplate::parse(&base_config()?)?.vary(
        &placement_variant(sandbox),
        &launcher(),
        &*(tools()?),
    )?;
    Ok(typed_rendered(&template)?.fleets)
}

#[test]
fn single_layout_keeps_the_one_fleet_of_the_base() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let doc = typed_rendered(&template.vary(
        &Variant::baseline(Sandbox::Bwrap),
        &launcher(),
        &*(tools()?),
    )?)?;
    assert_eq!(doc.fleets.len(), 1);
    assert_eq!(doc.fleets[0].name.as_str(), "local-worker-x86-64");
    Ok(())
}

#[test]
fn placement_layout_declares_the_fleets_in_preference_order() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    let names: Vec<&str> = fleets.iter().map(|fleet| fleet.name.as_str()).collect();
    assert_eq!(names, [SMALL_FLEET, LARGE_FLEET, NOOP_FLEET]);
    Ok(())
}

#[test]
fn placement_small_fleet_keeps_the_base_sizing_and_holds_one_instance() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    let small = &fleets[0];
    assert_eq!(small.sandbox, SandboxKey::Bwrap);
    assert_eq!(small.action_environment.as_str(), ROOTFS_IDENTITY);
    assert_eq!(small.vcpus_per_instance.get(), 4);
    assert_eq!(small.memory_per_instance, gang_types::NonZeroBytes::gib(8));
    assert_eq!(small.disk_per_instance, gang_types::NonZeroBytes::gib(20));
    assert_eq!(small.min_instances, 0);
    assert_eq!(small.max_instances.get(), 1);
    Ok(())
}

#[test]
fn placement_large_fleet_doubles_the_size_under_the_same_sandbox() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    let large = &fleets[1];
    assert_eq!(large.sandbox, SandboxKey::Bwrap);
    assert_eq!(large.action_environment.as_str(), ROOTFS_IDENTITY);
    assert_eq!(large.vcpus_per_instance.get(), 8);
    assert_eq!(large.memory_per_instance, gang_types::NonZeroBytes::gib(16));
    assert_eq!(large.disk_per_instance, gang_types::NonZeroBytes::gib(20));
    assert_eq!(large.min_instances, 0);
    assert_eq!(large.max_instances.get(), 2);
    Ok(())
}

#[test]
fn placement_noop_fleet_has_the_small_size_under_the_noop_sandbox() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    let noop = &fleets[2];
    assert_eq!(noop.sandbox, SandboxKey::Noop);
    assert_eq!(noop.action_environment.as_str(), "host:e2e");
    assert_eq!(noop.vcpus_per_instance.get(), 4);
    assert_eq!(noop.memory_per_instance, gang_types::NonZeroBytes::gib(8));
    assert_eq!(noop.disk_per_instance, gang_types::NonZeroBytes::gib(20));
    assert_eq!(noop.max_instances.get(), 1);
    Ok(())
}

#[test]
fn placement_layout_puts_the_first_two_fleets_under_the_variants_sandbox() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Noop)?;
    for fleet in &fleets {
        assert_eq!(fleet.sandbox, SandboxKey::Noop);
        assert_eq!(fleet.action_environment.as_str(), "host:e2e");
    }
    Ok(())
}

#[test]
fn placement_layout_carries_over_the_other_keys_of_the_base() -> gang_e2e::Result<()> {
    let base = ConfigTemplate::parse(&base_config()?)?;
    let base_doc = typed_rendered(&base)?;
    let base_fleet = &base_doc.fleets[0];
    for fleet in &placement_fleets(Sandbox::Bwrap)? {
        assert_eq!(fleet.isa, base_fleet.isa);
        assert_eq!(fleet.idle_timeout, base_fleet.idle_timeout);
        assert_eq!(fleet.max_drain_wait, base_fleet.max_drain_wait);
        assert_eq!(fleet.boot_grace, base_fleet.boot_grace);
    }
    let template = base.vary(
        &placement_variant(Sandbox::Bwrap),
        &launcher(),
        &*(tools()?),
    )?;
    let doc = typed_rendered(&template)?;
    assert_eq!(doc.default_isa, base_doc.default_isa);
    assert!(doc.sandbox.noop.is_some());
    Ok(())
}

#[test]
fn placement_layout_needs_the_noop_sandbox_section() -> gang_e2e::Result<()> {
    let text = without_sandbox(SandboxKey::Noop)?;
    let error = ConfigTemplate::parse(&text)?
        .vary(
            &placement_variant(Sandbox::Bwrap),
            &launcher(),
            &*(tools()?),
        )
        .expect_err("the noop fleet has no sandbox section");
    assert!(
        error.to_string().contains("sandbox.noop"),
        "{}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn placement_layout_needs_exactly_one_base_fleet() -> gang_e2e::Result<()> {
    let base = base_config()?;
    let mut table: Table = toml::from_str(&base).expect("base is TOML");
    let fleets = table
        .get_mut("fleets")
        .and_then(toml::Value::as_array_mut)
        .expect("base fleets");
    let mut extra = fleets[0].clone();
    extra
        .as_table_mut()
        .expect("fleet table")
        .insert("name".into(), "extra".into());
    fleets.push(extra);
    let text = toml::to_string(&table).expect("base renders");
    let error = ConfigTemplate::parse(&text)?
        .vary(
            &placement_variant(Sandbox::Bwrap),
            &launcher(),
            &*(tools()?),
        )
        .expect_err("two base fleets are ambiguous");
    assert!(
        error.to_string().contains("exactly one"),
        "{}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

#[test]
fn large_memory_is_what_the_large_fleet_registers_in_kib() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    assert_eq!(
        template.large_memory_kb()?,
        fleets[1].memory_per_instance.as_u64() / 1024
    );
    Ok(())
}

#[test]
fn large_memory_exceeds_the_small_fleet() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    assert!(template.large_memory_kb()? > fleets[0].memory_per_instance.as_u64() / 1024);
    Ok(())
}

#[test]
fn large_memory_is_the_same_for_the_base_and_its_placement_layout() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let placed = ConfigTemplate::parse(&base_config()?)?.vary(
        &placement_variant(Sandbox::Bwrap),
        &launcher(),
        &*(tools()?),
    )?;
    assert_eq!(template.large_memory_kb()?, placed.large_memory_kb()?);
    Ok(())
}

#[test]
fn large_memory_fails_naming_the_missing_memory() -> gang_e2e::Result<()> {
    let base = base_config()?;
    let text = base.replace("memory_per_instance = \"8GiB\"\n", "");
    assert_ne!(text, base);
    let error = ConfigTemplate::parse(&text).expect_err("a fleet without memory is invalid");
    assert!(
        error.to_string().contains("memory_per_instance"),
        "{}",
        gang_types::error_chain(&error)
    );
    Ok(())
}

/// A plugin set whose sections are opaque tables. The parse test checks the shape of each rendered
/// `gang.toml` against the real `GangConfig`, not the plugins' own options.
#[derive(Debug)]
struct OpaquePlugins;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum OpaqueSandboxKey {
    Bwrap,
    Noop,
}

impl fmt::Display for OpaqueSandboxKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Bwrap => "bwrap",
            Self::Noop => "noop",
        })
    }
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct OpaqueSandboxes {
    bwrap: Option<Table>,
    noop: Option<Table>,
}

impl SandboxSelection for OpaqueSandboxes {
    type Key = OpaqueSandboxKey;

    fn is_configured(&self, key: Self::Key) -> bool {
        match key {
            OpaqueSandboxKey::Bwrap => self.bwrap.is_some(),
            OpaqueSandboxKey::Noop => self.noop.is_some(),
        }
    }

    fn supports_capability(&self, _key: Self::Key, _capability: Capability) -> bool {
        false
    }

    fn try_for_each_program<E>(
        &mut self,
        _key: Self::Key,
        _visit: impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }

    fn try_for_each_root<E>(
        &mut self,
        _key: Self::Key,
        _visit: impl FnMut(&mut AbsolutePathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct OpaqueAuthentication {
    none: Option<Table>,
}

impl AuthenticationSettings for OpaqueAuthentication {
    fn requires_tls(&self) -> bool {
        self.none.is_none()
    }
}

impl PluginSet for OpaquePlugins {
    type Provisioner = Table;
    type Cache = Table;
    type Signer = Table;
    type Database = Table;
    type SandboxKey = OpaqueSandboxKey;
    type Sandbox = OpaqueSandboxes;
    type IdentityProvider = Table;
    type ClientAuthentication = OpaqueAuthentication;
    type WorkerAuthentication = OpaqueAuthentication;
    type Limits = Table;
    type FleetOptions = Table;

    fn fleet_options(
        _provisioner: &Self::Provisioner,
        table: FleetOptionsTable,
    ) -> Result<Self::FleetOptions, FleetOptionsError> {
        Ok(table)
    }

    fn option_properties(_options: &Self::FleetOptions) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

#[test]
fn rendered_tls_and_plaintext_configs_parse_as_gang_configs() -> gang_e2e::Result<()> {
    for (label, transport) in [("tls", Transport::Tls), ("plaintext", Transport::Plain)] {
        let (_placement, template) = placed(transport)?;
        let text = template.render()?;
        if let Err(error) = text.parse::<GangConfig<OpaquePlugins>>() {
            panic!("the {label} config does not parse as a gang.toml: {error}");
        }
    }
    Ok(())
}

fn check_real_plugin_parse(text: &str, cold_start: gang_e2e::ColdStart) -> gang_e2e::Result<()> {
    let parsed = text.parse::<GangConfig<gang_main::plugins::Plugins>>();
    if cold_start == gang_e2e::ColdStart::Misfit {
        let Err(error) = parsed else {
            return Err(E2eError::Template(
                "misfit cold start was accepted".to_owned(),
            ));
        };
        assert!(error.to_string().contains("actions.cold_start"), "{error}");
    } else {
        parsed.map_err(|error| E2eError::Template(error.to_string()))?;
    }
    Ok(())
}

fn check_base_variants(
    check: &dyn Fn(Variant, Transport, &str) -> gang_e2e::Result<()>,
) -> gang_e2e::Result<()> {
    for sandbox in [Sandbox::Noop, Sandbox::Bwrap] {
        for permit_network in [PermitNetwork::Denied, PermitNetwork::Permitted] {
            if sandbox == Sandbox::Noop && permit_network == PermitNetwork::Permitted {
                continue;
            }
            for input_links in [InputLinks::Keep, InputLinks::Break] {
                for limits in [ActionLimits::Absent, ActionLimits::Cgroup] {
                    for fleets in [FleetLayout::Single, FleetLayout::Placement] {
                        for soft_memory in [SoftMemory::Enforced, SoftMemory::Off] {
                            let variant = Variant {
                                sandbox,
                                permit_network,
                                input_links,
                                limits,
                                fleets,
                                soft_memory,
                                ..Variant::baseline(sandbox)
                            };
                            for transport in [Transport::Plain, Transport::Tls] {
                                check(variant, transport, "base")?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn every_rendered_variant_parses_with_the_real_plugins() -> gang_e2e::Result<()> {
    let base = base_config()?;
    let placement = Placement::create(None)?;
    let issuer = Url::parse("https://issuer.example.test/").expect("a valid issuer URL");
    let worker_launcher = launcher();

    let check = |variant: Variant, transport: Transport, preset: &str| -> gang_e2e::Result<()> {
        let result: gang_e2e::Result<()> = (|| {
            let template = ConfigTemplate::parse(&base)?
                .place(&placement, transport)?
                .vary(&variant, &worker_launcher, &*(tools()?))?;
            let template = if transport == Transport::Tls {
                template.with_identity(&issuer, variant.identity)?
            } else {
                template
            };
            let text = template.render()?;
            check_real_plugin_parse(&text, variant.execution.cold_start)
        })();
        result.map_err(|error| {
            E2eError::Template(format!(
                "rendering or parsing {variant:?}, {preset}, over {transport:?}: {}",
                gang_types::error_chain(&error)
            ))
        })
    };

    check_base_variants(&check)?;
    // Exercise each family independently for each sandbox and transport, in addition to the
    // complete base product above, without multiplying the family presets by every base knob.
    for sandbox in [Sandbox::Noop, Sandbox::Bwrap] {
        for transport in [Transport::Plain, Transport::Tls] {
            let baseline = Variant::baseline(sandbox);
            for &preset in IdentityKnobs::PRESETS {
                // The identity scenario uses Secured::create, which always selects TLS;
                // plaintext placement removes the provider needed for interactive sign-in.
                if transport == Transport::Plain {
                    continue;
                }
                check(
                    Variant {
                        identity: preset,
                        ..baseline
                    },
                    transport,
                    &format!("identity preset {preset:?}"),
                )?;
            }
            for &preset in ServerKnobs::PRESETS {
                let mut variant = baseline;
                variant.server = preset;
                check(variant, transport, &format!("server preset {preset:?}"))?;
            }
            for preset in ControlKnobs::PRESETS {
                check(
                    Variant {
                        control: preset,
                        fleets: if preset == ControlKnobs::FLEETS {
                            FleetLayout::Placement
                        } else {
                            baseline.fleets
                        },
                        ..baseline
                    },
                    transport,
                    &format!("control preset {preset:?}"),
                )?;
            }
            for &preset in ExecutionKnobs::PRESETS {
                // suite.rs runs worker renewal with this preset only over TLS; plaintext has
                // no worker mTLS table (execution_variant and execution_checks).
                if preset == ExecutionKnobs::SHORT_LIVED_CERTIFICATES
                    && transport == Transport::Plain
                {
                    continue;
                }
                check(
                    Variant {
                        execution: preset,
                        ..baseline
                    },
                    transport,
                    &format!("execution preset {preset:?}"),
                )?;
            }
        }
    }
    Ok(())
}
