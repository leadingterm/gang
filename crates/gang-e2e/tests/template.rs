//! Tests of the `gang.toml` template: placement, variants, identity, and the typed accessors,
//! run against the local-mode configuration of the specification.

use std::fmt;
use std::path::PathBuf;

use gang_config::{
    Document, FleetOptionsError, FleetOptionsTable, GangConfig, InputLinks, PluginSet, RawFleet,
    SandboxSelection,
};
use gang_e2e::{
    AUDIENCE, ActionLimits, ActionTools, ConfigTemplate, ControlKnobs, E2eError, ExecutionKnobs,
    FleetLayout, IdentityKnobs, LARGE_FLEET, LIMIT_CPU_PERIOD_MICROS, LIMIT_HEADROOM_PERCENT,
    LIMIT_MAX_PIDS, NOOP_FLEET, ORGANIZATION, ORGANIZATION_CLAIM, PermitNetwork, Placement,
    SMALL_FLEET, Sandbox, ServerKnobs, SoftMemory, Transport, Variant,
};
use gang_main::plugins::{
    CacheConfig, ClientAuthenticationConfig, DatabaseConfig, IdentityProviderConfig, LimitsConfig,
    Plugins, ProvisionerConfig, SandboxKey, SchedulerConfig, SignerConfig,
    WorkerAuthenticationConfig,
};
use gang_nativelink::NativeLinkConfig;
use gang_types::{
    AbsolutePathBuf, AuthenticationSettings, Bytes, CaPin, PermissionSet, ProviderName,
    SchedulerSettings, Url,
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

/// Self-contained tools from two Nix store closures.
fn tools() -> ActionTools {
    ActionTools {
        shell: PathBuf::from("/nix/store/aaa-bash/bin/bash"),
        bwrap: Some(PathBuf::from("/nix/store/ccc-bubblewrap/bin/bwrap")),
        path: "/nix/store/aaa-bash/bin:/nix/store/bbb-coreutils/bin".to_owned(),
        action_bin: PathBuf::from("/nix/store/ddd-action-tools/bin"),
        glibc_tool: None,
        glibc_loader: Vec::new(),
    }
}

#[test]
fn placed_base_omits_announced_endpoints_and_parses_as_toml() -> gang_e2e::Result<()> {
    for transport in [Transport::Plain, Transport::Tls] {
        let (_, template) = placed(transport)?;
        let doc = typed_rendered(&template)?;
        assert!(doc.endpoint.is_none());
        assert!(doc.scheduler.worker_endpoint().is_none());
    }
    Ok(())
}

#[test]
fn place_gives_every_local_fleet_the_harness_fleet_disk() -> gang_e2e::Result<()> {
    let placement = Placement::create(None)?;
    let template = ConfigTemplate::parse(&base_config()?)?
        .place(&placement, Transport::Tls)?
        .vary(&placement_variant(Sandbox::Bwrap), &launcher(), &tools())?;
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
    let DatabaseConfig::Sqlite(database) = &doc.database;
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
    assert!(doc.scheduler.worker_endpoint().is_none());
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
    assert!(doc.scheduler.worker_endpoint().is_none());
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
    assert!(doc.scheduler.worker_endpoint().is_none());
    Ok(())
}

#[test]
fn place_nativelink_table_parses_with_the_scheduler_config() -> gang_e2e::Result<()> {
    for transport in [Transport::Plain, Transport::Tls] {
        let (_placement, template) = placed(transport)?;
        let doc = typed_rendered(&template)?;
        let SchedulerConfig::Nativelink(_config) = doc.scheduler;
    }
    Ok(())
}

#[test]
fn three_cold_actions_fit_one_placed_worker() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Tls)?;
    let template = template.vary(&placement_variant(Sandbox::Bwrap), &launcher(), &tools())?;
    let doc = typed_rendered(&template)?;
    let SchedulerConfig::Nativelink(nativelink) = &doc.scheduler;
    let cold_memory = nativelink.cold_start.memory.get().get();
    let cold_disk = nativelink.cold_start.disk.get().get();
    let fleet = &doc.fleets[0];
    assert!(
        cold_memory.saturating_mul(3) <= fleet.memory_per_instance.as_u64(),
        "three cold actions of {cold_memory} bytes of memory do not fit one worker of {}",
        fleet.memory_per_instance
    );
    assert!(
        cold_disk.saturating_mul(3) <= fleet.disk_per_instance.as_u64(),
        "three cold actions of {cold_disk} bytes of disk do not fit one worker of {}",
        fleet.disk_per_instance
    );
    Ok(())
}

/// The scheduler configuration of the fixture varied by `variant`.
fn varied_nativelink(variant: Variant) -> gang_e2e::Result<NativeLinkConfig> {
    let doc = typed_rendered(&ConfigTemplate::parse(&base_config()?)?.vary(
        &variant,
        &launcher(),
        &tools(),
    )?)?;
    let SchedulerConfig::Nativelink(nativelink) = doc.scheduler;
    Ok(nativelink)
}

#[test]
fn vary_soft_memory_off_disables_the_nativelink_memory_enforcement() -> gang_e2e::Result<()> {
    let variant = Variant {
        soft_memory: SoftMemory::Off,
        ..Variant::baseline(Sandbox::Noop)
    };
    let nativelink = varied_nativelink(variant)?;
    assert_eq!(
        nativelink.memory.enforcement,
        gang_nativelink::MemoryEnforcement::None
    );
    Ok(())
}

#[test]
fn vary_soft_memory_enforced_uses_default_memory_tuning() -> gang_e2e::Result<()> {
    let nativelink = varied_nativelink(Variant::baseline(Sandbox::Noop))?;
    assert_eq!(nativelink.memory, gang_nativelink::MemoryTuning::default());
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
fn bwrap_binds_only_action_tool_roots_and_signal_dir() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let signal = std::path::Path::new("/tmp/e2e-signal");
    let template = template
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools())?
        .with_signal_dir(signal)?;
    let doc = typed_rendered(&template)?;
    let bwrap = doc.sandbox.bwrap.as_ref().expect("bwrap sandbox");
    assert_eq!(
        bwrap.program,
        PathBuf::from("/nix/store/ccc-bubblewrap/bin/bwrap")
    );
    let paths: Vec<_> = bwrap
        .readonly_bind_paths
        .iter()
        .map(AbsolutePathBuf::as_path)
        .collect();
    assert_eq!(paths, vec![std::path::Path::new("/nix/store"), signal]);
    Ok(())
}

#[test]
fn bwrap_binds_action_tools_at_standard_paths() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let tools = tools();
    let template = template.vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)?;
    let doc = typed_rendered(&template)?;
    let bwrap = doc.sandbox.bwrap.as_ref().expect("bwrap sandbox");
    let mounts: Vec<_> = bwrap
        .readonly_bind_mounts
        .iter()
        .map(|(destination, source)| (destination.as_path(), source.as_path()))
        .collect();
    assert_eq!(
        mounts,
        vec![
            (std::path::Path::new("/bin"), tools.action_bin.as_path()),
            (std::path::Path::new("/usr/bin"), tools.action_bin.as_path()),
        ]
    );
    Ok(())
}

/// Loader paths follow the action tool roots and precede the signal directory.
#[test]
fn bwrap_binds_the_host_glibc_loader_after_the_tool_roots() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let signal = std::path::Path::new("/tmp/e2e-signal");
    let mut tools = tools();
    tools.glibc_loader = vec![
        PathBuf::from("/lib64"),
        PathBuf::from("/run/current-system/sw"),
    ];
    let template = template
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)?
        .with_signal_dir(signal)?;
    let doc = typed_rendered(&template)?;
    let bwrap = doc.sandbox.bwrap.as_ref().expect("bwrap sandbox");
    let paths: Vec<_> = bwrap
        .readonly_bind_paths
        .iter()
        .map(AbsolutePathBuf::as_path)
        .collect();
    assert_eq!(
        paths,
        vec![
            std::path::Path::new("/nix/store"),
            std::path::Path::new("/lib64"),
            std::path::Path::new("/run/current-system/sw"),
            signal,
        ]
    );
    Ok(())
}

#[test]
fn bwrap_requires_a_program() -> gang_e2e::Result<()> {
    let (_, template) = placed(Transport::Plain)?;
    let mut tools = tools();
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
    let doc = typed_rendered(&template.vary(&variant, &launcher(), &tools())?)?;
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
fn bwrap_rejects_host_action_tools_but_noop_does_not() -> gang_e2e::Result<()> {
    for (tools, offending) in [
        (
            ActionTools {
                shell: PathBuf::from("/usr/bin/bash"),
                bwrap: Some(PathBuf::from("/nix/store/ccc-bubblewrap/bin/bwrap")),
                path: "/usr/bin:/bin".to_owned(),
                action_bin: PathBuf::from("/nix/store/ddd-action-tools/bin"),
                glibc_tool: None,
                glibc_loader: Vec::new(),
            },
            "/usr/bin/bash",
        ),
        (
            ActionTools {
                shell: PathBuf::from("/nix/store/aaa-bash/bin/bash"),
                bwrap: Some(PathBuf::from("/nix/store/ccc-bubblewrap/bin/bwrap")),
                path: "/usr/bin:/bin".to_owned(),
                action_bin: PathBuf::from("/nix/store/ddd-action-tools/bin"),
                glibc_tool: None,
                glibc_loader: Vec::new(),
            },
            "/usr/bin",
        ),
    ] {
        let text = base_config()?;
        let error = ConfigTemplate::parse(&text)?
            .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools)
            .expect_err("bwrap rejects host action tools");
        assert_eq!(
            error.to_string(),
            format!(
                "system under test: under bwrap the action tools must come from the Nix store, whose closures are self-contained: {offending}"
            )
        );
        ConfigTemplate::parse(&text)?.vary(
            &Variant::baseline(Sandbox::Noop),
            &launcher(),
            &tools,
        )?;
    }
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
        &tools(),
    )?)?;
    assert_eq!(doc.fleets[0].sandbox, SandboxKey::Bwrap);
    assert!(
        !doc.sandbox
            .bwrap
            .expect("bwrap sandbox")
            .permit_network_property
    );
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
        &tools(),
    )?)?;
    assert!(
        doc.sandbox
            .bwrap
            .expect("bwrap sandbox")
            .permit_network_property
    );
    assert_eq!(doc.execution.input_links, InputLinks::Keep);
    Ok(())
}

#[test]
fn vary_noop_selects_the_noop_sandbox() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let doc = typed_rendered(&template.vary(
        &Variant::baseline(Sandbox::Noop),
        &launcher(),
        &tools(),
    )?)?;
    assert_eq!(doc.fleets[0].sandbox, SandboxKey::Noop);
    assert_eq!(doc.execution.input_links, InputLinks::Break);
    assert!(doc.sandbox.noop.is_some());
    Ok(())
}

#[test]
fn vary_bwrap_without_its_section_fails_naming_it() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&without_sandbox(SandboxKey::Bwrap)?)?;
    let error = template
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools())
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
        .vary(&Variant::baseline(Sandbox::Noop), &launcher(), &tools())
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
        &tools(),
    )?)?;
    let Some(LimitsConfig::Cgroup(cgroup)) = doc.limits else {
        panic!("cgroup limits expected");
    };
    assert_eq!(cgroup.delegation, gang_cgroup::Delegation::Trusted);
    assert_eq!(
        u32::from(cgroup.memory_headroom_percent.get()),
        LIMIT_HEADROOM_PERCENT
    );
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
    let doc =
        typed_rendered(&base.vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools())?)?;
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
fn tmp_path_defaults_to_bwrap_default() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    assert_eq!(template.tmp_path()?, PathBuf::from("/tmp"));
    Ok(())
}

#[test]
fn tmp_path_is_the_configured_path() -> gang_e2e::Result<()> {
    let text = base_config()?.replace(
        "[sandbox.bwrap]",
        "[sandbox.bwrap]\ntmp_path = \"/var/tmp/gang\"",
    );
    assert_eq!(
        ConfigTemplate::parse(&text)?.tmp_path()?,
        PathBuf::from("/var/tmp/gang")
    );
    Ok(())
}

#[test]
fn tmp_path_fails_without_a_bwrap_section() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&without_sandbox(SandboxKey::Bwrap)?)?;
    let error = template
        .tmp_path()
        .expect_err("a configuration without [sandbox.bwrap] has no tmp path");
    assert!(error.to_string().contains("sandbox.bwrap"));
    Ok(())
}

#[test]
fn with_signal_dir_adds_it_to_the_read_only_binds() -> gang_e2e::Result<()> {
    let (_placement, template) = placed(Transport::Tls)?;
    let signal = std::path::Path::new("/scratch/signal-salt");
    let doc = typed_rendered(&template.with_signal_dir(signal)?)?;
    let bwrap = doc.sandbox.bwrap.expect("bwrap sandbox");
    assert!(
        bwrap
            .readonly_bind_paths
            .iter()
            .any(|path| path.as_path() == signal)
    );
    assert!(
        bwrap
            .readonly_bind_paths
            .iter()
            .any(|path| path.as_path() == std::path::Path::new("/nix/store"))
    );
    Ok(())
}

#[test]
fn with_signal_dir_fails_without_a_bwrap_section() -> gang_e2e::Result<()> {
    let text = without_sandbox(SandboxKey::Bwrap)?;
    let error = ConfigTemplate::parse(&text)?
        .with_signal_dir(std::path::Path::new("/scratch/signal-salt"))
        .expect_err("a configuration without [sandbox.bwrap] has no binds to extend");
    assert!(error.to_string().contains("sandbox.bwrap"));
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
        &tools(),
    )?)?;
    let ProvisionerConfig::Local(local) = doc.provisioner else {
        panic!("local provisioner expected");
    };
    assert_eq!(local.worker_launcher.argv(), launcher());
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
    let doc =
        typed_rendered(&base.vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools())?)?;
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
        &tools(),
    )?;
    Ok(typed_rendered(&template)?.fleets)
}

#[test]
fn single_layout_keeps_the_one_fleet_of_the_base() -> gang_e2e::Result<()> {
    let template = ConfigTemplate::parse(&base_config()?)?;
    let doc = typed_rendered(&template.vary(
        &Variant::baseline(Sandbox::Bwrap),
        &launcher(),
        &tools(),
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
    assert_eq!(small.vcpus_per_instance.get(), 4);
    assert_eq!(small.memory_per_instance, gang_types::NonZeroBytes::gib(8));
    assert_eq!(small.disk_per_instance, gang_types::NonZeroBytes::gib(20));
    assert_eq!(small.min_instances, 0);
    assert_eq!(small.max_instances.get(), 1);
    assert_eq!(small.target_actions_per_instance.get(), 1);
    Ok(())
}

#[test]
fn placement_large_fleet_doubles_the_size_under_the_same_sandbox() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    let large = &fleets[1];
    assert_eq!(large.sandbox, SandboxKey::Bwrap);
    assert_eq!(large.vcpus_per_instance.get(), 8);
    assert_eq!(large.memory_per_instance, gang_types::NonZeroBytes::gib(16));
    assert_eq!(large.disk_per_instance, gang_types::NonZeroBytes::gib(20));
    assert_eq!(large.min_instances, 0);
    assert_eq!(large.max_instances.get(), 2);
    assert_eq!(large.target_actions_per_instance.get(), 1);
    Ok(())
}

#[test]
fn placement_noop_fleet_has_the_small_size_under_the_noop_sandbox() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Bwrap)?;
    let noop = &fleets[2];
    assert_eq!(noop.sandbox, SandboxKey::Noop);
    assert_eq!(noop.vcpus_per_instance.get(), 4);
    assert_eq!(noop.memory_per_instance, gang_types::NonZeroBytes::gib(8));
    assert_eq!(noop.disk_per_instance, gang_types::NonZeroBytes::gib(20));
    assert_eq!(noop.max_instances.get(), 1);
    assert_eq!(noop.target_actions_per_instance.get(), 1);
    Ok(())
}

#[test]
fn placement_layout_puts_the_first_two_fleets_under_the_variants_sandbox() -> gang_e2e::Result<()> {
    let fleets = placement_fleets(Sandbox::Noop)?;
    for fleet in &fleets {
        assert_eq!(fleet.sandbox, SandboxKey::Noop);
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
    let template = base.vary(&placement_variant(Sandbox::Bwrap), &launcher(), &tools())?;
    let doc = typed_rendered(&template)?;
    assert_eq!(doc.default_isa, base_doc.default_isa);
    assert!(doc.sandbox.noop.is_some());
    Ok(())
}

#[test]
fn placement_layout_needs_the_noop_sandbox_section() -> gang_e2e::Result<()> {
    let text = without_sandbox(SandboxKey::Noop)?;
    let error = ConfigTemplate::parse(&text)?
        .vary(&placement_variant(Sandbox::Bwrap), &launcher(), &tools())
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
        .vary(&placement_variant(Sandbox::Bwrap), &launcher(), &tools())
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
        &tools(),
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

    fn try_for_each_program<E>(
        &mut self,
        _key: Self::Key,
        _visit: impl FnMut(&mut PathBuf) -> Result<(), E>,
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

/// The scheduler plugin section: an opaque table the tests never read.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
struct OpaqueScheduler(Table);

impl SchedulerSettings for OpaqueScheduler {}

impl PluginSet for OpaquePlugins {
    type Provisioner = Table;
    type Cache = Table;
    type Signer = Table;
    type Scheduler = OpaqueScheduler;
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
        let signal = std::path::Path::new("/scratch/signal-salt");
        let text = template.with_signal_dir(signal)?.render()?;
        if let Err(error) = text.parse::<GangConfig<OpaquePlugins>>() {
            panic!("the {label} config does not parse as a gang.toml: {error}");
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
                .vary(&variant, &worker_launcher, &tools())?;
            let template = if transport == Transport::Tls {
                template.with_identity(&issuer, variant.identity)?
            } else {
                template
            };
            let text = template.render()?;
            text.parse::<GangConfig<gang_main::plugins::Plugins>>()
                .map_err(|error| E2eError::Template(error.to_string()))?;
            Ok(())
        })();
        result.map_err(|error| {
            E2eError::Template(format!(
                "rendering or parsing {variant:?}, {preset}, over {transport:?}: {}",
                gang_types::error_chain(&error)
            ))
        })
    };

    for sandbox in [Sandbox::Noop, Sandbox::Bwrap] {
        for permit_network in [PermitNetwork::Denied, PermitNetwork::Permitted] {
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
