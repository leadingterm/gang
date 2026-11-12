//! Tests for the `gang-local` plugin ports: local provisioner, file signer, filesystem cache, scratch volumes, workload credentials, and fleet supply.

use std::path::Path;

use gang_local::{
    FileSigner, FileSignerConfig, FilesystemCache, FilesystemCacheConfig, Local, LocalConfig,
    LocalFleetOptions,
};
use gang_spi::{
    CertificateSignerPort, FleetSupply, NodeLifecyclePort, Plugin, PluginContext, PortError,
    ProvisionerPort, ScratchVolumePort, WorkloadCredentialPort,
};
use gang_types::{Bytes, Fleet, FleetName, InstanceId, Isa, Resources};
use p256::ecdsa::DerSignature;
use p256::ecdsa::VerifyingKey;
use p256::ecdsa::signature::Verifier;
use p256::pkcs8::DecodePublicKey;
use serde_json::json;

/// Returns a plugin context with a deterministic configuration path.
fn plugin_context() -> PluginContext<'static> {
    PluginContext {
        config_path: Path::new("gang.toml"),
    }
}

/// Builds the `[provisioner.local]` plugin with its state directory under `state_dir`.
fn build_local(state_dir: &Path) -> Result<Local, Box<dyn std::error::Error>> {
    let config: LocalConfig = serde_json::from_value(json!({
        "state_dir": state_dir,
        "mock_imds_listen": "127.0.0.1:9",
        "workload_token": { "audience": "gang", "ttl": "15m" },
    }))?;
    Ok(Local::build(config, &plugin_context())?)
}

/// Builds the `[cache.filesystem]` plugin with its cache root at `path`.
fn build_cache(path: &Path) -> Result<FilesystemCache, Box<dyn std::error::Error>> {
    let config: FilesystemCacheConfig = serde_json::from_value(json!({
        "path": path,
        "content_store_percent": 25,
        "action_store_percent": 2,
    }))?;
    Ok(FilesystemCache::build(config, &plugin_context())?)
}

/// Builds the `[signer.file]` plugin with its key file at `key_file`.
fn build_signer(key_file: &Path) -> Result<FileSigner, Box<dyn std::error::Error>> {
    let config: FileSignerConfig = serde_json::from_value(json!({
        "key_file": key_file,
    }))?;
    Ok(FileSigner::build(config, &plugin_context())?)
}

/// The fleet the supply test acts on.
fn fleet(name: &FleetName) -> Fleet<'_, LocalFleetOptions> {
    Fleet {
        name,
        isa: Isa::X86_64,
        instance: Resources {
            vcpus: 4,
            memory: Bytes::new(8 << 30),
            disk: Bytes::new(100 << 30),
        },
        options: &LocalFleetOptions {},
    }
}

#[tokio::test]
async fn signer_signs_and_the_public_key_verifies() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let signer = build_signer(&dir.path().join("keys").join("ca.key"))?;
    let message = b"certificate body";

    let public_key = signer.public_key().await?;
    let signature = signer.sign(message).await?;

    let key = VerifyingKey::from_public_key_der(&public_key)?;
    let signature = DerSignature::try_from(signature.as_slice())?;
    key.verify(message, &signature)?;
    assert!(
        key.verify(b"altered certificate body", &signature).is_err(),
        "an altered message must not verify"
    );
    Ok(())
}

#[tokio::test]
async fn signer_key_survives_a_rebuild() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let key_file = dir.path().join("keys").join("ca.key");
    let first = build_signer(&key_file)?.public_key().await?;
    let second = build_signer(&key_file)?.public_key().await?;

    assert_eq!(first, second);
    Ok(())
}

#[test]
fn building_the_signer_creates_no_key_file() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let keys = dir.path().join("keys");
    let key_file = keys.join("ca.key");

    build_signer(&key_file)?;

    assert!(
        !keys.exists(),
        "building the signer must not create its directory"
    );
    assert!(
        !key_file.exists(),
        "building the signer must not create its key file"
    );
    Ok(())
}

#[test]
fn building_the_cache_creates_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let cache_path = dir.path().join("cache");

    build_cache(&cache_path)?;

    assert!(
        !cache_path.exists(),
        "building the cache must not create its directory"
    );
    Ok(())
}

#[test]
fn building_the_local_plugin_creates_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let state_dir = dir.path().join("state");

    build_local(&state_dir)?;

    assert!(
        !state_dir.exists(),
        "building the local plugin must not create its state directory"
    );
    Ok(())
}

#[tokio::test]
async fn scratch_volume_is_prepared_once() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let local = build_local(&dir.path().join("state"))?;
    let instance = InstanceId::try_new(String::from("local-1"))?;

    let first = local.mount(&instance).await?;
    let second = local.mount(&instance).await?;

    assert_eq!(first.path, second.path);
    assert!(first.path.is_dir());
    assert!(first.path.starts_with(dir.path()));
    Ok(())
}

#[tokio::test]
async fn worker_ports_require_a_worker_environment() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let local = build_local(&dir.path().join("state"))?;

    assert!(matches!(local.identity().await, Err(PortError::Config(_))));
    assert!(matches!(
        local.workload_credential().await,
        Err(PortError::Config(_))
    ));
    Ok(())
}

#[tokio::test]
async fn supply_of_an_empty_fleet_is_empty() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let local = build_local(&dir.path().join("state"))?;
    let mut supply = FleetSupply::default();

    let name = FleetName::try_new("local-workers")?;
    local.supply(fleet(&name), &mut supply).await?;

    assert_eq!(supply.desired, 0);
    assert_eq!(supply.instances.len(), 0);
    Ok(())
}
