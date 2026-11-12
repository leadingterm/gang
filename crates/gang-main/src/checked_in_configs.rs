//! Every checked-in `configs/*.toml` loads through the plugin set the `gang` binary composes, and
//! the plaintext and TLS files keep the transport their names promise. Each one also round-trips
//! through its rendered document, which is how `gang local up` writes the copy the server reads.
//! The test lives in the crate because the composed plugin set is crate-private. The files reach
//! it through the `test_env` that `crates/gang-main/BUILD.bazel` sets.

use std::error::Error;
use std::path::{Path, PathBuf};

use gang_config::{ClientSecretSource, Document, Transport};
use gang_types::ClientSecret;

use crate::config_file::LoadedConfig;
use crate::plugins::{IdentityProviderConfig, Plugins};

/// The runfiles variable that lists the checked-in config files, space separated.
const CONFIGS_VAR: &str = "GANG_CHECKED_IN_CONFIGS";

/// The one checked-in file that must run over plaintext.
const PLAINTEXT_CONFIG: &str = "gang.local.plaintext.toml";

/// The one checked-in file that must run with TLS terminated by Gang.
const TLS_CONFIG: &str = "gang.local.toml";

/// Every checked-in config file, sorted.
fn checked_in_configs() -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let listed = std::env::var(CONFIGS_VAR)
        .map_err(|_| format!("{CONFIGS_VAR} must be set by the Bazel test_env"))?;
    let root = std::env::current_dir()?;
    let mut paths: Vec<PathBuf> = listed
        .split_whitespace()
        .map(|raw| root.join(raw))
        .collect();
    paths.sort();
    Ok(paths)
}

/// The file name of `path`, lossily converted; empty when it has none.
fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn secrets(document: &Document<Plugins>) -> Vec<(String, &ClientSecret)> {
    let mut secrets = Vec::new();
    for (name, provider) in &document.identity.providers {
        match provider {
            IdentityProviderConfig::Oidc(oidc) => {
                if let Some(secret) = oidc
                    .login
                    .as_ref()
                    .and_then(|login| login.client_secret.as_ref())
                {
                    secrets.push((
                        format!("identity.providers.{name}.oidc.login.client_secret"),
                        secret,
                    ));
                }
            }
        }
    }
    if let Some(login) = &document.dashboard.login
        && let ClientSecretSource::Inline(secret) = &login.client_secret
    {
        secrets.push(("dashboard.login.client_secret".to_owned(), secret));
    }
    secrets
}

fn secret_mismatches(original: &Document<Plugins>, reparsed: &Document<Plugins>) -> Vec<String> {
    let original = secrets(original);
    let reparsed = secrets(reparsed);
    let mut mismatches = Vec::new();
    for (path, secret) in &original {
        match reparsed.iter().find(|(other_path, _)| other_path == path) {
            Some((_, other)) if secret.matches(other.expose()) => {}
            _ => mismatches.push(path.clone()),
        }
    }
    for (path, _) in &reparsed {
        if !original.iter().any(|(other_path, _)| other_path == path) {
            mismatches.push(path.clone());
        }
    }
    mismatches
}

#[test]
fn checked_in_configs_are_present() -> Result<(), Box<dyn Error>> {
    let names: Vec<String> = checked_in_configs()?
        .iter()
        .map(|path| file_name(path))
        .collect();
    assert!(
        names.iter().any(|name| name == TLS_CONFIG)
            && names.iter().any(|name| name == PLAINTEXT_CONFIG),
        "the checked-in configs are missing: {names:?}"
    );
    Ok(())
}

#[test]
fn checked_in_configs_load_through_the_composed_plugins() -> Result<(), Box<dyn Error>> {
    let mut failures = Vec::new();
    for path in checked_in_configs()? {
        let loaded = match LoadedConfig::load(&path) {
            Ok(loaded) => loaded,
            Err(error) => {
                failures.push(format!("{}: {error}", path.display()));
                continue;
            }
        };
        let is_tls = matches!(loaded.config.server.transport, Transport::Tls { .. });
        let name = file_name(&path);
        if name == PLAINTEXT_CONFIG && is_tls {
            failures.push(format!(
                "{}: expected a plaintext transport",
                path.display()
            ));
        }
        if name == TLS_CONFIG && !is_tls {
            failures.push(format!("{}: expected a tls transport", path.display()));
        }
    }
    assert!(
        failures.is_empty(),
        "checked-in configs that do not load:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn checked_in_configs_round_trip_through_the_document() -> Result<(), Box<dyn Error>> {
    for path in checked_in_configs()? {
        let text = std::fs::read_to_string(&path)?;
        let document = Document::<Plugins>::parse(&text)?;
        let rendered = document.to_toml()?;
        let reparsed = Document::<Plugins>::parse(&rendered)?;
        let mismatches = secret_mismatches(&document, &reparsed);
        assert!(mismatches.is_empty(), "{}: {mismatches:?}", path.display());
        assert_eq!(reparsed.to_toml()?, rendered, "{}", path.display());
    }
    Ok(())
}

#[test]
fn a_dropped_or_corrupted_secret_fails_the_round_trip() -> Result<(), Box<dyn Error>> {
    let path = checked_in_configs()?
        .into_iter()
        .find(|path| file_name(path) == "gang.aws.toml")
        .expect("gang.aws.toml is among the checked-in configs");
    let text = std::fs::read_to_string(path)?;
    let document = Document::<Plugins>::parse(&text)?;
    let secrets = secrets(&document);
    assert!(!secrets.is_empty(), "gang.aws.toml must contain a secret");
    let (secret_path, secret) = &secrets[0];
    let rendered = document.to_toml()?;
    let value = secret.expose();
    let replacement = if value.chars().all(|character| character == 'x') {
        "y".repeat(value.len())
    } else {
        "x".repeat(value.len())
    };
    let corrupted = rendered.replace(value, &replacement);
    let reparsed = Document::<Plugins>::parse(&corrupted)?;
    assert!(secret_mismatches(&document, &reparsed).contains(secret_path));

    let line = rendered
        .lines()
        .find(|line| line.trim_start().starts_with("client_secret =") && line.contains(value))
        .expect("the rendered document contains the secret line");
    let dropped = rendered.replace(line, "");
    let reparsed = Document::<Plugins>::parse(&dropped)?;
    assert!(secret_mismatches(&document, &reparsed).contains(secret_path));
    Ok(())
}
