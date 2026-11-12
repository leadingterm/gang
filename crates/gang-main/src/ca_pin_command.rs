//! `gang ca-pin`: prints the CA pin of the configured signer, for the operators and CI jobs that
//! distribute it to clients.

use std::path::Path;
use std::process::ExitCode;

use gang_spi::PluginContext;

use crate::config_file::LoadedConfig;
use crate::errors::Result;
use crate::output::Output;
use crate::runtime;
use crate::signer_pin::configured_ca_pin;

/// Prints the CA pin of the `[signer]` key of the configuration at `config`, as `sha256:<hex>`.
pub(crate) fn run(config: &Path, out: &Output) -> Result<ExitCode> {
    let LoadedConfig { path, config, .. } = LoadedConfig::load(config)?;
    let context = PluginContext { config_path: &path };
    let pin = runtime::block_on(configured_ca_pin(config.signer, &context))?;
    out.data(pin.to_string())?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use gang_types::CaPin;

    use super::*;
    use crate::config_file::tests::VALID_LOCAL_CONFIG;

    /// Writes a configuration whose signer keeps its key under `dir`, and returns its path.
    fn configuration(dir: &Path) -> std::path::PathBuf {
        let path = dir.join("gang.toml");
        let key_file = dir.join("signer").join("ca.key");
        let text =
            VALID_LOCAL_CONFIG.replace("/tmp/gang-local/ca.key", &key_file.display().to_string());
        std::fs::write(&path, text).expect("write configuration");
        path
    }

    /// The pin `run` prints for the configuration at `path`.
    fn printed_pin(path: &Path) -> CaPin {
        let (out, stdout, _) = Output::captured();
        run(path, &out).expect("ca-pin runs");
        stdout
            .text()
            .trim_end()
            .parse()
            .expect("ca-pin prints a pin")
    }

    #[test]
    fn prints_the_pin_of_the_configured_signers_key() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = configuration(dir.path());

        let first = printed_pin(&path);
        assert_eq!(first, printed_pin(&path));

        let other_dir = tempfile::tempdir().expect("temporary directory");
        assert_ne!(first, printed_pin(&configuration(other_dir.path())));
    }
}
