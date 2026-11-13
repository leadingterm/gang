//! Staging of the sample Bazel workspace each end-to-end run builds.
//!
//! The sample is copied from the repository into a run's own workspace, and a generated
//! `config.bzl` next to it tells the sample the run's parameters: a salt that keeps its action
//! keys unique to the run, the instruction set to target, the limits of the limit probes, and
//! the fleet, memory, and sandbox the placement probes request.

use std::fs;
use std::path::{Path, PathBuf};

use gang_types::Isa;

use crate::errors::{E2eError, Result};
use crate::system::{Probe, Sandbox};

/// The file name the generated parameters are written to, inside the staged workspace.
const CONFIG_FILE: &str = "config.bzl";

/// The file name the glibc tool is staged under, the input of the sample's `glibc_tool` probe.
const GLIBC_TOOL_FILE: &str = "glibc-tool";

/// The number of random bytes a salt is made of. Each byte becomes two hex digits.
const SALT_BYTES: usize = 8;

/// The parameters one run passes to its sample workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleParameters {
    /// Makes the sample's action keys unique to this run.
    pub salt: String,
    /// The PATH of every action.
    pub action_path: String,
    /// The signal pipe actions see as `E2E_SIGNAL`.
    pub signal: PathBuf,
    /// The attempt counter actions see as `E2E_ATTEMPTS`.
    pub attempts: PathBuf,
    /// The instruction set the sample's actions target.
    pub isa: Isa,
    /// The memory, in kibibytes, that the sample's limit probes declare as their `memory_kb`.
    pub memory_kb: u64,
    /// The number of CPUs that the sample's limit probes declare as their `cpu_count`.
    pub cpu_count: u32,
    /// The fleet that the sample's `fleet_requested` probe pins with the `fleet` property.
    pub large_fleet: String,
    /// The memory, in kibibytes, that the sample's `memory_requested` probe declares as its
    /// `memory_kb`.
    pub large_memory_kb: u64,
    /// The sandbox that the sample's `sandbox_requested` probe declares as its `sandbox`.
    pub requested_sandbox: Sandbox,
    /// The dynamically linked glibc program staged as [`GLIBC_TOOL_FILE`]; `None` where the platform has no bwrap.
    pub glibc_tool: Option<PathBuf>,
}

/// Copies the sample workspace at `source` to `destination` and writes its parameters.
///
/// `destination` is created and must not exist. Directories and regular files are copied
/// with their permissions. Any other entry (a symbolic link, a socket, a device) is an error,
/// because it could make the staged copy differ from the source in ways the run cannot see.
/// After the copy, `BUILD.bazel` is checked for every probe target, and `config.bzl` in
/// `destination` is overwritten with the parameters.
/// The glibc tool, when set, is copied in as `glibc-tool` with its permissions.
///
/// # Errors
///
/// Returns [`E2eError::Io`] when `destination` already exists or a file cannot be read or
/// written, and [`E2eError::System`] when `source` contains an entry that is neither a
/// directory nor a regular file, or the staged build file lacks a probe target.
pub fn stage(source: &Path, destination: &Path, parameters: &SampleParameters) -> Result<()> {
    fs::create_dir(destination).map_err(E2eError::io(format!(
        "creating the staged sample {}",
        destination.display()
    )))?;
    copy_tree(source, destination)?;
    if let Some(tool) = &parameters.glibc_tool {
        let staged = destination.join(GLIBC_TOOL_FILE);
        fs::copy(tool, &staged).map_err(E2eError::io(format!(
            "copying {} to {}",
            tool.display(),
            staged.display()
        )))?;
    }
    let build_path = destination.join("BUILD.bazel");
    let build_file = fs::read_to_string(&build_path)
        .map_err(E2eError::io(format!("reading {}", build_path.display())))?;
    if let Some(probe) = missing_probe(&build_file) {
        return Err(E2eError::system(format!(
            "the sample workspace has no target for the probe `{}`",
            probe.as_str()
        )));
    }
    let config = format!(
        "\"\"\"Parameters of one end-to-end run, written by the harness.\"\"\"\n\
         \n\
         SALT = \"{}\"\n\
         ACTION_PATH = \"{}\"\n\
         SIGNAL = \"{}\"\n\
         ATTEMPTS = \"{}\"\n\
         ISA = \"{}\"\n\
         MEMORY_KB = {}\n\
         CPU_COUNT = {}\n\
         LARGE_FLEET = \"{}\"\n\
         LARGE_MEMORY_KB = {}\n\
         REQUESTED_SANDBOX = \"{}\"\n\
         SIGNAL_COMMAND = \"{}\"\n",
        escape(&parameters.salt),
        escape(&parameters.action_path),
        escape(&parameters.signal.to_string_lossy()),
        escape(&parameters.attempts.to_string_lossy()),
        parameters.isa.as_str(),
        parameters.memory_kb,
        parameters.cpu_count,
        escape(&parameters.large_fleet),
        parameters.large_memory_kb,
        parameters.requested_sandbox.key(),
        escape(crate::hold::SIGNAL_COMMAND),
    );
    let config_path = destination.join(CONFIG_FILE);
    fs::write(&config_path, config)
        .map_err(E2eError::io(format!("writing {}", config_path.display())))
}

/// Returns the first probe whose name attribute is absent from the build file.
fn missing_probe(build_file: &str) -> Option<Probe> {
    Probe::ALL.into_iter().find(|probe| {
        !build_file.match_indices("name").any(|(index, _)| {
            let before = &build_file[..index];
            if before
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return false;
            }
            let after = &build_file[index + "name".len()..];
            after
                .trim_start_matches(' ')
                .strip_prefix('=')
                .map(|value| value.trim_start_matches(' '))
                .and_then(|value| value.strip_prefix('"'))
                .and_then(|value| value.strip_prefix(probe.as_str()))
                .is_some_and(|value| value.starts_with('"'))
        })
    })
}

/// Returns 16 lowercase hex characters drawn from the operating system's randomness.
///
/// # Errors
///
/// Returns [`E2eError::System`] when the operating system cannot provide randomness.
pub fn salt() -> Result<String> {
    let mut bytes = [0_u8; SALT_BYTES];
    getrandom::fill(&mut bytes)
        .map_err(E2eError::system_from("reading operating system randomness"))?;
    Ok(hex::encode(bytes))
}

/// Recursively copies the contents of directory `source` into the existing directory
/// `destination`.
fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    let entries =
        fs::read_dir(source).map_err(E2eError::io(format!("reading {}", source.display())))?;
    for entry in entries {
        let entry = entry.map_err(E2eError::io(format!("reading {}", source.display())))?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(E2eError::io(format!("inspecting {}", from.display())))?;
        if file_type.is_dir() {
            fs::create_dir(&to).map_err(E2eError::io(format!("creating {}", to.display())))?;
            copy_tree(&from, &to)?;
        } else if file_type.is_file() {
            fs::copy(&from, &to).map_err(E2eError::io(format!(
                "copying {} to {}",
                from.display(),
                to.display()
            )))?;
        } else {
            return Err(E2eError::system(format!(
                "cannot stage {}: it is neither a directory nor a regular file",
                from.display()
            )));
        }
    }
    Ok(())
}

/// Escapes a value for a double-quoted Starlark string.
fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if control.is_control() => {
                let mut bytes = [0; 4];
                for byte in control.encode_utf8(&mut bytes).bytes() {
                    escaped.push_str("\\x");
                    for digit in [byte >> 4, byte & 0x0f] {
                        escaped.push(char::from(if digit < 10 {
                            b'0' + digit
                        } else {
                            b'a' + digit - 10
                        }));
                    }
                }
            }
            other => escaped.push(other),
        }
    }
    escaped
}
