//! Code generator for `gang-reapi` checked-in protobuf and gRPC bindings.
//!
//! Compiles the vendored `REAPI v2` / Google protos and `leadingterm.gang.worker.v1` using
//! `protox` (no external `protoc` binary required) and `tonic-prost-build`, optionally formats
//! the generated files with `rustfmt`, and either writes them to an output directory (`--out`)
//! or checks an existing directory for byte-for-byte drift (`--check`).

use std::collections::BTreeSet;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::atomic::{AtomicU64, Ordering};

const INPUT_PROTOS: &[&str] = &[
    "build/bazel/remote/execution/v2/remote_execution.proto",
    "build/bazel/semver/semver.proto",
    "google/bytestream/bytestream.proto",
    "google/longrunning/operations.proto",
    "google/rpc/code.proto",
    "google/rpc/status.proto",
    "leadingterm/gang/worker/v1/worker.proto",
];

const DEFAULT_REMOTE_APIS_DIR: &str = "third_party/remote-apis";
const DEFAULT_PROTO_DIR: &str = "proto";
const USAGE_EXIT: u8 = 2;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(
        "usage: gang-protogen <--out DIR | --check DIR> [--remote-apis DIR] [--proto DIR] [--rustfmt PATH]"
    )]
    Usage,
    #[error("protobuf compilation failed: {0}")]
    Protox(#[from] protox::Error),
    #[error("I/O error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("tonic-prost codegen failed: {0}")]
    Codegen(io::Error),
    #[error("rustfmt invocation failed: {0}")]
    RustfmtSpawn(io::Error),
    #[error("rustfmt exited with status {0}")]
    RustfmtStatus(std::process::ExitStatus),
    #[error("generated sources drifted from checked-in directory {dir}:\n{details}")]
    Drift { dir: PathBuf, details: String },
}

#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Generate { out_dir: PathBuf },
    Check { dir: PathBuf },
}

#[derive(Debug, PartialEq, Eq)]
struct Args {
    mode: Mode,
    remote_apis_dir: PathBuf,
    proto_dir: PathBuf,
    rustfmt: Option<PathBuf>,
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Result<Self, Error> {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("gang-protogen-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn main() -> ExitCode {
    match run(env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gang-protogen: {error}");
            match error {
                Error::Usage => ExitCode::from(USAGE_EXIT),
                _ => ExitCode::FAILURE,
            }
        }
    }
}

fn run(args: impl IntoIterator<Item = OsString>) -> Result<(), Error> {
    let parsed = parse_args(args)?;
    match parsed.mode {
        Mode::Generate { out_dir } => generate_to_dir(
            &parsed.remote_apis_dir,
            &parsed.proto_dir,
            &out_dir,
            parsed.rustfmt.as_deref(),
        ),
        Mode::Check { dir } => check_dir(
            &parsed.remote_apis_dir,
            &parsed.proto_dir,
            &dir,
            parsed.rustfmt.as_deref(),
        ),
    }
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Args, Error> {
    let mut mode = None;
    let mut remote_apis_dir = env::var_os("GANG_PROTOGEN_REMOTE_APIS")
        .map_or_else(|| PathBuf::from(DEFAULT_REMOTE_APIS_DIR), PathBuf::from);
    let mut proto_dir = env::var_os("GANG_PROTOGEN_PROTO")
        .map_or_else(|| PathBuf::from(DEFAULT_PROTO_DIR), PathBuf::from);
    let mut rustfmt = env::var_os("RUSTFMT").map(PathBuf::from);

    let mut iter = args.into_iter();
    while let Some(flag) = iter.next() {
        let flag_str = flag.to_str().ok_or(Error::Usage)?;
        let value = iter.next().ok_or(Error::Usage)?;
        match flag_str {
            "--out" => {
                if mode.is_some() {
                    return Err(Error::Usage);
                }
                mode = Some(Mode::Generate {
                    out_dir: PathBuf::from(value),
                });
            }
            "--check" => {
                if mode.is_some() {
                    return Err(Error::Usage);
                }
                mode = Some(Mode::Check {
                    dir: PathBuf::from(value),
                });
            }
            "--remote-apis" => remote_apis_dir = PathBuf::from(value),
            "--proto" => proto_dir = PathBuf::from(value),
            "--rustfmt" => rustfmt = Some(PathBuf::from(value)),
            _ => return Err(Error::Usage),
        }
    }

    let mode = mode.ok_or(Error::Usage)?;
    Ok(Args {
        mode,
        remote_apis_dir,
        proto_dir,
        rustfmt,
    })
}

fn generate_to_dir(
    remote_apis_dir: &Path,
    proto_dir: &Path,
    out_dir: &Path,
    rustfmt: Option<&Path>,
) -> Result<(), Error> {
    let staging = TempDir::new()?;
    let fds = protox::compile(INPUT_PROTOS, [remote_apis_dir, proto_dir])?;
    tonic_prost_build::configure()
        .out_dir(staging.path())
        .build_transport(false)
        .bytes(".")
        .compile_fds(fds)
        .map_err(Error::Codegen)?;

    let generated_files = collect_rs_files(staging.path())?;
    if let Some(rustfmt_bin) = rustfmt
        && !generated_files.is_empty()
    {
        let mut cmd = Command::new(rustfmt_bin);
        cmd.arg("--edition").arg("2024");
        for file_name in &generated_files {
            cmd.arg(staging.path().join(file_name));
        }
        let status = cmd.status().map_err(Error::RustfmtSpawn)?;
        if !status.success() {
            return Err(Error::RustfmtStatus(status));
        }
    }

    fs::create_dir_all(out_dir).map_err(|source| Error::Io {
        path: out_dir.to_owned(),
        source,
    })?;
    for file_name in &generated_files {
        let src = staging.path().join(file_name);
        let dst = out_dir.join(file_name);
        fs::copy(&src, &dst).map_err(|source| Error::Io { path: dst, source })?;
    }
    Ok(())
}

fn check_dir(
    remote_apis_dir: &Path,
    proto_dir: &Path,
    checked_in_dir: &Path,
    rustfmt: Option<&Path>,
) -> Result<(), Error> {
    let generated_dir = TempDir::new()?;
    generate_to_dir(remote_apis_dir, proto_dir, generated_dir.path(), rustfmt)?;

    let expected_files = collect_rs_files(generated_dir.path())?;
    let actual_files = collect_generated_rs_files(checked_in_dir)?;

    let mut problems = Vec::new();
    for missing in expected_files.difference(&actual_files) {
        problems.push(format!("  missing generated file: {missing}"));
    }
    for extra in actual_files.difference(&expected_files) {
        problems.push(format!("  unexpected generated file: {extra}"));
    }
    for file_name in expected_files.intersection(&actual_files) {
        let expected_path = generated_dir.path().join(file_name);
        let actual_path = checked_in_dir.join(file_name);
        let expected_bytes = fs::read(&expected_path).map_err(|source| Error::Io {
            path: expected_path,
            source,
        })?;
        let actual_bytes = fs::read(&actual_path).map_err(|source| Error::Io {
            path: actual_path,
            source,
        })?;
        if expected_bytes != actual_bytes {
            problems.push(format!(
                "  content differs: {file_name} (generated {} bytes vs checked-in {} bytes)",
                expected_bytes.len(),
                actual_bytes.len()
            ));
        }
    }

    if !problems.is_empty() {
        return Err(Error::Drift {
            dir: checked_in_dir.to_owned(),
            details: problems.join("\n"),
        });
    }
    Ok(())
}

fn collect_rs_files(dir: &Path) -> Result<BTreeSet<String>, Error> {
    let mut files = BTreeSet::new();
    let entries = fs::read_dir(dir).map_err(|source| Error::Io {
        path: dir.to_owned(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: dir.to_owned(),
            source,
        })?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "rs")
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            files.insert(name.to_owned());
        }
    }
    Ok(files)
}

/// Lists checked-in prost-generated files in `crates/gang-reapi/src`.
///
/// Generated package files always include dotted protobuf package names (e.g.
/// `google.rpc.rs`), whereas handwritten modules (`lib.rs`, `digest.rs`, `build.rs`,
/// `google.rs`, `leadingterm.rs`) have single-word stems without dots.
fn collect_generated_rs_files(dir: &Path) -> Result<BTreeSet<String>, Error> {
    Ok(collect_rs_files(dir)?
        .into_iter()
        .filter(|name| {
            name.strip_suffix(".rs")
                .is_some_and(|stem| stem.contains('.'))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_generate_and_check_modes() {
        let out = parse_args(args(&[
            "--out",
            "/tmp/out",
            "--remote-apis",
            "ra",
            "--proto",
            "pr",
            "--rustfmt",
            "rf",
        ]))
        .expect("parse --out");
        assert_eq!(
            out,
            Args {
                mode: Mode::Generate {
                    out_dir: PathBuf::from("/tmp/out"),
                },
                remote_apis_dir: PathBuf::from("ra"),
                proto_dir: PathBuf::from("pr"),
                rustfmt: Some(PathBuf::from("rf")),
            }
        );

        let check = parse_args(args(&[
            "--check",
            "/tmp/in",
            "--remote-apis",
            "ra",
            "--proto",
            "pr",
            "--rustfmt",
            "rf",
        ]))
        .expect("parse --check");
        assert_eq!(
            check,
            Args {
                mode: Mode::Check {
                    dir: PathBuf::from("/tmp/in"),
                },
                remote_apis_dir: PathBuf::from("ra"),
                proto_dir: PathBuf::from("pr"),
                rustfmt: Some(PathBuf::from("rf")),
            }
        );
    }

    #[test]
    fn rejects_invalid_arguments() {
        for bad in [
            &[][..],
            &["--out"],
            &["--out", "a", "--check", "b"],
            &["--unknown", "x"],
        ] {
            assert!(
                matches!(parse_args(args(bad)), Err(Error::Usage)),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn generated_reapi_sources_have_no_drift() {
        let remote_apis = env::var_os("GANG_PROTOGEN_REMOTE_APIS")
            .map_or_else(|| PathBuf::from(DEFAULT_REMOTE_APIS_DIR), PathBuf::from);
        let proto = env::var_os("GANG_PROTOGEN_PROTO")
            .map_or_else(|| PathBuf::from(DEFAULT_PROTO_DIR), PathBuf::from);
        let reapi_dir = env::var_os("GANG_PROTOGEN_REAPI_DIR")
            .map_or_else(|| PathBuf::from("crates/gang-reapi/src"), PathBuf::from);
        let rustfmt = env::var_os("RUSTFMT").map(PathBuf::from);

        check_dir(&remote_apis, &proto, &reapi_dir, rustfmt.as_deref())
            .expect("gang-reapi generated sources must match gang-protogen output");
    }
}
