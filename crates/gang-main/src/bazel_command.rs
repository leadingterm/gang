//! The Bazel commands: `gang get`, the credential helper Bazel runs for a server, and `gang bazelrc`,
//! which prints the Bazel settings that point a build at a server.

use std::ffi::{OsStr, OsString};
use std::io::{self, IsTerminal, Read};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;

use gang_client::{BazelrcRequest, CA_FILE_NAME};

use crate::cli::BazelrcArgs;
use crate::client_command::client;
use crate::errors::{MainError, Result};
use crate::output::{Line, Output, Record};
use crate::record::COLUMN;
use crate::runtime;

/// The label of the wait line shown while a person has not yet given `gang get` a request.
const REQUEST_WAIT_LABEL: &str = "reading a credential request on stdin";

/// Answers the credential request read from standard input and prints the reply on standard output.
pub(crate) fn get(out: &Output) -> Result<ExitCode> {
    let request = read_request(out)?;
    let client = client(out)?;
    let reply = runtime::block_on(client.get(&request))?;
    present_reply(out, reply)?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the credential reply as data: Bazel reads it from stdout, so it is never styled.
fn present_reply(out: &Output, reply: impl Into<Line>) -> Result<()> {
    out.data(reply)
}

/// Reads the credential request from standard input. When a person is typing at a terminal, says
/// what the command is for and shows a wait line until the request has been read.
fn read_request(out: &Output) -> Result<String> {
    let mut stdin = io::stdin();
    let waiting = if stdin.is_terminal() {
        present_terminal_hint(out);
        Some(out.progress(REQUEST_WAIT_LABEL))
    } else {
        None
    };
    let mut request = String::new();
    stdin
        .read_to_string(&mut request)
        .map_err(MainError::Stdin)?;
    drop(waiting);
    Ok(request)
}

/// The rows `gang get` shows a person who ran it at a terminal: what the command is for, then a
/// blank row before the wait line.
fn terminal_hint() -> [Line; 3] {
    [
        Line::new()
            .pad(COLUMN)
            .muted("gang get is Bazel's credential helper: it reads Bazel's request on stdin."),
        Line::new()
            .pad(COLUMN)
            .muted("To check your credentials, run gang doctor."),
        Line::new(),
    ]
}

/// Explains `gang get` to a person who ran it at a terminal. A decoration: pipes never see it.
fn present_terminal_hint(out: &Output) {
    out.decoration(None, &terminal_hint());
}

/// Prints the Bazel settings of a server; writes the server's CA certificate next to them when the
/// server has one.
pub(crate) fn bazelrc(args: &BazelrcArgs, out: &Output) -> Result<ExitCode> {
    let client = client(out)?;
    let helper = credential_helper(args.helper.as_deref())?;
    let request = BazelrcRequest {
        server: &args.server,
        ca_pin: args.ca_pin.as_ref(),
        helper: &helper,
    };
    let settings = runtime::block_on(client.bazelrc(&request))?;
    let ca_path = match &settings.ca_certificate {
        Some(certificate) => {
            let path = ca_certificate_path(|key| std::env::var_os(key));
            write_ca_certificate(&path, certificate.as_str())?;
            Some(path)
        }
        None => None,
    };
    present_bazelrc(out, &settings.text, ca_path.as_deref())?;
    Ok(ExitCode::SUCCESS)
}

/// The credential helper `gang bazelrc` names: `explicit` when given, otherwise the running `gang`
/// by absolute path. Bazel looks a bare name up on its own `PATH`, which need not hold the
/// directory the user's shell found `gang` in.
pub(crate) fn credential_helper(explicit: Option<&str>) -> Result<String> {
    if let Some(helper) = explicit {
        return Ok(helper.to_owned());
    }
    let running = std::env::current_exe().map_err(MainError::CurrentExecutable)?;
    let argv0 = std::env::args_os().next().unwrap_or_default();
    let cwd = std::env::current_dir().ok();
    let search = std::env::var_os("PATH");
    let path = invoked_executable(
        Path::new(&argv0),
        cwd.as_deref(),
        search.as_deref(),
        &running,
    )
    .map_err(MainError::CurrentExecutable)?;
    path.into_os_string()
        .into_string()
        .map_err(|path| MainError::ExecutablePath(PathBuf::from(path)))
}

/// The absolute path the running executable `running` was invoked by: `argv0` against `cwd` when
/// it names a path, otherwise the first absolute `search` entry holding `argv0` that is `running`,
/// as a shell finds a command. Symlinks stay unresolved, so an upgrade that replaces the file or
/// retargets a link keeps the path valid. Without such a path, or when it holds `..` (which
/// Bazel refuses in a credential helper), the answer is `running` with every link resolved.
fn invoked_executable(
    argv0: &Path,
    cwd: Option<&Path>,
    search: Option<&OsStr>,
    running: &Path,
) -> io::Result<PathBuf> {
    let running = std::fs::canonicalize(running)?;
    let is_running = |file: &Path| std::fs::canonicalize(file).is_ok_and(|real| real == running);
    let invoked = if argv0.as_os_str().as_encoded_bytes().contains(&b'/') {
        cwd.map(|cwd| cwd.join(argv0))
    } else {
        search
            .into_iter()
            .flat_map(std::env::split_paths)
            .filter(|dir| dir.is_absolute())
            .map(|dir| dir.join(argv0))
            .find(|file| is_running(file))
    };
    Ok(invoked
        // Collecting the components drops every `.` after the root.
        .map(|file| file.components().collect::<PathBuf>())
        .filter(|file| {
            file.is_absolute()
                && !file.components().any(|part| part == Component::ParentDir)
                && is_running(file)
        })
        .unwrap_or(running))
}

/// Resolves the CA file relative to the directory where the command was invoked.
fn ca_certificate_path(env: impl Fn(&str) -> Option<OsString>) -> PathBuf {
    crate::workspace::absolute(PathBuf::from(CA_FILE_NAME), env)
}

/// Writes the CA certificate that the rc file refers to, replacing any earlier copy.
fn write_ca_certificate(path: &Path, pem: &str) -> Result<()> {
    gang_os::fs::write_atomic(path, pem.as_bytes(), gang_os::fs::PUBLIC_FILE)
        .map_err(MainError::CaCertificate)
}

/// Prints the rc text as data, one line per line of text, then says which file was written for
/// it. The outcome lands on stderr because the rc text was already written to stdout; when
/// stdout is a terminal too, a blank row keeps the two apart.
fn present_bazelrc(out: &Output, rc_text: &str, ca_path: Option<&Path>) -> Result<()> {
    for line in rc_text.lines() {
        out.data(line)?;
    }
    if let Some(path) = ca_path {
        if out.stdout_decorates() {
            out.decoration(None, &[Line::new()]);
        }
        out.result(&Record::ok(
            Line::new()
                .plain(format!("wrote {} ", path.display()))
                .muted("for --tls_certificate"),
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use crate::palette::Palette;

    use super::*;

    const RC_TEXT: &str = "# Written by `gang bazelrc`. Do not edit.\n\
                           build --remote_executor=grpcs://gang.example.com:443\n\
                           build --remote_cache=grpcs://gang.example.com:443\n\
                           build --remote_retries=12\n\
                           build --remote_retry_max_delay=5s\n\
                           build --grpc_keepalive_time=10s\n\
                           build --grpc_keepalive_timeout=30s\n\
                           build --credential_helper=gang.example.com=/usr/local/bin/gang\n";

    /// An installed link to the running test binary and its resolved path.
    struct Installed {
        /// Directory holding the executable link.
        bin: tempfile::TempDir,
        /// Path of the running test binary.
        running: PathBuf,
        /// Canonical path of the running test binary.
        real: PathBuf,
    }

    /// Installs a link to the running binary in a temporary directory.
    fn installed() -> Installed {
        let bin = tempfile::tempdir().expect("temporary bin");
        let running = std::env::current_exe().expect("test binary path");
        let real = std::fs::canonicalize(&running).expect("canonical test binary");
        symlink(&running, bin.path().join("gang")).expect("gang symlink");
        Installed { bin, running, real }
    }

    /// Creates another executable named gang that is not the running binary.
    fn other_gang() -> tempfile::TempDir {
        let other = tempfile::tempdir().expect("other directory");
        let path = other.path().join("gang");
        std::fs::write(&path, b"#!/bin/sh\n").expect("other gang");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("executable permissions");
        other
    }

    #[test]
    fn a_bare_name_is_the_path_entry_holding_it_with_links_unresolved() {
        let Installed { bin, running, real } = installed();
        let search =
            std::env::join_paths([Path::new("relative/dir"), bin.path()]).expect("search path");
        let found = invoked_executable(Path::new("gang"), None, Some(&search), &running)
            .expect("invoked executable");
        assert_eq!(found, bin.path().join("gang"));
        assert_eq!(
            std::fs::canonicalize(found).expect("resolved symlink"),
            real
        );
    }

    #[test]
    fn a_bare_name_skips_path_entries_holding_another_file() {
        let Installed { bin, running, real } = installed();
        let other = other_gang();
        let search = std::env::join_paths([other.path(), bin.path()]).expect("search path");
        let found = invoked_executable(Path::new("gang"), None, Some(&search), &running)
            .expect("invoked executable");
        assert_eq!(found, bin.path().join("gang"));
        assert_eq!(
            std::fs::canonicalize(found).expect("resolved symlink"),
            real
        );
    }

    #[test]
    fn a_bare_name_on_no_path_entry_is_the_running_file_resolved() {
        let Installed {
            bin: _bin,
            running,
            real,
        } = installed();
        let other = tempfile::tempdir().expect("other directory");
        let search = std::env::join_paths([other.path()]).expect("search path");
        assert_eq!(
            invoked_executable(Path::new("gang"), None, Some(&search), &running)
                .expect("invoked executable"),
            real
        );
        assert_eq!(
            invoked_executable(Path::new("gang"), None, None, &running)
                .expect("invoked executable"),
            real
        );
    }

    #[test]
    fn a_relative_path_is_absolute_against_the_working_directory() {
        let Installed { bin, running, real } = installed();
        let found = invoked_executable(Path::new("./gang"), Some(bin.path()), None, &running)
            .expect("invoked executable");
        assert_eq!(found, bin.path().join("gang"));
        assert_eq!(
            std::fs::canonicalize(found).expect("resolved symlink"),
            real
        );
    }

    #[test]
    fn a_path_with_parent_components_is_the_running_file_resolved() {
        let Installed { bin, running, real } = installed();
        let sub = bin.path().join("sub");
        std::fs::create_dir(&sub).expect("subdirectory");
        assert_eq!(
            invoked_executable(Path::new("../gang"), Some(&sub), None, &running)
                .expect("invoked executable"),
            real
        );
    }

    #[test]
    fn a_path_naming_another_file_is_the_running_file_resolved() {
        let Installed { bin, running, real } = installed();
        let other = other_gang();
        let other_gang = other.path().join("gang");
        assert_eq!(
            invoked_executable(&other_gang, Some(bin.path()), None, &running)
                .expect("invoked executable"),
            real
        );
    }

    #[test]
    fn an_explicit_helper_is_used_verbatim() {
        assert_eq!(
            credential_helper(Some("gang")).expect("explicit helper"),
            "gang"
        );
    }

    #[test]
    fn ca_certificate_path_uses_bazels_working_directory() {
        let path = ca_certificate_path(|key| {
            (key == crate::workspace::WORKING_DIRECTORY_ENV).then(|| OsString::from("/workspace"))
        });
        assert_eq!(path, PathBuf::from("/workspace/gang-ca.pem"));
    }

    #[test]
    fn bazelrc_prints_the_rc_text_verbatim_on_stdout() {
        let (out, stdout, stderr) = Output::captured();
        present_bazelrc(&out, RC_TEXT, None).expect("rc text is printed");
        assert_eq!(stdout.text(), RC_TEXT);
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn bazelrc_reports_the_ca_file_on_stderr_once_data_was_written() {
        let (out, stdout, stderr) = Output::captured();
        present_bazelrc(&out, RC_TEXT, Some(Path::new("/workspace/gang-ca.pem")))
            .expect("rc text is printed");
        assert_eq!(stdout.text(), RC_TEXT);
        assert_eq!(
            stderr.text(),
            "    ok  wrote /workspace/gang-ca.pem for --tls_certificate\n"
        );
    }

    #[test]
    fn bazelrc_separates_the_ca_file_report_on_a_terminal() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        present_bazelrc(&out, RC_TEXT, Some(Path::new("/workspace/gang-ca.pem")))
            .expect("rc text is printed");
        let text = stderr.text();
        assert!(text.starts_with('\n'), "{text:?}");
        assert_eq!(text.matches('\n').count(), 2, "{text:?}");
        assert!(text.contains("wrote /workspace/gang-ca.pem"), "{text:?}");
    }

    #[test]
    fn bazelrc_keeps_blank_lines_of_the_rc_text() {
        let (out, stdout, _) = Output::captured();
        present_bazelrc(&out, "build --a\n\nbuild --b\n", None).expect("rc text is printed");
        assert_eq!(stdout.text(), "build --a\n\nbuild --b\n");
    }

    #[test]
    fn get_prints_the_reply_alone_on_stdout() {
        let (out, stdout, stderr) = Output::captured();
        present_reply(&out, r#"{"headers":{}}"#).expect("reply is printed");
        assert_eq!(stdout.text(), "{\"headers\":{}}\n");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn get_hint_is_absent_from_piped_output() {
        let (out, stdout, stderr) = Output::captured();
        present_terminal_hint(&out);
        assert_eq!(stdout.text(), "");
        assert_eq!(stderr.text(), "");
    }
}
