//! The Bazel commands: `gang get`, the credential helper Bazel runs for a server, and `gang bazelrc`,
//! which prints the Bazel settings that point a build at a server.

use std::ffi::OsString;
use std::io::{self, IsTerminal, Read};
use std::path::{Path, PathBuf};
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
    let request = BazelrcRequest {
        server: &args.server,
        ca_pin: args.ca_pin.as_ref(),
        helper: &args.helper,
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

/// Resolves the CA file relative to the directory where the command was invoked.
fn ca_certificate_path(env: impl Fn(&str) -> Option<OsString>) -> PathBuf {
    crate::workspace::absolute(PathBuf::from(CA_FILE_NAME), env)
}

/// Writes the CA certificate that the rc file refers to, replacing any earlier copy.
fn write_ca_certificate(path: &Path, pem: &str) -> Result<()> {
    gang_os::fs::write_atomic(path, pem.as_bytes(), 0o644).map_err(MainError::CaCertificate)
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
    use crate::palette::Palette;

    use super::*;

    const RC_TEXT: &str = "# Written by `gang bazelrc`. Do not edit.\n\
                           build --remote_executor=grpcs://gang.example.com:443\n\
                           build --credential_helper=gang.example.com=gang\n";

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
