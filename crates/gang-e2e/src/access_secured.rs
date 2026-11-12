//! The identity scenario's view of a real [`Secured`] deployment, and the proof command that
//! `gang login --token-command` re-executes.

use std::ffi::OsString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use gang_clock::{Clock, SystemClock, Timestamp};
use gang_types::{CaPin, CertificatePem, PrivateKeyPem, Url};
use rustls::ClientConfig;
use x509_cert::Certificate;
use x509_cert::der::DecodePem;

use crate::Ran;
use crate::access::{
    Access, Administration, Aging, Login, MisdirectedLogin, Misdirection, TokenAnswer, Validity,
    WorkerIdentity,
};
use crate::errors::{E2eError, Result};
use crate::flags::helper_path;
use crate::h2_frames::H2Channel;
use crate::identity::{Admission, Credentials, Identity, Issuance};
use crate::issuer::TokenSpec;
use crate::process::{self, scrub_environment};
use crate::secured::{self, CA_CERTIFICATE_FILE, Secured};
use crate::server::{Answer, Request, ServerApi};
use crate::session::{self, TokenUsage};
use crate::system::System;
use crate::template::PROVIDER;
use crate::tls_client::ALPN_H2;

/// Client credential directory beneath the client home.
const CLIENT_DIRECTORY: [&str; 2] = [".config", "gang"];
/// Stored workload token filename.
const TOKEN_FILE: &str = "token";
/// Stored identity-provider token filename.
const ID_TOKEN_FILE: &str = "id-token";
/// Stored refresh token filename.
const REFRESH_TOKEN_FILE: &str = "refresh-token";
/// Worker client certificate filename.
const WORKER_CERTIFICATE: &str = "certificate.pem";
/// Worker private key filename.
const WORKER_KEY: &str = "private-key.pem";
/// Worker CA filename.
const WORKER_AUTHORITY: &str = "ca.pem";
/// Bazel output root used by doctor.
const DOCTOR_OUTPUT_ROOT: &str = "doctor-bazel";
/// Proof command subcommand.
const PROOF_COMMAND: &str = "proof-command";
/// Age of an expired token's issue time.
const EXPIRED_AGE: Duration = Duration::from_hours(2);
/// Lifetime of an expired token.
const EXPIRED_LIFETIME: Duration = Duration::from_hours(1);
/// Lifetime of a current token.
const CURRENT_LIFETIME: Duration = Duration::from_mins(10);

/// Process and output readers for an interactive login.
type LoginProcess = (
    std::process::Child,
    mpsc::Receiver<String>,
    thread::JoinHandle<()>,
    thread::JoinHandle<String>,
);

/// The identity scenario's real deployment adapter.
impl Access for Secured<'_> {
    type Channel = H2Channel;

    fn endpoint(&self) -> Result<Url> {
        self.stack().endpoint()
    }

    fn server_pid(&self) -> Option<u32> {
        System::server_pid(self.stack())
    }

    fn administrator(&self) -> Result<Credentials> {
        Identity::administrator(self)
    }

    fn workload(&self, subject: &str) -> Result<Credentials> {
        match Identity::issue(self, &TokenSpec::admitted(subject))? {
            Issuance::Issued(credentials) => Ok(credentials),
            Issuance::Refused(output) => Err(E2eError::system(format!(
                "the server refused the workload {subject}: {output}"
            ))),
        }
    }

    fn misdirected_login(&self, direction: Misdirection) -> Result<MisdirectedLogin> {
        let home = self.new_home()?;
        let (proof, _) = proof_paths(&home);
        secured::write_secret(
            &proof,
            &self
                .issuer()
                .mint(&TokenSpec::admitted(crate::access::MEMBER_SUBJECT))?,
        )?;
        let endpoint = self.stack().endpoint()?;
        let host = endpoint
            .host_str()
            .ok_or_else(|| E2eError::system(format!("{endpoint} has no host")))?;
        let port = endpoint
            .port_or_known_default()
            .ok_or_else(|| E2eError::system(format!("{endpoint} has no port")))?;
        let host_port = format!("{host}:{port}");
        let server = match direction {
            Misdirection::WrongPin => endpoint.as_str().to_owned(),
            Misdirection::PlainScheme => format!("http://{host_port}"),
            Misdirection::UnsupportedScheme => format!("ftp://{host_port}"),
        };
        let pin = if direction == Misdirection::WrongPin {
            CaPin::new([0; 32])
        } else {
            self.stack().ca_pin()?
        };
        let mut command = gang(self, &home);
        command
            .arg("login")
            .arg("--server")
            .arg(server)
            .arg("--ca-pin")
            .arg(pin.to_string())
            .arg("--workload")
            .arg("--provider")
            .arg(PROVIDER)
            .arg("--token")
            .arg(proof);
        let outcome = process::run(command, None, self.stack().settings().command_timeout)?;
        Ok(MisdirectedLogin {
            outcome,
            stored: stored_files(&home)?,
        })
    }

    fn administer(&self, who: &Credentials, action: &Administration) -> Result<Ran> {
        let mut command = gang(self, &who.home);
        match action {
            Administration::RevokePrincipal(principal) => {
                command.arg("revoke").arg("--principal").arg(principal);
            }
            Administration::RestorePrincipal(principal) => {
                command.arg("restore").arg("--principal").arg(principal);
            }
            Administration::RevokeSerial(serial) => {
                command.arg("revoke").arg("--serial").arg(serial.as_str());
            }
        }
        process::run(command, None, self.stack().settings().command_timeout)
    }

    fn request(&self, request: &Request) -> Result<Answer> {
        ServerApi::send(self.stack(), request)
    }

    fn visit(&self, url: &Url) -> Result<Answer> {
        let reply = self.visit_http.send(gang_tls::HttpRequest {
            method: http::Method::GET,
            url,
            bearer: None,
            headers: &[],
            body: None,
        })?;
        Ok(Answer {
            status: reply.status.as_u16(),
            headers: reply
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_ascii_lowercase(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
                .collect(),
            body: reply.body.to_vec(),
        })
    }

    fn mint_bearer(&self, principal: &str, validity: Validity) -> Result<String> {
        let endpoint = self.stack().endpoint()?;
        let current = now();
        let (issued, lifetime) = match validity {
            Validity::Current => (current, CURRENT_LIFETIME),
            Validity::Expired => (
                Timestamp::from_unix_millis(
                    current.as_unix_millis().saturating_sub(millis(EXPIRED_AGE)),
                ),
                EXPIRED_LIFETIME,
            ),
        };
        session::signed_token(
            self.stack().signer_key()?,
            &endpoint,
            principal,
            TokenUsage::Client,
            issued,
            lifetime,
        )
    }

    fn session_cookie(&self, who: &Credentials) -> Result<session::SessionCookie> {
        Identity::session_cookie(self, who)
    }

    fn command_login(&self) -> Result<Login> {
        let home = self.new_home()?;
        let (proof, runs) = proof_paths(&home);
        secured::write_secret(
            &proof,
            &self
                .issuer()
                .mint(&TokenSpec::admitted(crate::access::PROVER_SUBJECT))?,
        )?;
        let executable =
            std::env::current_exe().map_err(E2eError::io("locating the harness binary"))?;
        let endpoint = self.stack().endpoint()?;
        let pin = self.stack().ca_pin()?;
        let mut command = gang(self, &home);
        command
            .arg("login")
            .arg("--server")
            .arg(endpoint.as_str())
            .arg("--ca-pin")
            .arg(pin.to_string())
            .arg("--workload")
            .arg("--provider")
            .arg(PROVIDER)
            .arg("--token-command")
            .arg(executable)
            .arg(PROOF_COMMAND)
            .arg(proof)
            .arg(runs);
        let outcome = process::run(command, None, self.stack().settings().command_timeout)?;
        Ok(Login { home, outcome })
    }

    fn proof_runs(&self, home: &Path) -> Result<usize> {
        let (_, runs) = proof_paths(home);
        match fs::read_to_string(&runs) {
            Ok(text) => Ok(text.lines().count()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(E2eError::io(format!("reading {}", runs.display()))(error)),
        }
    }

    fn interactive_login(&self) -> Result<Login> {
        let home = self.new_home()?;
        let (mut child, receiver, stderr_thread, stdout_thread) = self.start_login(&home)?;
        let waited = self.stack().settings().history_timeout;
        let deadline = Instant::now() + waited;
        let prefix = format!(
            "{}/authorize",
            self.issuer().url().as_str().trim_end_matches('/')
        );
        let mut lines = Vec::new();
        let authorization = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match receiver.recv_timeout(remaining) {
                Ok(line) => {
                    let is_authorization = line.trim().starts_with(&prefix);
                    let authorization = is_authorization.then(|| line.trim().to_owned());
                    lines.push(line);
                    if let Some(authorization) = authorization {
                        break authorization;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // `gang login` ended without an authorization URL: its outcome is the verdict.
                    let status = child
                        .wait()
                        .map_err(E2eError::io("waiting for interactive gang login"))?;
                    let stdout = stdout_thread.join().unwrap_or_default();
                    let _ = stderr_thread.join();
                    return Ok(Login {
                        home,
                        outcome: Ran {
                            code: status.code(),
                            stdout,
                            stderr: lines.join("\n"),
                        },
                    });
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = stderr_thread.join();
                    let _ = stdout_thread.join();
                    return Err(E2eError::timeout(
                        "gang login to print its authorization URL and finish",
                        waited,
                    ));
                }
            }
        };
        let authorization_url = Url::parse(&authorization)
            .map_err(E2eError::system_from("parsing the authorization URL"))?;
        let approved = self.visit(&authorization_url)?;
        let Some(location) = approved
            .header("location")
            .and_then(|value| Url::parse(value).ok())
        else {
            let _ = child.kill();
            let _ = child.wait();
            let stdout = stdout_thread.join().unwrap_or_default();
            let _ = stderr_thread.join();
            return Ok(Login {
                home,
                outcome: Ran {
                    code: child
                        .try_wait()
                        .ok()
                        .flatten()
                        .and_then(|status| status.code()),
                    stdout,
                    stderr: lines.join("\n"),
                },
            });
        };
        self.visit(&location)?;
        if !drain_login_output(&receiver, deadline, &mut lines) {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stderr_thread.join();
            let _ = stdout_thread.join();
            return Err(E2eError::timeout(
                "gang login to print its authorization URL and finish",
                waited,
            ));
        }
        let status = child
            .wait()
            .map_err(E2eError::io("waiting for interactive gang login"))?;
        let stdout = stdout_thread.join().unwrap_or_default();
        let _ = stderr_thread.join();
        Ok(Login {
            home,
            outcome: Ran {
                code: status.code(),
                stdout,
                stderr: lines.join("\n"),
            },
        })
    }

    fn get(&self, home: &Path) -> Result<TokenAnswer> {
        let endpoint = self.stack().endpoint()?;
        let input = serde_json::json!({"uri": endpoint.as_str()}).to_string();
        let mut command = gang(self, home);
        command.arg("get");
        let outcome = process::run(
            command,
            Some(input.as_bytes()),
            self.stack().settings().command_timeout,
        )?;
        let bearer = if outcome.code == Some(0) {
            secured::bearer_of(&outcome.stdout).ok()
        } else {
            None
        };
        Ok(TokenAnswer { outcome, bearer })
    }

    fn age(&self, home: &Path, aging: Aging) -> Result<()> {
        let path = client_file(home, TOKEN_FILE);
        let text = fs::read_to_string(&path)
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        let mut value: serde_json::Value =
            serde_json::from_str(&text).map_err(E2eError::parse("the stored token"))?;
        let current_millis = now().as_unix_millis();
        value["issued_at"] = serde_json::json!(current_millis.saturating_sub(millis(EXPIRED_AGE)));
        value["expires_at"] =
            serde_json::json!(current_millis.saturating_sub(millis(EXPIRED_LIFETIME)));
        secured::write_secret(&path, &value.to_string())?;
        if aging == Aging::TokenAndProof {
            secured::write_secret(
                &client_file(home, ID_TOKEN_FILE),
                &self
                    .issuer()
                    .mint_expired(&TokenSpec::admitted(crate::access::SIGN_IN_SUBJECT))?,
            )?;
        }
        Ok(())
    }

    fn refresh_grants(&self, home: &Path) -> Result<usize> {
        let token = fs::read_to_string(client_file(home, REFRESH_TOKEN_FILE))
            .map_err(E2eError::io("reading the refresh token"))?;
        Ok(self.issuer().refresh_grants(token.trim()))
    }

    fn reject_refresh(&self, home: &Path) -> Result<()> {
        let token = fs::read_to_string(client_file(home, REFRESH_TOKEN_FILE))
            .map_err(E2eError::io("reading the refresh token"))?;
        self.issuer().reject_refresh_token(token.trim());
        Ok(())
    }

    fn doctor(&self, who: &Credentials) -> Result<Ran> {
        let rc = self.render_bazelrc(&who.home)?;
        let placement = self.stack().placement();
        let settings = self.stack().settings();
        let bazelrc = who.home.join(".bazelrc");
        let mut text = format!(
            "startup --output_user_root={}\n",
            placement.root().join(DOCTOR_OUTPUT_ROOT).display()
        );
        for flag in &settings.bazel_startup_flags {
            text.push_str("startup ");
            text.push_str(flag);
            text.push('\n');
        }
        text.push_str(
            &fs::read_to_string(&rc).map_err(E2eError::io(format!("reading {}", rc.display())))?,
        );
        fs::write(&bazelrc, text)
            .map_err(E2eError::io(format!("writing {}", bazelrc.display())))?;
        // The credential helper's directory only: the harness never hands the host PATH on.
        let path = helper_path(self.stack().binary(), "");
        let mut command = gang(self, &who.home);
        command
            .current_dir(placement.workspace())
            .arg("doctor")
            .arg("--bazel")
            .arg(self.stack().bazel_program())
            .env("PATH", &path);
        let outcome = process::run(command, None, settings.command_timeout)?;
        let mut shutdown = Command::new(self.stack().bazel_program());
        shutdown
            .arg("shutdown")
            .current_dir(placement.workspace())
            .env("HOME", &who.home)
            .env("XDG_CONFIG_HOME", &who.home)
            .env("PATH", path);
        let _ = process::run(shutdown, None, settings.command_timeout);
        Ok(outcome)
    }

    fn standing_worker(&self) -> Result<WorkerIdentity> {
        System::await_stream(self.stack(), &mut |frames| {
            crate::mirror::follow(frames)
                .state
                .is_some_and(|state| !state.workers.is_empty())
        })?;
        let root = self.stack().placement().state_dir();
        let mut candidates = Vec::new();
        collect_worker_certs(&root, &mut candidates)?;
        if candidates.len() != 1 {
            return Err(E2eError::system(format!(
                "expected one worker certificate under {}, found {:?}",
                root.display(),
                candidates
            )));
        }
        let path = candidates.remove(0);
        let directory = path
            .parent()
            .ok_or_else(|| E2eError::system("worker certificate has no parent"))?
            .to_path_buf();
        let instance = directory
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .ok_or_else(|| E2eError::system("worker certificate has no instance directory"))?
            .to_path_buf();
        let pem = fs::read_to_string(&path)
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        let certificate = Certificate::from_pem(pem.as_bytes())
            .map_err(E2eError::parse("the worker certificate"))?;
        let serial = gang_types::CertSerial::from_be_bytes(
            certificate.tbs_certificate().serial_number().as_bytes(),
        )
        .map_err(E2eError::parse("the worker certificate"))?;
        Ok(WorkerIdentity {
            serial,
            directory,
            instance,
        })
    }

    fn worker_exit_log(&self, worker: &WorkerIdentity) -> Result<Option<String>> {
        let lock = worker.instance.join("alive.lock");
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("identity-worker-exit".to_owned())
            .spawn(move || {
                // The lock is released at once: holding it would mark the instance alive.
                let released = gang_os::FileLock::exclusive_blocking(&lock).map(drop);
                let _ = sender.send(released);
            })
            .map_err(E2eError::io("starting identity worker exit wait"))?;
        match receiver.recv_timeout(self.stack().settings().history_timeout) {
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(E2eError::system(
                "the worker exit wait ended without an answer",
            )),
            Ok(Err(error)) => Err(E2eError::system_from("waiting for the worker to exit")(
                error,
            )),
            Ok(Ok(())) => {
                let name = worker
                    .instance
                    .file_name()
                    .ok_or_else(|| E2eError::system("worker instance has no file name"))?;
                let path = self
                    .stack()
                    .placement()
                    .state_dir()
                    .join("logs")
                    .join(name)
                    .with_extension("log");
                let text =
                    fs::read(&path).map_err(E2eError::io(format!("reading {}", path.display())))?;
                Ok(Some(String::from_utf8_lossy(&text).into_owned()))
            }
        }
    }

    fn connect_worker(&self, worker: Option<&WorkerIdentity>) -> Result<Admission<Self::Channel>> {
        let url = Url::parse(&format!("https://{}", self.stack().worker_endpoint()?))
            .map_err(E2eError::system_from("parsing the worker endpoint"))?;
        let authority = worker.map_or_else(
            || {
                self.stack()
                    .placement()
                    .workspace()
                    .join(CA_CERTIFICATE_FILE)
            },
            |identity| identity.directory.join(WORKER_AUTHORITY),
        );
        let authority_text = fs::read_to_string(&authority)
            .map_err(E2eError::io(format!("reading {}", authority.display())))?;
        let certificate = CertificatePem::try_new(authority_text)
            .map_err(E2eError::system_from("reading worker authority PEM"))?;
        let identity = worker
            .map(|identity| -> Result<_> {
                let cert_path = identity.directory.join(WORKER_CERTIFICATE);
                let key_path = identity.directory.join(WORKER_KEY);
                let cert = CertificatePem::try_new(
                    fs::read_to_string(&cert_path)
                        .map_err(E2eError::io(format!("reading {}", cert_path.display())))?,
                )
                .map_err(E2eError::system_from("reading worker certificate PEM"))?;
                let key = PrivateKeyPem::try_new(
                    fs::read_to_string(&key_path)
                        .map_err(E2eError::io(format!("reading {}", key_path.display())))?,
                )
                .map_err(E2eError::system_from("reading worker private key PEM"))?;
                Ok((cert, key))
            })
            .transpose()?;
        let config: ClientConfig = gang_tls::client_config(
            gang_tls::ServerTrust::Ca(&certificate),
            identity
                .as_ref()
                .map(|(certificate, private_key)| gang_tls::ClientIdentity {
                    certificate,
                    private_key,
                }),
            &[ALPN_H2],
            Arc::new(SystemClock::new()),
        )
        .map_err(E2eError::system_from("configuring worker TLS"))?;
        H2Channel::connect(
            Arc::new(config),
            &url,
            None,
            self.stack().settings().handshake_timeout,
        )
    }

    fn restart_server(&self) -> Result<()> {
        System::kill_server(self.stack())?;
        System::start_server(self.stack())
    }
}

impl Secured<'_> {
    /// Starts an interactive login and captures its output streams.
    fn start_login(&self, home: &Path) -> Result<LoginProcess> {
        let endpoint = self.stack().endpoint()?;
        let pin = self.stack().ca_pin()?;
        let mut command = gang(self, home);
        command
            .arg("login")
            .arg("--server")
            .arg(endpoint.as_str())
            .arg("--ca-pin")
            .arg(pin.to_string())
            .arg("--provider")
            .arg(PROVIDER)
            .env("PATH", "")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        scrub_environment(&mut command);
        let mut child = command
            .spawn()
            .map_err(E2eError::io("starting interactive gang login"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| E2eError::system("interactive login stderr was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| E2eError::system("interactive login stdout was not piped"))?;
        let (sender, receiver) = mpsc::channel();
        let stderr_thread = thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(stderr).lines() {
                match line {
                    Ok(line) => {
                        if sender.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let stdout_thread = thread::spawn(move || {
            let mut text = String::new();
            let mut reader = std::io::BufReader::new(stdout);
            let _ = reader.read_to_string(&mut text);
            text
        });
        Ok((child, receiver, stderr_thread, stdout_thread))
    }
}

/// Collects the remaining login output until the stream closes or the deadline expires.
fn drain_login_output(
    receiver: &mpsc::Receiver<String>,
    deadline: Instant,
    lines: &mut Vec<String>,
) -> bool {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(remaining) {
            Ok(line) => lines.push(line),
            Err(mpsc::RecvTimeoutError::Disconnected) => return true,
            Err(mpsc::RecvTimeoutError::Timeout) => return false,
        }
    }
}

/// Returns whole milliseconds, saturating at `u64::MAX`.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Runs by `gang login --token-command`: records an invocation then prints the proof file.
///
/// # Errors
///
/// This function reports failures through its exit code and standard error.
#[must_use]
pub fn proof_command(arguments: &[OsString]) -> ExitCode {
    if arguments.len() != 2 {
        let _ = writeln!(std::io::stderr(), "usage: proof-command PROOF RUNS");
        return ExitCode::from(2);
    }
    let result = (|| -> std::io::Result<()> {
        let proof = Path::new(&arguments[0]);
        let runs = Path::new(&arguments[1]);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(runs)?;
        writeln!(file, "run")?;
        let mut content = fs::File::open(proof)?;
        std::io::copy(&mut content, &mut std::io::stdout())?;
        Ok(())
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "{error}");
            ExitCode::from(1)
        }
    }
}

/// Constructs a gang command scoped to this client home.
fn gang(secured: &Secured<'_>, home: &Path) -> Command {
    let mut command = Command::new(secured.stack().binary());
    command
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home)
        .env_remove("RUNNER_TEMP");
    command
}

/// Lists regular files below a home directory, relative to it and sorted.
fn stored_files(home: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_files(home, home, &mut files)?;
    files.sort();
    Ok(files)
}

/// Recursively collects regular files below `directory`.
fn collect_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(E2eError::io(format!("reading {}", directory.display()))(
                error,
            ));
        }
    };
    for entry in entries {
        let entry = entry.map_err(E2eError::io(format!("reading {}", directory.display())))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        if kind.is_dir() {
            collect_files(root, &path, files)?;
        } else if kind.is_file() {
            files.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
    Ok(())
}

/// Resolves a credential file in the gang client store.
fn client_file(home: &Path, name: &str) -> PathBuf {
    home.join(CLIENT_DIRECTORY[0])
        .join(CLIENT_DIRECTORY[1])
        .join(name)
}

/// Reads the wall-clock timestamp.
fn now() -> Timestamp {
    SystemClock::new().wall()
}

/// Resolves the proof file and its invocation log for a home.
fn proof_paths(home: &Path) -> (PathBuf, PathBuf) {
    (home.with_extension("proof"), home.with_extension("runs"))
}

/// Recursively finds worker certificate files beneath `root`.
fn collect_worker_certs(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(E2eError::io(format!("reading {}", root.display()))(error));
        }
    };
    for entry in entries {
        let entry = entry.map_err(E2eError::io(format!("reading {}", root.display())))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        if kind.is_dir() {
            collect_worker_certs(&path, files)?;
        } else if kind.is_file()
            && entry.file_name() == WORKER_CERTIFICATE
            && path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .is_some_and(|name| name == "identity")
        {
            files.push(path);
        }
    }
    Ok(())
}
