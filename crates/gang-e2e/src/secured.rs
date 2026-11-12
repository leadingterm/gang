//! The bearer-token deployment the Gate 2 scenarios drive: a [`Stack`] whose public listener is
//! server-TLS only, with developers logged in through the real `gang` client.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gang_clock::SystemClock;
use gang_types::{CaPin, Url};

use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::flags::Remote;
use crate::gang_cli::GangCli;
use crate::h2_frames::H2Channel;
use crate::identity::{Admission, Credentials, Identity, Issuance, Presenting};
use crate::issuer::{TestIssuer, TokenSpec};
use crate::process::run;
use crate::session::SessionCookie;
use crate::stack::Stack;
use crate::state_api::{StateApi, StateStream};
use crate::suite::Invocation;
use crate::system::{Probe, System, Variant};
use crate::template::{PROVIDER, Transport};
use crate::tls_client::{self, ALPN_H2};

/// The probes of a build made with a bearer token: every probe the lifecycle builds.
const ALL_BUILD_PROBES: &[Probe] = &[Probe::Isolated, Probe::NetworkRequested, Probe::Pinned];

/// The probe of a build made without a token.
const PINNED_PROBE: &[Probe] = &[Probe::Pinned];

/// The file in a client home that holds the token a login presents.
const TOKEN_FILE: &str = "token";

/// The Bazel rc file in a client home that `gang bazelrc` renders.
pub(crate) const BAZEL_RC_FILE: &str = "bazel.rc";

/// The Gang CA certificate that `gang bazelrc` writes into the workspace.
pub(crate) const CA_CERTIFICATE_FILE: &str = "gang-ca.pem";

/// The Bazel rc file in an anonymous client home: a copy of the rendered rc without its
/// credential helper.
const ANONYMOUS_BAZEL_RC_FILE: &str = "anonymous.bazelrc";

/// A started deployment whose public listener needs a bearer token, and the `gang` client that
/// logs developers in.
pub struct Secured<'a> {
    /// The running deployment.
    stack: Stack<'a>,
    /// The in-process issuer that vouches for the developers' tokens.
    issuer: &'a TestIssuer,
    /// The number of client homes created so far, which names the next one.
    homes: AtomicU64,
    /// The client of the sign-in pages the flow visits; see [`visit_client`].
    pub(crate) visit_http: crate::http::Http,
}

impl<'a> Secured<'a> {
    /// Creates the deployment under `variant` and starts its server.
    ///
    /// # Errors
    ///
    /// When the deployment cannot be created or the server does not start listening.
    pub fn create(
        invocation: &Invocation,
        variant: &Variant,
        issuer: &'a TestIssuer,
    ) -> Result<Self> {
        let stack = Stack::create(invocation, variant, Transport::Tls, issuer)?;
        stack.start_server()?;
        stack.administrator()?.enroll(&stack.ca_pin()?)?;
        Ok(Self {
            stack,
            issuer,
            visit_http: visit_client(invocation.settings.command_timeout)?,
            homes: AtomicU64::new(0),
        })
    }

    /// Creates an empty client home that no earlier login has used.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the home cannot be created.
    pub fn new_home(&self) -> Result<PathBuf> {
        let index = self.homes.fetch_add(1, Ordering::Relaxed);
        let home = self
            .stack
            .placement()
            .home()
            .join(format!("client-{index}"));
        std::fs::create_dir_all(&home)
            .map_err(E2eError::io(format!("creating {}", home.display())))?;
        Ok(home)
    }

    /// The deployment wrapped by this client.
    pub fn stack(&self) -> &Stack<'a> {
        &self.stack
    }

    /// The issuer that vouches for the developers' tokens.
    pub fn issuer(&self) -> &'a TestIssuer {
        self.issuer
    }

    /// The `gang` client that uses `home` as its client home.
    fn client(&self, home: PathBuf) -> GangCli {
        GangCli::new(
            self.stack.binary().to_path_buf(),
            self.stack.placement().config_file(),
            home,
        )
    }

    /// The Bazel rc file of a client in `home`, which `gang bazelrc` renders. Rendering also
    /// writes the Gang CA certificate into the workspace, where the TLS clients read it.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint or CA pin is unavailable or rendering fails.
    pub fn render_bazelrc(&self, home: &Path) -> Result<PathBuf> {
        render_bazelrc(
            &self.client(home.to_path_buf()),
            &self.stack.endpoint()?,
            Some(&self.stack.ca_pin()?),
            &self.stack.placement().workspace(),
            home,
            self.stack.settings().command_timeout,
        )
    }

    /// The Bazel remote for a client in `home`, whose credential helper is the `gang` binary.
    fn remote(&self, home: &Path) -> Result<Remote> {
        let endpoint = self.stack.endpoint()?;
        let host = endpoint
            .host_str()
            .ok_or_else(|| E2eError::system(format!("{endpoint} has no host")))?;
        Ok(Remote::Gang {
            host: host.to_owned(),
            bazelrc: self.render_bazelrc(home)?,
            home: home.to_path_buf(),
            helper: self.stack.binary().to_path_buf(),
        })
    }

    /// The Bazel remote of an anonymous client: its rc file is a copy of the one `gang bazelrc`
    /// renders in a fresh home, without the credential helper, so Bazel sends no credentials and
    /// the server answers for an unauthenticated client.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::new_home`] and [`Self::render_bazelrc`], and [`E2eError::Io`] when
    /// the rc file cannot be read or written.
    fn anonymous_remote(&self) -> Result<Remote> {
        let home = self.new_home()?;
        let rendered = self.render_bazelrc(&home)?;
        let text = std::fs::read_to_string(&rendered)
            .map_err(E2eError::io(format!("reading {}", rendered.display())))?;
        let bazelrc = home.join(ANONYMOUS_BAZEL_RC_FILE);
        std::fs::write(&bazelrc, without_credential_helper(&text))
            .map_err(E2eError::io(format!("writing {}", bazelrc.display())))?;
        Ok(Remote::Plain { bazelrc, home })
    }

    /// The state API of the public endpoint, presenting `cookie` when given and nothing else. It
    /// trusts the Gang CA certificate that `gang bazelrc` wrote into the workspace when the
    /// server started.
    fn state_api(&self, cookie: Option<&SessionCookie>) -> Result<StateApi> {
        let endpoint = self.stack.endpoint()?;
        let authority = self.stack.placement().workspace().join(CA_CERTIFICATE_FILE);
        match cookie {
            Some(cookie) => StateApi::with_cookie(
                &endpoint,
                Some(&authority),
                cookie,
                self.stack.settings().handshake_timeout,
            ),
            None => StateApi::anonymous(
                &endpoint,
                Some(&authority),
                self.stack.settings().handshake_timeout,
            ),
        }
    }
}

/// The client of the pages a browser visits while signing in: the identity provider's
/// authorization endpoint and the `gang login` loopback callback. Both are plain HTTP on the
/// loopback interface (the test issuer serves `http`), so the client is plain.
fn visit_client(timeout: Duration) -> Result<crate::http::Http> {
    crate::http::Http::try_new(
        gang_tls::HttpTransport::Plain,
        gang_tls::HttpProtocol::Negotiated,
        timeout,
    )
}

impl Identity for Secured<'_> {
    type Channel = H2Channel;
    type Stream = StateStream;

    fn issue(&self, spec: &TokenSpec) -> Result<Issuance> {
        let home = self.new_home()?;
        log_in(
            &self.client(home.clone()),
            self.issuer,
            spec,
            &self.stack.endpoint()?,
            &self.stack.ca_pin()?,
            &self.stack.placement().workspace(),
            self.stack.settings().command_timeout,
        )
    }

    fn connect(
        &self,
        credentials: &Credentials,
        presenting: Presenting,
    ) -> Result<Admission<H2Channel>> {
        let config = tls_client::client_config(
            credentials.authority.as_deref(),
            &[ALPN_H2],
            Arc::new(SystemClock::new()),
        )?;
        let token = match presenting {
            Presenting::Token => Some(credentials.token.as_str()),
            Presenting::Nothing => None,
        };
        H2Channel::connect(
            Arc::new(config),
            &self.stack.endpoint()?,
            token,
            self.stack.settings().handshake_timeout,
        )
    }

    fn build_as(&self, credentials: &Credentials, presenting: Presenting) -> Result<BuildOutcome> {
        let (remote, probes) = match presenting {
            Presenting::Token => (self.remote(&credentials.home)?, ALL_BUILD_PROBES),
            Presenting::Nothing => (self.anonymous_remote()?, PINNED_PROBE),
        };
        self.stack.run_clean(&remote)?;
        self.stack.run_build(&remote, probes)
    }

    fn revoke(&self, target: &Credentials) -> Result<()> {
        self.stack
            .administrator()?
            .revoke(&self.stack.ca_pin()?, &target.principal)
    }

    fn restore(&self, target: &Credentials) -> Result<()> {
        self.stack
            .administrator()?
            .restore(&self.stack.ca_pin()?, &target.principal)
    }

    fn server_pid(&self) -> Option<u32> {
        self.stack.server_pid()
    }

    fn administrator(&self) -> Result<Credentials> {
        self.stack
            .administrator()?
            .credentials(&self.stack.ca_pin()?)
    }

    fn session_cookie(&self, who: &Credentials) -> Result<SessionCookie> {
        self.stack.session_cookie(&who.principal)
    }

    fn status(&self, path: &str, cookie: Option<&SessionCookie>) -> Result<u16> {
        self.state_api(cookie)?.status(path)
    }

    fn open_state_stream(&self, cookie: &SessionCookie) -> Result<StateStream> {
        self.state_api(Some(cookie))?.open_stream()
    }
}

/// Logs `spec` in through `cli` against `endpoint`, in the client home `home`, and returns the
/// session's credentials. The token is minted by `issuer` and written to a file only the owner
/// can read.
///
/// # Errors
///
/// [`E2eError::Issuer`] when the token cannot be minted, [`E2eError::Parse`] when the session's
/// bearer cannot be read, [`E2eError::System`] when the token file cannot be written, and
/// [`E2eError::Command`] when `gang bazelrc` or `gang get` does not succeed. A login the server
/// refuses is `Ok` with [`Issuance::Refused`].
pub(crate) fn log_in(
    cli: &GangCli,
    issuer: &TestIssuer,
    spec: &TokenSpec,
    endpoint: &Url,
    pin: &CaPin,
    workspace: &Path,
    timeout: Duration,
) -> Result<Issuance> {
    let home = cli.home();
    let token = issuer.mint(spec)?;
    let token_file = home.join(TOKEN_FILE);
    write_secret(&token_file, &token)?;
    let login = run(
        cli.login_workload(endpoint, pin, &token_file),
        None,
        timeout,
    )?;
    if !login.succeeded() {
        return Ok(Issuance::Refused(login.combined()));
    }
    render_bazelrc(cli, endpoint, Some(pin), workspace, home, timeout)?;
    let request = serde_json::json!({ "uri": endpoint.as_str() }).to_string();
    let answer = run(cli.get(), Some(request.as_bytes()), timeout)?;
    if !answer.succeeded() {
        return Err(E2eError::Command {
            program: "gang get".to_owned(),
            outcome: answer
                .code
                .map_or_else(|| "a signal".to_owned(), |code| format!("status {code}")),
            output: answer.combined(),
        });
    }
    Ok(Issuance::Issued(Credentials {
        principal: format!("{PROVIDER}:{}", spec.subject),
        home: home.to_path_buf(),
        token: bearer_of(&answer.stdout)?,
        authority: Some(workspace.join(CA_CERTIFICATE_FILE)),
    }))
}

/// Renders the Bazel rc file of `endpoint` into `home` with `gang bazelrc`. The pin, when there is
/// one, is passed to `gang bazelrc`, which then writes the Gang CA certificate into `workspace`.
/// A plaintext deployment has no pin. Returns the path of the rc file.
///
/// # Errors
///
/// [`E2eError::Command`] when `gang bazelrc` does not succeed, [`E2eError::Io`] when the rc file
/// cannot be written.
fn render_bazelrc(
    cli: &GangCli,
    endpoint: &Url,
    pin: Option<&CaPin>,
    workspace: &Path,
    home: &Path,
    timeout: Duration,
) -> Result<PathBuf> {
    let rendered = run(cli.bazelrc(endpoint, pin, workspace), None, timeout)?;
    if !rendered.succeeded() {
        return Err(E2eError::Command {
            program: "gang bazelrc".to_owned(),
            outcome: rendered
                .code
                .map_or_else(|| "a signal".to_owned(), |code| format!("status {code}")),
            output: rendered.combined(),
        });
    }
    let path = home.join(BAZEL_RC_FILE);
    std::fs::write(&path, rendered.stdout)
        .map_err(E2eError::io(format!("writing {}", path.display())))?;
    Ok(path)
}

/// Renders the Bazel rc file of a deployment that `binary` serves at `endpoint` into `home`, with
/// the configuration at `config` and the pin of its certificate authority, when it has one.
/// Returns the path of the rc file.
///
/// # Errors
///
/// The errors of [`render_bazelrc`].
pub(crate) fn render_deployment_bazelrc(
    binary: &Path,
    config: &Path,
    endpoint: &Url,
    pin: Option<CaPin>,
    workspace: &Path,
    home: &Path,
    timeout: Duration,
) -> Result<PathBuf> {
    let cli = GangCli::new(
        binary.to_path_buf(),
        config.to_path_buf(),
        home.to_path_buf(),
    );
    render_bazelrc(&cli, endpoint, pin.as_ref(), workspace, home, timeout)
}

/// The bearer token of the `authorization` header in the answer of `gang get`.
///
/// # Errors
///
/// [`E2eError::Parse`] when the answer is not JSON with a `Bearer` authorization header.
pub fn bearer_of(answer: &str) -> Result<String> {
    let value: serde_json::Value =
        serde_json::from_str(answer.trim()).map_err(E2eError::parse("the gang get answer"))?;
    value
        .pointer("/headers/authorization/0")
        .and_then(serde_json::Value::as_str)
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::to_owned)
        .ok_or_else(|| E2eError::Parse {
            subject: "the gang get answer",
            source: "no bearer authorization header".into(),
        })
}

/// Atomically replaces `path` with `secret`, readable only by its owner.
///
/// # Errors
///
/// [`E2eError::System`] when the secret cannot be written.
pub(crate) fn write_secret(path: &Path, secret: &str) -> Result<()> {
    gang_os::fs::write_atomic(path, secret.as_bytes(), gang_os::fs::PRIVATE_FILE)
        .map_err(E2eError::system_from("writing the secret"))
}

/// The Bazel rc file `rc` with every `build --credential_helper=` line removed; every other line,
/// the trailing newline included, is kept as it is.
fn without_credential_helper(rc: &str) -> String {
    rc.split_inclusive('\n')
        .filter(|line| !line.starts_with("build --credential_helper="))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::settings::Settings;

    #[test]
    fn the_visit_client_reaches_the_identity_provider() -> Result<()> {
        let issuer = TestIssuer::start(&Settings::default())?;
        let discovery = issuer
            .url()
            .join("/.well-known/openid-configuration")
            .map_err(E2eError::system_from("forming the discovery url"))?;
        let reply = visit_client(Duration::from_secs(10))?.send(gang_tls::HttpRequest {
            method: http::Method::GET,
            url: &discovery,
            bearer: None,
            headers: &[],
            body: None,
        })?;
        assert_eq!(reply.status, http::StatusCode::OK);
        Ok(())
    }

    #[test]
    fn a_repeated_login_replaces_the_token_and_keeps_it_private() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let path = directory.path().join(TOKEN_FILE);
        write_secret(&path, "first").expect("the first token is written");
        write_secret(&path, "second").expect("a repeated login writes its token over the first");
        assert_eq!(
            std::fs::read_to_string(&path).expect("the token is read"),
            "second"
        );
        let mode = std::fs::metadata(&path)
            .expect("the token has metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        assert_eq!(
            std::fs::read_dir(directory.path())
                .expect("the directory is read")
                .count(),
            1,
            "no staged file is left behind"
        );
    }

    #[test]
    fn an_anonymous_rc_keeps_every_setting_but_the_credential_helper() {
        let rc = "# Written by `gang bazelrc`. Do not edit.\nbuild --remote_executor=grpcs://exec.example.test:443\nbuild --remote_cache=grpcs://cache.example.test:443\nbuild --credential_helper=gang.example.test=/usr/local/bin/gang\nbuild --remote_default_exec_properties=ISA=x86-64\nbuild --tls_certificate=gang-ca.pem\n";
        let expected = "# Written by `gang bazelrc`. Do not edit.\nbuild --remote_executor=grpcs://exec.example.test:443\nbuild --remote_cache=grpcs://cache.example.test:443\nbuild --remote_default_exec_properties=ISA=x86-64\nbuild --tls_certificate=gang-ca.pem\n";
        assert_eq!(without_credential_helper(rc), expected);
    }

    #[test]
    fn an_rc_without_a_credential_helper_is_unchanged() {
        let rc = "# Written by `gang bazelrc`. Do not edit.\nbuild --remote_executor=grpcs://exec.example.test:443\nbuild --remote_cache=grpcs://cache.example.test:443\nbuild --remote_default_exec_properties=ISA=x86-64\nbuild --tls_certificate=gang-ca.pem\n";
        assert_eq!(without_credential_helper(rc), rc);
    }
}
