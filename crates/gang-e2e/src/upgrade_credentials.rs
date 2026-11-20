//! One-shot previous-client cold mints and a shared-authentication drain witness.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::Bytes;
use gang_client::{ClientOptions, ProofSource};
use gang_clock::SystemClock;
use gang_types::{CaPin, ProviderName, Url};
use gang_wire::{AuthenticateRequest, Credential};
use serde::Deserialize;
use tempfile::TempDir;

use crate::errors::{E2eError, Result};
use crate::gang_cli::GangCli;
use crate::identity::Credentials;
use crate::process::{self, Ran};
use crate::secured;
use crate::upgrade_probe::{self, DirectTarget, SharedAuthentication};

/// Two independent cold homes, consumed together without retries or intervening probes.
///
/// Preparation copies only the opaque session bytes. The stored workload proof continues to
/// refer to its original file, with its original lifetime; the original bearer cache is untouched.
#[must_use]
pub(crate) struct ColdHelpers {
    helpers: [ColdHelper; 2],
    request: String,
    timeout: Duration,
}

struct ColdHelper {
    cli: GangCli,
    _home: TempDir,
}

// Validate the workload-file session shape without serializing or adapting any of its contents.
#[derive(Deserialize)]
struct WorkloadSession {
    server: Url,
    #[serde(rename = "ca_pin")]
    _ca_pin: Option<CaPin>,
    provider: ProviderName,
    kind: SessionKind,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SessionKind {
    Workload { source: ProofSource },
}

impl ColdHelpers {
    /// Prepares both isolated homes before the incoming-healthy/pre-stop window.
    pub(crate) fn prepare(
        cli: &GangCli,
        endpoint: &Url,
        probe_root: &Path,
        timeout: Duration,
    ) -> Result<Self> {
        let session = fs::read(client_directory(cli.home()).join("session.json")).map_err(
            E2eError::io("reading the previous client's cold-probe session"),
        )?;
        validate_session(&session, endpoint)?;
        gang_os::fs::require_private_dir(probe_root).map_err(E2eError::system_from(
            "preparing the cold credential probe root",
        ))?;
        Ok(Self {
            helpers: [
                prepare_home(cli, probe_root, &session)?,
                prepare_home(cli, probe_root, &session)?,
            ],
            request: serde_json::json!({ "uri": endpoint.as_str() }).to_string(),
            timeout,
        })
    }

    /// Runs the two actual previous `gang get` commands serially, returning bearer tokens only.
    ///
    /// The availability monitor must call this inside the healthy/pre-stop window without
    /// interleaving other probes. Its clock must keep running throughout both commands. A pair
    /// does not by itself witness which backend served either mint.
    pub(crate) fn mint_pair(self) -> Result<[String; 2]> {
        Ok([self.mint(0)?, self.mint(1)?])
    }

    /// Builds the canonical DTO from the original session only after a warm-pair failure.
    pub(crate) fn diagnostic_body(cli: &GangCli, endpoint: &Url) -> Result<Bytes> {
        let bytes = fs::read(client_directory(cli.home()).join("session.json"))
            .map_err(|_| E2eError::system("warm diagnostic session unavailable"))?;
        let session = validate_session(&bytes, endpoint)?;
        let SessionKind::Workload { source } = session.kind;
        authentication_body(&session.provider, &source)
    }

    fn mint(&self, index: usize) -> Result<String> {
        let helper = &self.helpers[index];
        ensure_cold(helper.cli.home()).map_err(|error| {
            E2eError::system(format!("cold credential probe {}: {error}", index + 1))
        })?;
        let answer = process::run(
            helper.cli.get(),
            Some(self.request.as_bytes()),
            self.timeout,
        )
        .map_err(E2eError::system_from(format!(
            "cold credential probe {} could not run the previous gang get",
            index + 1
        )))?;
        project_answer(&answer, index + 1)
    }
}

fn validate_session(bytes: &[u8], endpoint: &Url) -> Result<WorkloadSession> {
    let session: WorkloadSession = serde_json::from_slice(bytes).map_err(|_| {
        E2eError::system("invalid previous-client session for cold workload credential probes")
    })?;
    if session.server != *endpoint {
        return Err(E2eError::system(
            "cold credential probe endpoint does not match the stored session server",
        ));
    }
    let SessionKind::Workload {
        source: ProofSource::File { path },
    } = &session.kind
    else {
        return Err(E2eError::system(
            "cold credential probes require the original bound workload proof file",
        ));
    };
    if !path.is_absolute() || !path.is_file() {
        return Err(E2eError::system(
            "cold credential probes require the original bound workload proof file",
        ));
    }
    Ok(session)
}

fn prepare_home(cli: &GangCli, root: &Path, session: &[u8]) -> Result<ColdHelper> {
    let home = tempfile::Builder::new()
        .prefix("gang-cold-helper-")
        .tempdir_in(root)
        .map_err(E2eError::io("creating a cold credential helper home"))?;
    for owned_directory in [home.path(), &home.path().join(".config")] {
        gang_os::fs::ensure_private_dir(owned_directory)
            .map_err(E2eError::system_from("securing the owned cold helper home"))?;
    }
    let directory = client_directory(home.path());
    gang_os::fs::ensure_private_dir(&directory)
        .map_err(E2eError::system_from("creating the cold credential store"))?;
    gang_os::fs::write_atomic(
        &directory.join("session.json"),
        session,
        gang_os::fs::PRIVATE_FILE,
    )
    .map_err(E2eError::system_from(
        "copying the opaque previous-client session",
    ))?;
    ensure_cold(home.path())?;
    Ok(ColdHelper {
        cli: cli.with_home(home.path().to_path_buf()),
        _home: home,
    })
}

fn client_directory(home: &Path) -> PathBuf {
    home.join(".config").join("gang")
}

fn ensure_cold(home: &Path) -> Result<()> {
    match fs::symlink_metadata(client_directory(home).join("token")) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(E2eError::io("checking the cold helper bearer cache")(error)),
        Ok(_) => Err(E2eError::system(
            "cold credential helper already has a bearer cache; refusing a cached probe",
        )),
    }
}

fn project_answer(answer: &Ran, index: usize) -> Result<String> {
    if !answer.succeeded() {
        return Err(E2eError::Command {
            program: format!("cold credential probe {index}: previous gang get"),
            outcome: answer.code.map_or_else(
                || "ended by a signal".to_owned(),
                |code| format!("status {code}"),
            ),
            // Helper output can contain credentials even on failure.
            output: format!(
                "helper output withheld to protect credentials; {}",
                helper_failure(&answer.stderr)
            ),
        });
    }
    let token = secured::bearer_of(&answer.stdout).map_err(|_| {
        E2eError::system(format!(
            "cold credential probe {index} returned no valid bearer authorization header"
        ))
    })?;
    if token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/=".contains(&byte))
    {
        return Err(E2eError::system(format!(
            "cold credential probe {index} returned an invalid bearer token"
        )));
    }
    Ok(token)
}

// v0.3.2 record.rs renders this exact uncolored, piped failure prefix. Inspect only the
// fixed status position, never search server-controlled message/body/continuation lines.
fn helper_failure(stderr: &str) -> String {
    let Some(status) = stderr
        .as_bytes()
        .strip_prefix(b"  fail  gang get: the server answered ")
        .and_then(|tail| tail.get(..5))
        .filter(|field| field[..3].iter().all(u8::is_ascii_digit) && &field[3..] == b": ")
        .and_then(|field| std::str::from_utf8(&field[..3]).ok())
        .and_then(|digits| digits.parse::<u16>().ok())
        .filter(|status| (100..=599).contains(status))
    else {
        return "helper_stage=unknown; helper_http_status=unknown".to_owned();
    };
    format!("helper_stage=server_response; helper_http_status={status}")
}

/// A third independent, one-shot old-client helper reserved for the live-drain witness.
#[must_use]
pub(crate) struct DrainCredentials {
    helper: ColdHelper,
    endpoint: Url,
    provider: ProviderName,
    source: ProofSource,
    mint_timeout: Duration,
}

/// Secret-free evidence that shared authentication issued a bearer accepted by the original NLB.
#[derive(Debug)]
pub(crate) struct SharedCredentialWitness {
    pub(crate) status: u16,
    pub(crate) marker_absent: bool,
    pub(crate) token_valid: bool,
}

/// Secret-free offsets from witness entry; success requires every recorded phase to finish.
#[derive(Debug)]
pub(crate) struct DrainCredentialWitness {
    pub(crate) shared_started: Duration,
    pub(crate) shared_finished: Duration,
    pub(crate) shared: SharedCredentialWitness,
    pub(crate) shared_verification_started: Duration,
    pub(crate) shared_verification_finished: Duration,
    pub(crate) mint_started: Duration,
    pub(crate) mint_finished: Duration,
    pub(crate) verification_started: Duration,
    pub(crate) verification_finished: Duration,
    pub(crate) token_valid: bool,
}

impl DrainCredentials {
    /// Copies only the opaque original session, before any incoming-controller warmup.
    pub(crate) fn prepare(
        cli: &GangCli,
        endpoint: &Url,
        root: &Path,
        mint_timeout: Duration,
    ) -> Result<Self> {
        let bytes = fs::read(client_directory(cli.home()).join("session.json")).map_err(
            E2eError::io("reading the previous client's drain-probe session"),
        )?;
        let session = validate_session(&bytes, endpoint)?;
        gang_os::fs::require_private_dir(root).map_err(E2eError::system_from(
            "preparing the drain credential probe root",
        ))?;
        let SessionKind::Workload { source } = session.kind;
        Ok(Self {
            helper: prepare_home(cli, root, &bytes)?,
            endpoint: endpoint.clone(),
            provider: session.provider,
            source,
            mint_timeout,
        })
    }

    /// Uses one absolute budget for shared authentication and a cold NLB mint.
    pub(crate) fn witness(
        self,
        target: DirectTarget,
        credentials: &Credentials,
        probe_timeout: Duration,
        budget: Duration,
    ) -> Result<DrainCredentialWitness> {
        let mint_timeout = self.mint_timeout;
        observe_drain(
            &mut DrainAttempt {
                prepared: self,
                target,
                credentials,
            },
            probe_timeout,
            mint_timeout,
            budget,
        )
    }

    fn authentication_body(&self) -> Result<Bytes> {
        authentication_body(&self.provider, &self.source)
    }
}

fn authentication_body(provider: &ProviderName, source: &ProofSource) -> Result<Bytes> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| E2eError::system("starting workload proof reader unavailable"))?;
    let proof = runtime
        .block_on(source.read(&SystemClock::new(), &ClientOptions::default()))
        .map_err(|_| E2eError::system("reading the original workload proof unavailable"))?;
    serde_json::to_vec(&AuthenticateRequest {
        provider: provider.clone(),
        credential: Credential::Proof(proof),
    })
    .map(Bytes::from)
    .map_err(|_| E2eError::system("serializing the authentication request unavailable"))
}

// The ordered protocol has one clock and no retry/fallback path. This seam also lets regression
// tests drive failures and elapsed time without running helpers or opening network connections.
trait DrainSteps {
    fn now(&self) -> Instant;
    fn original_token(&self) -> &str;
    fn authenticate(&mut self, deadline: Instant) -> Result<SharedAuthentication>;
    fn mint(&mut self, deadline: Instant) -> Result<String>;
    fn verify(&mut self, token: String, deadline: Instant) -> Result<()>;
}

struct DrainAttempt<'a> {
    prepared: DrainCredentials,
    target: DirectTarget,
    credentials: &'a Credentials,
}

impl DrainSteps for DrainAttempt<'_> {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn original_token(&self) -> &str {
        &self.credentials.token
    }

    fn authenticate(&mut self, deadline: Instant) -> Result<SharedAuthentication> {
        let body = self.prepared.authentication_body()?;
        upgrade_probe::probe_shared_authentication(
            &self.prepared.endpoint,
            self.credentials,
            self.target,
            body,
            remaining(deadline, self.now())?,
        )
    }

    fn mint(&mut self, deadline: Instant) -> Result<String> {
        ensure_cold(self.prepared.helper.cli.home())?;
        let request = serde_json::json!({ "uri": self.prepared.endpoint.as_str() }).to_string();
        let answer = process::run(
            self.prepared.helper.cli.get(),
            Some(request.as_bytes()),
            remaining(deadline, self.now())?,
        )
        .map_err(|_| {
            E2eError::system("drain credential probe could not run the previous gang get")
        })?;
        project_answer(&answer, 3)
    }

    fn verify(&mut self, token: String, deadline: Instant) -> Result<()> {
        let minted = Credentials {
            principal: self.credentials.principal.clone(),
            home: self.prepared.helper.cli.home().to_path_buf(),
            token,
            authority: self.credentials.authority.clone(),
        };
        upgrade_probe::probe_capabilities(
            &self.prepared.endpoint,
            &minted,
            remaining(deadline, self.now())?,
        )
    }
}

fn observe_drain(
    steps: &mut dyn DrainSteps,
    probe_timeout: Duration,
    mint_timeout: Duration,
    budget: Duration,
) -> Result<DrainCredentialWitness> {
    let started = steps.now();
    if budget.is_zero() {
        return Err(E2eError::system(
            "drain credential witness requires a budget",
        ));
    }
    let deadline = started
        .checked_add(budget)
        .ok_or_else(|| E2eError::system("drain credential witness deadline overflow"))?;
    let shared_started = steps.now().duration_since(started);
    let shared_deadline = phase_deadline(steps.now(), deadline, probe_timeout)?;
    let shared_limit = remaining(shared_deadline, steps.now())?;
    let shared = steps.authenticate(shared_deadline)?;
    remaining(shared_deadline, steps.now())?;
    if shared.elapsed >= shared_limit || shared.status != 200 || !shared.marker_absent {
        return Err(E2eError::system(format!(
            "drain shared authentication requires timely HTTP 200 without admission marker: status={}, marker_absent={}",
            shared.status, shared.marker_absent
        )));
    }
    let shared_finished = steps.now().duration_since(started);
    let shared_verification_started = steps.now().duration_since(started);
    let shared_verification_deadline = phase_deadline(steps.now(), deadline, probe_timeout)?;
    verify_fresh(steps, shared.token, shared_verification_deadline)?;
    let shared_verification_finished = steps.now().duration_since(started);
    let mint_started = steps.now().duration_since(started);
    let mint_deadline = phase_deadline(steps.now(), deadline, mint_timeout)?;
    let token = steps.mint(mint_deadline)?;
    remaining(mint_deadline, steps.now())?;
    let mint_finished = steps.now().duration_since(started);
    let verification_started = steps.now().duration_since(started);
    let verification_deadline = phase_deadline(steps.now(), deadline, probe_timeout)?;
    verify_fresh(steps, token, verification_deadline)?;
    Ok(DrainCredentialWitness {
        shared_started,
        shared_finished,
        shared: SharedCredentialWitness {
            status: shared.status,
            marker_absent: shared.marker_absent,
            token_valid: true,
        },
        shared_verification_started,
        shared_verification_finished,
        mint_started,
        mint_finished,
        verification_started,
        verification_finished: steps.now().duration_since(started),
        token_valid: true,
    })
}

fn verify_fresh(steps: &mut dyn DrainSteps, token: String, deadline: Instant) -> Result<()> {
    if token.is_empty() || token == steps.original_token() {
        return Err(E2eError::system(
            "drain credential witness requires a fresh nonempty bearer",
        ));
    }
    steps.verify(token, deadline)?;
    remaining(deadline, steps.now())?;
    Ok(())
}

fn remaining(deadline: Instant, now: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(now)
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| E2eError::system("drain credential witness deadline expired"))
}

fn phase_deadline(now: Instant, deadline: Instant, limit: Duration) -> Result<Instant> {
    if limit.is_zero() {
        return Err(E2eError::system(
            "drain credential witness phase budget is zero",
        ));
    }
    now.checked_add(limit.min(remaining(deadline, now)?))
        .ok_or_else(|| E2eError::system("drain credential witness deadline overflow"))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn private_tempdir() -> TempDir {
        let root = tempfile::tempdir().expect("a test root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("the owned test root is explicitly private");
        root
    }

    fn stored_client(root: &Path) -> (GangCli, Url, Vec<u8>) {
        let home = root.join("original");
        let directory = client_directory(&home);
        fs::create_dir_all(&directory).expect("the original credential store exists");
        let proof = home.join("proof.jwt");
        fs::write(&proof, "original proof with unchanged lifetime").expect("the proof exists");
        let endpoint: Url = "https://127.0.0.1:8443".parse().expect("a valid URL");
        let record = serde_json::json!({
            "server": endpoint.as_str(),
            "ca_pin": null,
            "provider": "test",
            "kind": {
                "kind": "workload",
                "source": { "kind": "file", "path": proof },
            },
            "opaque_extension": true,
        });
        let session = format!(" \n{record}\n").into_bytes();
        fs::write(directory.join("session.json"), &session).expect("the original session exists");
        fs::write(directory.join("token"), "original cached bearer").expect("the old cache exists");
        fs::write(
            home.join(".bazelrc"),
            "unchanged previous helper configuration\n",
        )
        .expect("the old rc exists");
        let cli = GangCli::new(root.join("previous-gang"), root.join("previous.toml"), home);
        (cli, endpoint, session)
    }

    #[test]
    fn preparation_copies_only_session_to_two_private_cold_homes() {
        let root = private_tempdir();
        let (cli, endpoint, session) = stored_client(root.path());
        let original_session = client_directory(cli.home()).join("session.json");
        let original_token = client_directory(cli.home()).join("token");
        let session_metadata = fs::metadata(&original_session).expect("session metadata");
        let token_metadata = fs::metadata(&original_token).expect("token metadata");
        let proof = cli.home().join("proof.jwt");
        let proof_metadata = fs::metadata(&proof).expect("proof metadata");
        let pair = ColdHelpers::prepare(&cli, &endpoint, root.path(), Duration::from_secs(1))
            .expect("both cold homes are prepared without executing a binary");
        assert_ne!(pair.helpers[0].cli.home(), pair.helpers[1].cli.home());
        for helper in &pair.helpers {
            let directory = client_directory(helper.cli.home());
            assert_ne!(helper.cli.home(), cli.home());
            assert_eq!(
                fs::read(directory.join("session.json")).expect("the copy"),
                session
            );
            assert_eq!(fs::read_dir(&directory).expect("store entries").count(), 1);
            assert!(!directory.join("token").exists());
            for private in [
                helper.cli.home(),
                &helper.cli.home().join(".config"),
                &directory,
            ] {
                assert_eq!(
                    fs::metadata(private)
                        .expect("directory mode")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700
                );
            }
            assert_eq!(
                fs::metadata(directory.join("session.json"))
                    .expect("file mode")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(helper.cli.get().get_program(), cli.get().get_program());
            assert_eq!(
                helper.cli.server().get_args().collect::<Vec<_>>(),
                cli.server().get_args().collect::<Vec<_>>()
            );
        }
        assert_eq!(
            fs::read(&original_session).expect("original session"),
            session
        );
        assert_eq!(
            fs::read_to_string(&original_token).expect("original cache"),
            "original cached bearer"
        );
        assert_eq!(
            fs::metadata(&original_session)
                .expect("original session metadata")
                .modified()
                .expect("session mtime"),
            session_metadata.modified().expect("prior session mtime")
        );
        assert_eq!(
            fs::metadata(&original_token)
                .expect("original token metadata")
                .modified()
                .expect("token mtime"),
            token_metadata.modified().expect("prior token mtime")
        );
        assert_eq!(
            fs::metadata(&proof)
                .expect("original proof metadata")
                .modified()
                .expect("proof mtime"),
            proof_metadata.modified().expect("prior proof mtime")
        );
        assert_eq!(
            fs::read_to_string(&proof).expect("original proof"),
            "original proof with unchanged lifetime"
        );
        assert_eq!(
            fs::read_to_string(cli.home().join(".bazelrc")).expect("original rc"),
            "unchanged previous helper configuration\n"
        );
        let homes = pair
            .helpers
            .each_ref()
            .map(|helper| helper.cli.home().to_path_buf());
        drop(pair);
        assert!(homes.iter().all(|home| !home.exists()));
        assert!(original_token.exists());
    }

    #[test]
    fn insecure_probe_root_is_rejected_without_changing_permissions() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let probe_root = root.path().join("insecure-probes");
        fs::create_dir(&probe_root).expect("the probe root exists");
        fs::set_permissions(&probe_root, fs::Permissions::from_mode(0o755))
            .expect("the probe root is explicitly insecure");
        let error = ColdHelpers::prepare(&cli, &endpoint, &probe_root, Duration::from_secs(1))
            .err()
            .expect("an insecure probe root must be refused");
        let E2eError::System {
            source: Some(source),
            ..
        } = error
        else {
            panic!("the probe root must fail its private-directory check");
        };
        assert!(matches!(
            source.downcast_ref::<gang_os::fs::PrivateDirError>(),
            Some(gang_os::fs::PrivateDirError::NotPrivate { mode, .. })
                if *mode == gang_os::fs::Mode::from_bits_retain(0o755)
        ));
        assert_eq!(
            fs::metadata(&probe_root)
                .expect("the probe root remains")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(
            fs::read_dir(&probe_root)
                .expect("probe root entries")
                .count(),
            0
        );
    }

    #[test]
    fn missing_or_invalid_session_fails_preparation() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let path = client_directory(cli.home()).join("session.json");
        for invalid in [b"not JSON".as_slice(), b"{}", b"null"] {
            fs::write(&path, invalid).expect("an invalid session is stored");
            let error = ColdHelpers::prepare(&cli, &endpoint, root.path(), Duration::from_secs(1))
                .err()
                .expect("the invalid stored session must be refused");
            assert!(matches!(
                error,
                E2eError::System { context, source: None }
                    if context.contains("invalid previous-client session")
            ));
        }
        fs::remove_file(path).expect("the session is absent");
        let error = ColdHelpers::prepare(&cli, &endpoint, root.path(), Duration::from_secs(1))
            .err()
            .expect("the missing stored session must be refused");
        assert!(matches!(
            error,
            E2eError::Io { source, .. } if source.kind() == ErrorKind::NotFound
        ));
    }

    #[test]
    fn mismatched_endpoint_and_missing_bound_proof_fail_preparation() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let other = "https://127.0.0.1:9443"
            .parse()
            .expect("a different endpoint");
        let error = ColdHelpers::prepare(&cli, &other, root.path(), Duration::from_secs(1))
            .err()
            .expect("the endpoint mismatch must be refused");
        assert!(matches!(
            error,
            E2eError::System { context, source: None }
                if context.contains("stored session server")
        ));
        fs::remove_file(cli.home().join("proof.jwt")).expect("the original proof is absent");
        let error = ColdHelpers::prepare(&cli, &endpoint, root.path(), Duration::from_secs(1))
            .err()
            .expect("the missing original bound proof must be refused");
        assert!(matches!(
            error,
            E2eError::System { context, source: None }
                if context.contains("bound workload proof file")
        ));
    }

    #[test]
    fn cache_created_after_preparation_cannot_be_accepted_as_a_cold_mint() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let pair = ColdHelpers::prepare(&cli, &endpoint, root.path(), Duration::from_secs(1))
            .expect("cold homes");
        fs::write(
            client_directory(pair.helpers[0].cli.home()).join("token"),
            "cached",
        )
        .expect("an intervening cache write");
        // No binary is needed: a warm home must be rejected before launching any helper.
        let error = pair
            .mint_pair()
            .expect_err("a warm home is not a cold probe");
        assert!(matches!(
            error,
            E2eError::System { context, source: None }
                if context.contains("probe 1") && context.contains("bearer cache")
        ));
    }

    #[test]
    fn actual_helper_response_shape_projects_only_the_bearer_token() {
        let answer = Ran {
            code: Some(0),
            stdout: r#"{"headers":{"authorization":["Bearer eyJhbGciOi.test-signature"],"x-extra":["ignored"]},"expires":"2030-01-01T00:00:00Z"}"#.to_owned(),
            stderr: String::new(),
        };
        assert_eq!(
            project_answer(&answer, 1).expect("the actual header shape"),
            "eyJhbGciOi.test-signature"
        );
    }

    #[test]
    fn invalid_or_failed_answers_never_disclose_helper_output() {
        let marker = "secret-output-marker";
        for stdout in [
            format!(r#"{{"headers":{{"x-secret":["{marker}"]}}}}"#),
            format!(r#"{{"headers":{{"authorization":["Basic {marker}"]}}}}"#),
            r#"{"headers":{"authorization":["Bearer "]}}"#.to_owned(),
            format!(r#"{{"headers":{{"authorization":["Bearer {marker}\r\ninjected"]}}}}"#),
            format!("invalid JSON {marker}"),
        ] {
            let answer = Ran {
                code: Some(0),
                stdout,
                stderr: marker.to_owned(),
            };
            let error = project_answer(&answer, 2).expect_err("invalid authorization is rejected");
            assert!(error.to_string().contains("probe 2"));
            assert!(!format!("{error:?}").contains(marker));
        }
        for code in [Some(1), None] {
            let answer = Ran {
                code,
                stdout: marker.to_owned(),
                stderr: marker.to_owned(),
            };
            let error = project_answer(&answer, 1).expect_err("the helper did not succeed");
            assert!(error.to_string().contains("probe 1"));
            assert!(!format!("{error:?}").contains(marker));
        }
    }

    #[test]
    fn failed_helper_classifies_only_the_published_status_position() {
        let secret = "secret-token-proof-session-body";
        let answer = Ran {
            code: Some(1),
            stdout: secret.to_owned(),
            stderr: format!("  fail  gang get: the server answered 503: {secret}\n{secret}"),
        };
        let error = project_answer(&answer, 1).expect_err("the original failure remains a failure");
        let rendered = format!("{error:?}");
        assert!(rendered.contains("helper_stage=server_response"));
        assert!(rendered.contains("helper_http_status=503"));
        assert!(rendered.contains("helper output withheld"));
        assert!(!rendered.contains(secret));
        for stderr in [
            secret.to_owned(),
            format!("  fail  gang get: {secret}\n  fail  gang get: the server answered 503: x"),
            format!("  fail  gang get: the server answered {secret}: 503"),
            "  fail  gang get: the server answered 5030: x".to_owned(),
            "  fail  gang get: the server answered 099: x".to_owned(),
            "  fail  gang get: the server answered 600: x".to_owned(),
            "  fail  gang get: the server answered 503".to_owned(),
            String::new(),
        ] {
            assert_eq!(
                helper_failure(&stderr),
                "helper_stage=unknown; helper_http_status=unknown"
            );
        }
    }

    #[test]
    fn drain_preparation_adds_one_private_cold_home_without_changing_original_or_pair() {
        let root = private_tempdir();
        let (cli, endpoint, session) = stored_client(root.path());
        let pair = ColdHelpers::prepare(&cli, &endpoint, root.path(), Duration::from_secs(4))
            .expect("original pair");
        let originals = [
            client_directory(cli.home()).join("session.json"),
            client_directory(cli.home()).join("token"),
            cli.home().join("proof.jwt"),
            cli.home().join(".bazelrc"),
        ];
        let snapshots = originals.each_ref().map(|path| {
            (
                fs::read(path).expect("original bytes"),
                fs::metadata(path)
                    .expect("original metadata")
                    .modified()
                    .expect("mtime"),
            )
        });
        let drain_root = root.path().join("drain-credentials");
        let drain = DrainCredentials::prepare(&cli, &endpoint, &drain_root, Duration::from_secs(4))
            .expect("missing private child is created");
        let home = drain.helper.cli.home();
        assert!(pair.helpers.iter().all(|helper| helper.cli.home() != home));
        assert_ne!(home, cli.home());
        assert_eq!(
            fs::read_dir(&drain_root).expect("one third home").count(),
            1
        );
        let store = client_directory(home);
        assert_eq!(fs::read_dir(&store).expect("session only").count(), 1);
        assert_eq!(
            fs::read(store.join("session.json")).expect("opaque copy"),
            session
        );
        assert!(!store.join("token").exists());
        for path in [drain_root.as_path(), home, &home.join(".config"), &store] {
            assert_eq!(
                fs::metadata(path)
                    .expect("private directory")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        assert_eq!(
            fs::metadata(store.join("session.json"))
                .expect("private session")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        for (path, (bytes, modified)) in originals.iter().zip(snapshots) {
            assert_eq!(fs::read(path).expect("unchanged original"), bytes);
            assert_eq!(
                fs::metadata(path)
                    .expect("unchanged metadata")
                    .modified()
                    .expect("mtime"),
                modified
            );
        }
        for helper in &pair.helpers {
            ensure_cold(helper.cli.home()).expect("original pair remains cold");
            assert_eq!(
                fs::read(client_directory(helper.cli.home()).join("session.json"))
                    .expect("pair session"),
                session
            );
        }
        let third_home = home.to_path_buf();
        drop(drain);
        assert!(!third_home.exists());
    }

    #[test]
    fn drain_request_reads_rotated_proof_with_canonical_source_and_wire_semantics() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let drain = DrainCredentials::prepare(&cli, &endpoint, root.path(), Duration::from_secs(4))
            .expect("prepared before proof rotation");
        fs::write(cli.home().join("proof.jwt"), " \nrotated-proof\t ").expect("rotated proof");
        let body = drain.authentication_body().expect("canonical request");
        let request: AuthenticateRequest = serde_json::from_slice(&body).expect("canonical DTO");
        assert_eq!(request.provider.as_str(), "test");
        let Credential::Proof(proof) = request.credential else {
            panic!("a workload file must produce a proof exchange");
        };
        assert_eq!(proof.expose(), "rotated-proof");
        let wire: serde_json::Value = serde_json::from_slice(&body).expect("wire object");
        assert_eq!(wire.as_object().expect("object").len(), 2);
        assert_eq!(wire["provider"], "test");
        assert_eq!(wire["proof"], "rotated-proof");
        assert!(wire.get("refreshToken").is_none());
    }

    #[test]
    fn drain_proof_errors_do_not_fall_back_to_original_cache_or_disclose_proof() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let drain = DrainCredentials::prepare(&cli, &endpoint, root.path(), Duration::from_secs(4))
            .expect("prepared");
        let proof = cli.home().join("proof.jwt");
        for invalid in [b" \n\t ".as_slice(), b"secret-proof\xff"] {
            fs::write(&proof, invalid).expect("invalid proof");
            let error = drain
                .authentication_body()
                .expect_err("proof read must fail");
            assert!(!format!("{error:?}").contains("secret-proof"));
        }
        fs::remove_file(proof).expect("proof removed after preparation");
        assert!(drain.authentication_body().is_err());
        assert_eq!(
            fs::read_to_string(client_directory(cli.home()).join("token")).expect("original cache"),
            "original cached bearer"
        );
    }

    struct ModeledDrain {
        now: Instant,
        elapsed: [Duration; 4],
        calls: Vec<(usize, Duration)>,
        verified: Vec<String>,
        expected_tokens: [&'static str; 2],
        failure: Option<usize>,
        shared: SharedAuthentication,
        stdout: &'static str,
    }

    impl ModeledDrain {
        fn new(elapsed: [Duration; 4]) -> Self {
            Self {
                now: Instant::now(),
                elapsed,
                calls: Vec::new(),
                verified: Vec::new(),
                expected_tokens: ["shared-token", "minted-token"],
                failure: None,
                shared: SharedAuthentication {
                    status: 200,
                    marker_absent: true,
                    token: "shared-token".to_owned(),
                    elapsed: elapsed[0],
                },
                stdout: r#"{"headers":{"authorization":["Bearer minted-token"]}}"#,
            }
        }

        fn complete(&mut self, phase: usize, deadline: Instant) -> Result<()> {
            self.calls.push((phase, remaining(deadline, self.now)?));
            self.now += self.elapsed[phase];
            if self.failure == Some(phase) {
                return Err(E2eError::system(
                    "modeled transport or authentication rejection",
                ));
            }
            Ok(())
        }

        fn observe(&mut self, budget: Duration) -> Result<DrainCredentialWitness> {
            observe_drain(self, Duration::from_secs(4), Duration::from_secs(4), budget)
        }
    }

    impl DrainSteps for ModeledDrain {
        fn now(&self) -> Instant {
            self.now
        }

        fn original_token(&self) -> &'static str {
            "original-cached-token"
        }

        fn authenticate(&mut self, deadline: Instant) -> Result<SharedAuthentication> {
            self.complete(0, deadline)?;
            Ok(SharedAuthentication {
                status: self.shared.status,
                marker_absent: self.shared.marker_absent,
                token: std::mem::take(&mut self.shared.token),
                elapsed: self.shared.elapsed,
            })
        }

        fn mint(&mut self, deadline: Instant) -> Result<String> {
            self.complete(2, deadline)?;
            project_answer(
                &Ran {
                    code: Some(0),
                    stdout: self.stdout.to_owned(),
                    stderr: "secret-helper-output".to_owned(),
                },
                3,
            )
        }

        fn verify(&mut self, token: String, deadline: Instant) -> Result<()> {
            let index = self.verified.len();
            self.complete(if index == 0 { 1 } else { 3 }, deadline)?;
            if self.expected_tokens.get(index).copied() != Some(token.as_str()) {
                return Err(E2eError::system("modeled bearer rejected by NLB"));
            }
            self.verified.push(token);
            Ok(())
        }
    }

    #[test]
    fn modeled_witness_uses_exact_tokens_and_all_raw_offsets() {
        let mut steps = ModeledDrain::new([
            Duration::from_secs(1),
            Duration::from_millis(500),
            Duration::from_secs(2),
            Duration::from_millis(250),
        ]);
        let witness = steps
            .observe(Duration::from_secs(5))
            .expect("all four modeled steps finish before the shared deadline");
        assert_eq!(
            steps.calls,
            vec![
                (0, Duration::from_secs(4)),
                (1, Duration::from_secs(4)),
                (2, Duration::from_millis(3500)),
                (3, Duration::from_millis(1500)),
            ]
        );
        assert_eq!(steps.verified, ["shared-token", "minted-token"]);
        assert_eq!(witness.shared_started, Duration::ZERO);
        assert_eq!(witness.shared_finished, Duration::from_secs(1));
        assert_eq!(witness.shared_verification_started, witness.shared_finished);
        assert_eq!(
            witness.shared_verification_finished,
            Duration::from_millis(1500)
        );
        assert_eq!(witness.mint_started, witness.shared_verification_finished);
        assert_eq!(witness.mint_finished, Duration::from_millis(3500));
        assert_eq!(witness.verification_started, witness.mint_finished);
        assert_eq!(witness.verification_finished, Duration::from_millis(3750));
        assert_eq!(witness.shared.status, 200);
        assert!(witness.shared.marker_absent && witness.shared.token_valid);
        assert!(witness.token_valid);
        let receipt = format!("{witness:?}");
        for secret in ["shared-token", "minted-token", "original-cached-token"] {
            assert!(!receipt.contains(secret));
        }
    }

    #[test]
    fn modeled_each_phase_enforces_its_cap_and_the_single_absolute_budget() {
        for phase in 0..4 {
            for elapsed in [Duration::from_secs(4), Duration::from_secs(5)] {
                let mut durations = [Duration::ZERO; 4];
                durations[phase] = elapsed;
                let mut steps = ModeledDrain::new(durations);
                assert!(steps.observe(Duration::from_secs(8)).is_err());
                assert_eq!(steps.calls.len(), phase + 1);
            }
        }
        let mut steps = ModeledDrain::new([Duration::from_secs(1); 4]);
        assert!(steps.observe(Duration::from_millis(3500)).is_err());
        assert_eq!(steps.calls.len(), 4);
        assert_eq!(steps.calls[3], (3, Duration::from_millis(500)));
        let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
        assert!(steps.observe(Duration::ZERO).is_err());
        assert!(steps.calls.is_empty());
        assert!(remaining(steps.now, steps.now).is_err());
    }

    #[test]
    fn modeled_network_and_old_cli_keep_their_independent_existing_caps() {
        let mut steps = ModeledDrain::new([
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_secs(3),
            Duration::from_millis(100),
        ]);
        observe_drain(
            &mut steps,
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(6),
        )
        .expect("mint retains its four-second cap while every network cap stays two seconds");
        assert_eq!(
            steps.calls,
            vec![
                (0, Duration::from_secs(2)),
                (1, Duration::from_secs(2)),
                (2, Duration::from_secs(4)),
                (3, Duration::from_secs(2)),
            ]
        );
    }

    #[test]
    fn modeled_reported_network_elapsed_cannot_override_deadlines() {
        for reported_late in [false, true] {
            let mut durations = [Duration::ZERO; 4];
            if !reported_late {
                durations[0] = Duration::from_secs(4);
            }
            let mut steps = ModeledDrain::new(durations);
            steps.shared.elapsed = if reported_late {
                Duration::from_secs(4)
            } else {
                Duration::ZERO
            };
            assert!(steps.observe(Duration::from_secs(8)).is_err());
            assert_eq!(steps.calls.len(), 1);
        }
    }

    #[test]
    fn modeled_transport_rejection_and_late_errors_are_terminal_without_retry() {
        for phase in 0..4 {
            for duration in [Duration::ZERO, Duration::from_secs(5)] {
                let mut durations = [Duration::ZERO; 4];
                durations[phase] = duration;
                let mut steps = ModeledDrain::new(durations);
                steps.failure = Some(phase);
                assert!(steps.observe(Duration::from_secs(8)).is_err());
                assert_eq!(steps.calls.len(), phase + 1);
            }
        }
    }

    #[test]
    fn modeled_false_shared_evidence_never_reaches_cold_mint() {
        let shared_mutations: [fn(&mut SharedAuthentication); 4] = [
            |s| s.status = 503,
            |s| s.status = 500,
            |s| s.marker_absent = false,
            |s| s.token.clear(),
        ];
        for mutate in shared_mutations {
            let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
            mutate(&mut steps.shared);
            assert!(steps.observe(Duration::from_secs(8)).is_err());
            assert_eq!(steps.calls.len(), 1);
        }
    }

    #[test]
    fn modeled_cached_or_unverified_tokens_and_helper_secrets_cannot_make_a_witness() {
        let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
        steps.shared.token = steps.original_token().to_owned();
        assert!(steps.observe(Duration::from_secs(8)).is_err());
        assert_eq!(steps.calls.len(), 1);
        let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
        steps.stdout = r#"{"headers":{"authorization":["Bearer original-cached-token"]}}"#;
        assert!(steps.observe(Duration::from_secs(8)).is_err());
        assert_eq!(steps.calls.len(), 3);
        for phase in [0, 1] {
            let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
            steps.expected_tokens[phase] = "different-accepted-token";
            assert!(steps.observe(Duration::from_secs(8)).is_err());
            assert_eq!(steps.calls.len(), if phase == 0 { 2 } else { 4 });
        }
        let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
        steps.stdout = r#"{"headers":{"authorization":["Bearer secret-invalid\r\n"]}}"#;
        let error = steps
            .observe(Duration::from_secs(8))
            .expect_err("invalid helper output must never reach verification");
        assert_eq!(steps.calls.len(), 3);
        assert!(!format!("{error:?}").contains("secret-"));
    }

    #[test]
    fn modeled_equal_fresh_bearers_still_require_cold_mint_and_both_verifications() {
        let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
        steps.stdout = r#"{"headers":{"authorization":["Bearer shared-token"]}}"#;
        steps.expected_tokens[1] = "shared-token";
        let witness = steps
            .observe(Duration::from_secs(8))
            .expect("independent mints can produce identical fresh bearers");
        assert_eq!(
            steps.calls,
            vec![
                (0, Duration::from_secs(4)),
                (1, Duration::from_secs(4)),
                (2, Duration::from_secs(4)),
                (3, Duration::from_secs(4)),
            ]
        );
        assert_eq!(steps.verified, ["shared-token", "shared-token"]);
        assert!(witness.shared.token_valid && witness.token_valid);
    }

    #[test]
    fn modeled_zero_caps_never_run_the_disallowed_phase() {
        for (probe, mint, count) in [
            (Duration::ZERO, Duration::from_secs(4), 0),
            (Duration::from_secs(4), Duration::ZERO, 2),
        ] {
            let mut steps = ModeledDrain::new([Duration::ZERO; 4]);
            assert!(observe_drain(&mut steps, probe, mint, Duration::from_secs(8),).is_err());
            assert_eq!(steps.calls.len(), count);
        }
    }

    #[test]
    fn consuming_witness_cleans_third_home_on_preflight_and_proof_errors() {
        for failure in 0..3 {
            let root = private_tempdir();
            let (cli, endpoint, _) = stored_client(root.path());
            let prepared =
                DrainCredentials::prepare(&cli, &endpoint, root.path(), Duration::from_secs(4))
                    .expect("prepared cold");
            let home = prepared.helper.cli.home().to_path_buf();
            let originals = [
                client_directory(cli.home()).join("session.json"),
                client_directory(cli.home()).join("token"),
                cli.home().join(".bazelrc"),
            ];
            let snapshots = originals
                .each_ref()
                .map(|path| fs::read(path).expect("original"));
            if failure == 2 {
                fs::write(cli.home().join("proof.jwt"), " \n\t ").expect("invalid current proof");
            }
            let credentials = Credentials {
                principal: "test:principal".to_owned(),
                home: cli.home().to_path_buf(),
                token: "original-cached-token".to_owned(),
                authority: None,
            };
            let error = prepared
                .witness(
                    DirectTarget {
                        address: "127.0.0.1:8443".parse().expect("literal address"),
                        proxy_protocol_v2: false,
                    },
                    &credentials,
                    if failure == 0 {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(4)
                    },
                    if failure == 1 {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(8)
                    },
                )
                .expect_err("preflight and invalid proof errors precede all network or CLI work");
            assert!(!home.exists());
            assert!(!format!("{error:?}").contains("original-cached-token"));
            for (path, bytes) in originals.iter().zip(snapshots) {
                assert_eq!(fs::read(path).expect("unchanged original"), bytes);
            }
        }
    }

    #[test]
    fn drain_home_warmed_after_preparation_fails_before_running_old_cli() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let prepared =
            DrainCredentials::prepare(&cli, &endpoint, root.path(), Duration::from_secs(4))
                .expect("prepared cold");
        fs::write(
            client_directory(prepared.helper.cli.home()).join("token"),
            "intervening-cache",
        )
        .expect("intervening cache");
        let credentials = Credentials {
            principal: "test:principal".to_owned(),
            home: cli.home().to_path_buf(),
            token: "original-token".to_owned(),
            authority: None,
        };
        let mut attempt = DrainAttempt {
            prepared,
            target: DirectTarget {
                address: "127.0.0.1:8443".parse().expect("literal address"),
                proxy_protocol_v2: false,
            },
            credentials: &credentials,
        };
        let error = attempt
            .mint(Instant::now() + Duration::from_secs(4))
            .expect_err("a warm helper must fail before the nonexistent old binary is launched");
        assert!(error.to_string().contains("bearer cache"));
        let home = attempt.prepared.helper.cli.home().to_path_buf();
        drop(attempt);
        assert!(!home.exists());
        assert_eq!(
            fs::read_to_string(client_directory(cli.home()).join("token")).expect("original cache"),
            "original cached bearer"
        );
    }

    #[test]
    fn drain_insecure_root_is_refused_without_modifying_its_mode() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let probes = root.path().join("public-root");
        fs::create_dir(&probes).expect("probe root");
        fs::set_permissions(&probes, fs::Permissions::from_mode(0o755)).expect("public mode");
        assert!(
            DrainCredentials::prepare(&cli, &endpoint, &probes, Duration::from_secs(4)).is_err()
        );
        assert_eq!(
            fs::metadata(&probes)
                .expect("unmodified root")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(fs::read_dir(probes).expect("no helper home").count(), 0);
    }
}
