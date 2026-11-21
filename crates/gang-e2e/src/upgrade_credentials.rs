//! Previous-client cold mints, unchanged warm sessions, and canonical workload proofs.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use gang_client::{ClientOptions, ProofSource};
use gang_clock::SystemClock;
use gang_types::{CaPin, ProviderName, Url};
use gang_wire::{AuthenticateRequest, Credential};
use serde::Deserialize;
use tempfile::TempDir;

use crate::errors::{E2eError, Result};
use crate::gang_cli::GangCli;
use crate::process::{self, Ran};
use crate::secured;

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
    /// Prepares both isolated homes before starting the replacement controller.
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
    /// The availability monitor calls this while original work is held and the successor is healthy,
    /// without interleaving other probes. Its clock keeps running throughout both commands. A pair
    /// does not by itself witness which backend served either mint.
    pub(crate) fn mint_pair(self) -> Result<[String; 2]> {
        Ok([self.mint(0)?, self.mint(1)?])
    }

    /// Captures the opaque original session without logging in or rewriting its bound proof.
    pub(crate) fn session_bytes(cli: &GangCli, endpoint: &Url) -> Result<Vec<u8>> {
        let bytes = fs::read(client_directory(cli.home()).join("session.json"))
            .map_err(|_| E2eError::system("original workload session unavailable"))?;
        validate_session(&bytes, endpoint)?;
        Ok(bytes)
    }

    /// Reuses the actual old helper home, rejecting any replacement of its original session.
    pub(crate) fn warm_token(
        cli: &GangCli,
        endpoint: &Url,
        original_session: &[u8],
        timeout: Duration,
    ) -> Result<String> {
        if Self::session_bytes(cli, endpoint)? != original_session {
            return Err(E2eError::system("original warm workload session changed"));
        }
        let request = serde_json::json!({ "uri": endpoint.as_str() }).to_string();
        let answer = process::run(cli.get(), Some(request.as_bytes()), timeout)?;
        if !answer.succeeded() {
            return Err(E2eError::system(
                "original warm gang get failed; helper output withheld to protect credentials",
            ));
        }
        let token = secured::bearer_of(&answer.stdout).map_err(|_| {
            E2eError::system("original warm gang get returned no bearer authorization")
        })?;
        if Self::session_bytes(cli, endpoint)? != original_session {
            return Err(E2eError::system(
                "original warm gang get replaced its session",
            ));
        }
        Ok(token)
    }

    /// Builds a canonical proof request from the original session and current bound proof.
    pub(crate) fn proof_request(cli: &GangCli, endpoint: &Url) -> Result<Bytes> {
        let bytes = fs::read(client_directory(cli.home()).join("session.json"))
            .map_err(|_| E2eError::system("original workload session unavailable"))?;
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
    fn warm_reuse_rejects_a_replaced_session_before_running_the_old_helper() {
        let root = private_tempdir();
        let (cli, endpoint, session) = stored_client(root.path());
        assert_eq!(
            ColdHelpers::session_bytes(&cli, &endpoint).expect("original session is readable"),
            session,
        );
        let mut changed = session.clone();
        changed.push(b'\n');
        fs::write(client_directory(cli.home()).join("session.json"), changed)
            .expect("replace session without changing its parsed content");
        let error = ColdHelpers::warm_token(&cli, &endpoint, &session, Duration::from_secs(1))
            .expect_err("even a byte-level session replacement must fail before helper execution");
        assert!(
            error
                .to_string()
                .contains("original warm workload session changed")
        );
        assert_eq!(
            fs::read_to_string(cli.home().join("proof.jwt")).expect("original proof remains"),
            "original proof with unchanged lifetime",
        );
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
    fn refusal_request_reads_rotated_proof_with_canonical_wire_semantics() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        fs::write(cli.home().join("proof.jwt"), " \nrotated-proof\t ").expect("rotated proof");
        let body = ColdHelpers::proof_request(&cli, &endpoint).expect("canonical request");
        let request: AuthenticateRequest = serde_json::from_slice(&body).expect("canonical DTO");
        assert_eq!(request.provider.as_str(), "test");
        let Credential::Proof(proof) = request.credential else {
            panic!("a workload file must produce a proof exchange");
        };
        assert_eq!(proof.expose(), "rotated-proof");
        let wire: serde_json::Value = serde_json::from_slice(&body).expect("wire object");
        assert_eq!(wire.as_object().expect("object").len(), 2);
        assert_eq!(wire["proof"], "rotated-proof");
    }

    #[test]
    fn refusal_proof_errors_do_not_fall_back_to_cached_bearer() {
        let root = private_tempdir();
        let (cli, endpoint, _) = stored_client(root.path());
        let proof = cli.home().join("proof.jwt");
        for invalid in [b" \n\t ".as_slice(), b"secret-proof\xff"] {
            fs::write(&proof, invalid).expect("invalid proof");
            let error = ColdHelpers::proof_request(&cli, &endpoint).expect_err("invalid proof");
            assert!(!format!("{error:?}").contains("secret-proof"));
        }
        fs::remove_file(proof).expect("proof removed");
        assert!(ColdHelpers::proof_request(&cli, &endpoint).is_err());
        assert_eq!(
            fs::read_to_string(client_directory(cli.home()).join("token")).expect("original cache"),
            "original cached bearer"
        );
    }
}
