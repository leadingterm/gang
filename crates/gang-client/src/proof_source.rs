//! Where a workload login reads its identity-provider proof from: a command's standard output,
//! or a file.

use std::io;
use std::path::PathBuf;

use gang_clock::Clock;
use gang_os::PathIo;
use gang_types::Proof;
use serde::{Deserialize, Serialize};

use crate::{ClientError, ClientOptions};

/// Where a workload proof is read from. Read again for every token that is minted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProofSource {
    /// The standard output of a program, run directly without a shell.
    Command {
        /// The program followed by its arguments.
        argv: Vec<String>,
    },
    /// A file, read whole.
    File {
        /// The file holding the proof.
        path: PathBuf,
    },
}

impl ProofSource {
    /// Reads the proof. Surrounding whitespace is removed.
    ///
    /// # Errors
    ///
    /// Returns a command error if the program cannot run, fails, times out, exceeds the output
    /// limit, or produces non-UTF-8 output; [`ClientError::EmptyToken`] when nothing is left
    /// after trimming; and [`ClientError::Io`] when the file cannot be read.
    pub async fn read(
        &self,
        clock: &impl Clock,
        options: &ClientOptions,
    ) -> Result<Proof, ClientError> {
        let text = match self {
            Self::Command { argv } => run(argv, clock, options).await?,
            Self::File { path } => {
                std::fs::read_to_string(path).map_err(PathIo::at("read", path))?
            }
        };
        proof_from(&text)
    }
}

/// Runs `argv` without a shell and returns its standard output.
async fn run(
    argv: &[String],
    clock: &impl Clock,
    options: &ClientOptions,
) -> Result<String, ClientError> {
    let command = argv.join(" ");
    let Some((program, arguments)) = argv.split_first() else {
        return Err(ClientError::TokenCommand {
            command,
            source: io::Error::new(io::ErrorKind::InvalidInput, "no program given"),
        });
    };
    let mut process = tokio::process::Command::new(program);
    process.args(arguments);
    let timeout = options.proof_command_timeout.get();
    let output = clock
        .timeout(
            timeout,
            gang_os::run_bounded(&mut process, None, options.proof_command_max_output),
        )
        .await
        .map_err(|_| ClientError::TokenCommandTimedOut {
            command: command.clone(),
            seconds: timeout.as_secs(),
        })?
        .map_err(|error| match error {
            gang_os::RunError::Spawn { source, .. } | gang_os::RunError::Io { source, .. } => {
                ClientError::TokenCommand {
                    command: command.clone(),
                    source,
                }
            }
            source @ gang_os::RunError::StdoutTooLarge { .. } => {
                ClientError::TokenCommandOutputTooLarge {
                    command: command.clone(),
                    source,
                }
            }
        })?;
    if !output.status.success() {
        return Err(ClientError::TokenCommandFailed {
            command,
            status: output.status.to_string(),
        });
    }
    String::from_utf8(output.stdout).map_err(|error| ClientError::TokenCommand {
        command,
        source: io::Error::new(io::ErrorKind::InvalidData, error),
    })
}

/// Trims `text` and wraps it as a proof.
fn proof_from(text: &str) -> Result<Proof, ClientError> {
    let token = text.trim();
    if token.is_empty() {
        return Err(ClientError::EmptyToken);
    }
    Ok(Proof::new(token))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use gang_clock::TestClock;
    use tempfile::TempDir;

    use super::ProofSource;
    use crate::{ClientError, ClientOptions};

    #[tokio::test]
    async fn file_is_read_and_trimmed() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("proof");
        std::fs::write(&path, "  oidc-proof \n").expect("write fixture");

        let proof = ProofSource::File { path }
            .read(&TestClock::default(), &ClientOptions::default())
            .await
            .expect("file is readable");

        assert_eq!(proof.expose(), "oidc-proof");
    }

    #[tokio::test]
    async fn empty_file_is_an_empty_token() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("proof");
        std::fs::write(&path, " \n\t").expect("write fixture");

        let result = ProofSource::File { path }
            .read(&TestClock::default(), &ClientOptions::default())
            .await;

        assert!(matches!(result, Err(ClientError::EmptyToken)));
    }

    #[tokio::test]
    async fn missing_file_is_an_io_error() {
        let tmp = TempDir::new().expect("temp dir");
        let path = tmp.path().join("absent");

        let result = ProofSource::File { path: path.clone() }
            .read(&TestClock::default(), &ClientOptions::default())
            .await;

        assert!(matches!(
            &result,
            Err(ClientError::Io(failed)) if failed.action == "read" && failed.path == path
        ));
    }

    #[tokio::test]
    async fn hanging_proof_command_times_out() {
        let helper = std::env::var_os("FAKE_CREDENTIAL_HELPER")
            .expect("FAKE_CREDENTIAL_HELPER must be set by the Bazel test_env");
        let helper = std::env::current_dir()
            .expect("a working directory")
            .join(helper);
        let source = ProofSource::Command {
            argv: vec![
                helper
                    .to_str()
                    .expect("runfiles paths are UTF-8")
                    .to_owned(),
                "hang".to_owned(),
            ],
        };
        let clock = TestClock::default();
        let options = ClientOptions::default();
        let future = source.read(&clock, &options);
        tokio::pin!(future);
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(future.as_mut().poll(cx).is_pending())
        })
        .await;
        assert!(pending);
        clock.advance(options.proof_command_timeout.get() + Duration::from_secs(1));
        assert!(matches!(
            future.await,
            Err(ClientError::TokenCommandTimedOut { seconds: 10, .. })
        ));
    }

    #[test]
    fn command_round_trips_through_json() {
        let source = ProofSource::Command {
            argv: vec![
                "fetch-proof".to_owned(),
                "--audience".to_owned(),
                "gang".to_owned(),
            ],
        };

        let text = serde_json::to_string(&source).expect("source serializes");
        let parsed: ProofSource = serde_json::from_str(&text).expect("source deserializes");

        assert_eq!(
            text,
            r#"{"kind":"command","argv":["fetch-proof","--audience","gang"]}"#
        );
        assert_eq!(parsed, source);
    }

    #[test]
    fn file_round_trips_through_json() {
        let source = ProofSource::File {
            path: PathBuf::from("/run/secrets/proof"),
        };

        let text = serde_json::to_string(&source).expect("source serializes");
        let parsed: ProofSource = serde_json::from_str(&text).expect("source deserializes");

        assert_eq!(text, r#"{"kind":"file","path":"/run/secrets/proof"}"#);
        assert_eq!(parsed, source);
    }
}
