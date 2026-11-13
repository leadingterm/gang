//! The command-line grammar of the `gang` binary: clap definitions and the typed argument structs
//! that the handlers consume.

use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Arg, ArgGroup, ArgMatches, Args, FromArgMatches, Parser, Subcommand};
use gang_client::ProofSource;
use gang_deploy::{
    MaxAge, PlanPrefix, PlanVersion, PrNumber, Repository, RepositoryId, RunAttempt, RunId, Sha,
    UnixTime,
};
use gang_types::{CaPin, CertSerial, Identity, NonZeroDuration, ProviderName, Url};

use crate::errors::{MainError, Result};

/// Parses a duration such as `24h` into the plan store's maximum age.
fn max_age(text: &str) -> std::result::Result<MaxAge, Box<dyn std::error::Error + Send + Sync>> {
    Ok(MaxAge::try_from(gang_types::duration::parse(text)?)?)
}

/// Default time a `gang local up` waits for the server to print its CA pin.
pub(crate) const DEFAULT_STARTUP_TIMEOUT: &str = "30s";
/// Default time a `gang local down` waits for the server to exit.
pub(crate) const DEFAULT_STOP_TIMEOUT: &str = "30s";
/// Default interval between two checks of a local server's progress.
pub(crate) const DEFAULT_POLL_INTERVAL: &str = "100ms";
/// Default command Bazel runs as the credential helper: the `gang` binary found on `PATH`.
pub(crate) const DEFAULT_CREDENTIAL_HELPER: &str = "gang";
/// Default Bazel program `gang doctor` asks for its effective options: the `bazel` on `PATH`.
pub(crate) const DEFAULT_BAZEL_PROGRAM: &str = "bazel";

/// Builds an `after_help` text: an `Examples:` heading, then each comment followed by its command lines.
/// A continuation line is an entry whose text carries the extra indentation.
macro_rules! examples {
    ($($comment:literal => [$($line:literal),+ $(,)?];)+) => {
        concat!("Examples:", $("\n  # ", $comment, $("\n  ", $line),+),+)
    };
}

/// The root command's `after_help`.
const ROOT_EXAMPLES: &str = examples! {
    "a laptop: log in, then point Bazel at the farm" => [
        "gang login --server https://gang.example.com",
        "gang bazelrc --server https://gang.example.com > .bazelrc",
    ];
    "a CI job: log in with the job's own token" => [
        "gang login --server https://gang.example.com --workload \\",
        "  --provider github-actions --token-command ./ci/job-token",
    ];
};
/// The `after_help` of `gang login`.
const LOGIN_EXAMPLES: &str = examples! {
    "a laptop: opens the browser" => [
        "gang login --server https://gang.example.com",
    ];
    "a CI job" => [
        "gang login --server https://gang.example.com --workload \\",
        "  --provider github-actions --token-command ./ci/job-token",
    ];
};
/// The `after_help` of `gang get`.
const GET_EXAMPLES: &str = examples! {
    "a laptop: ask by hand what Bazel would ask" => [
        "echo '{\"uri\":\"https://gang.example.com/\"}' | gang get",
    ];
    "a CI job: Bazel runs it, as set up by gang bazelrc" => [
        "build --credential_helper=gang.example.com=gang",
    ];
};
/// The `after_help` of `gang bazelrc`.
const BAZELRC_EXAMPLES: &str = examples! {
    "a laptop: write the settings next to your Bazel workspace" => [
        "gang bazelrc --server https://gang.example.com > .bazelrc",
    ];
    "a CI job: pin the server's certificate authority" => [
        "gang bazelrc --server https://gang.example.com --ca-pin sha256:<hex> > .bazelrc",
    ];
};
/// The `after_help` of `gang revoke`.
const REVOKE_EXAMPLES: &str = examples! {
    "a laptop: revoke one certificate by its serial" => [
        "gang revoke --serial 3f9a1c",
    ];
    "a CI job: revoke everything a workload principal holds" => [
        "gang revoke --principal github-actions:acme/app",
    ];
};
/// The `after_help` of `gang restore`.
const RESTORE_EXAMPLES: &str = examples! {
    "a laptop: let a person log in again" => [
        "gang restore --principal google:alice@example.com",
    ];
    "a CI job: let a workload log in again" => [
        "gang restore --principal github-actions:acme/app",
    ];
};
/// The `after_help` of `gang server`.
const SERVER_EXAMPLES: &str = examples! {
    "a laptop: run a server from a configuration file" => [
        "gang server --config gang.toml",
    ];
    "a CI job or controller host: the installed configuration" => [
        "gang server --config /etc/gang/gang.toml",
    ];
};
/// The `after_help` of `gang ca-pin`.
const CA_PIN_EXAMPLES: &str = examples! {
    "a laptop or CI job: the pin of the controller's signer" => [
        "gang ca-pin --config gang.toml",
    ];
    "a controller host: the installed configuration" => [
        "gang ca-pin --config /etc/gang/gang.toml",
    ];
};
/// The `after_help` of `gang worker`.
const WORKER_EXAMPLES: &str = examples! {
    "a worker machine: started by the server's fleet" => [
        "gang worker --config /etc/gang/gang.toml",
    ];
    "a CI job: with the endpoints the server resolved" => [
        "gang worker --config worker.toml --endpoint https://gang.example.com \\",
        "  --worker-endpoint grpcs://gang.example.com:8443",
    ];
};
/// The `after_help` of `gang exec`.
const EXEC_EXAMPLES: &str = examples! {
    "a laptop: try the sandbox entry point by hand" => [
        "gang exec -- sh -c 'echo hello'",
    ];
    "a CI job: the worker runs every action through the gang-exec link" => [
        "gang-exec sh -c 'echo hello'",
    ];
};
/// The `after_help` of `gang deploy`.
const DEPLOY_EXAMPLES: &str = examples! {
    "a deploy job: decide whether a saved plan may merge" => [
        "tofu show -json infra.tfplan | gang deploy policy >> \"$GITHUB_OUTPUT\"",
    ];
    "a deploy job: check the stored plan object against the manifest" => [
        "aws s3api head-object ... | gang deploy check-object --manifest manifest.json \\",
        "  --now \"$(date -u +%s)\" --max-age 24h",
    ];
};
/// The `after_help` of `gang deploy policy`.
const DEPLOY_POLICY_EXAMPLES: &str = examples! {
    "a plan job: record the verdict as step outputs; exit 3 blocks the merge" => [
        "gang deploy policy < infra-plan.json >> \"$GITHUB_OUTPUT\"",
    ];
};
/// The `after_help` of `gang deploy restart-needed`.
const DEPLOY_RESTART_NEEDED_EXAMPLES: &str = examples! {
    "an apply job: does the controller reload its configuration?" => [
        "gang deploy restart-needed < infra-plan.json",
    ];
};
/// The `after_help` of `gang deploy store-request`.
const DEPLOY_STORE_REQUEST_EXAMPLES: &str = examples! {
    "a plan job: the key and metadata of the plan to upload" => [
        "gang deploy store-request --repository example-org/infra --repository-id 42 \\",
        "  --pr-number 7 --pr-head-sha <sha> --sha <sha> --head <sha> --parent <base> \\",
        "  --parent <head> --tree-sha <sha> --run-id 99 --run-attempt 1 --prefix plans \\",
        "  --max-age 24h --now \"$(date -u +%s)\" --plan-file infra.tfplan \\",
        "  < infra-plan.json > store-request.json",
    ];
};
/// The `after_help` of `gang deploy manifest`.
const DEPLOY_MANIFEST_EXAMPLES: &str = examples! {
    "a plan job: the manifest once the plan store returned the version" => [
        "gang deploy manifest --plan-version <version> < store-request.json > manifest.json",
    ];
};
/// The `after_help` of `gang deploy comment`.
const DEPLOY_COMMENT_EXAMPLES: &str = examples! {
    "a plan job: the comment payload of a stored plan" => [
        "gang deploy comment --manifest manifest.json < infra-plan.json > infra-comment.json",
    ];
    "a plan job: the plan did not complete" => [
        "gang deploy comment --tree \"$(git rev-parse 'HEAD^{tree}')\" < /dev/null",
    ];
};
/// The `after_help` of `gang deploy comment-id`.
const DEPLOY_COMMENT_ID_EXAMPLES: &str = examples! {
    "a plan job: the comment a new plan replaces" => [
        "gang deploy comment-id < infra-comments.json",
    ];
};
/// The `after_help` of `gang deploy merged-pr`.
const DEPLOY_MERGED_PR_EXAMPLES: &str = examples! {
    "an apply job: the pull request merged as this commit" => [
        "gang deploy merged-pr --sha \"$GITHUB_SHA\" --repository \"$GITHUB_REPOSITORY\" < pulls.json",
    ];
};
/// The `after_help` of `gang deploy plan-comment`.
const DEPLOY_PLAN_COMMENT_EXAMPLES: &str = examples! {
    "an apply job: the manifest the plan comment carries" => [
        "gang deploy plan-comment < infra-comments.json",
    ];
};
/// The `after_help` of `gang deploy workflow-file`.
const DEPLOY_WORKFLOW_FILE_EXAMPLES: &str = examples! {
    "an apply job: the running workflow's file name" => [
        "gang deploy workflow-file --ref \"$GITHUB_WORKFLOW_REF\"",
    ];
};
/// The `after_help` of `gang deploy resolve`.
const DEPLOY_RESOLVE_EXAMPLES: &str = examples! {
    "an apply job: the reviewed plan of the merge on main" => [
        "gang deploy resolve --ref \"$GITHUB_REF\" --head <sha> --sha \"$GITHUB_SHA\" \\",
        "  --parent <base> --tree-sha <sha> --repository \"$GITHUB_REPOSITORY\" \\",
        "  --repository-id \"$GITHUB_REPOSITORY_ID\" --prefix plans \\",
        "  --workflow-ref \"$GITHUB_WORKFLOW_REF\" < evidence.json > manifest.json",
    ];
};
/// The `after_help` of `gang deploy check-versions`.
const DEPLOY_CHECK_VERSIONS_EXAMPLES: &str = examples! {
    "an apply job: the plan object has exactly the manifest's version" => [
        "aws s3api list-object-versions ... | gang deploy check-versions --manifest manifest.json",
    ];
};
/// The `after_help` of `gang deploy check-object`.
const DEPLOY_CHECK_OBJECT_EXAMPLES: &str = examples! {
    "an apply job: the plan object matches the manifest and is fresh" => [
        "aws s3api head-object ... | gang deploy check-object --manifest manifest.json \\",
        "  --now \"$(date -u +%s)\" --max-age 24h",
    ];
};
/// The `after_help` of `gang deploy check-plan`.
const DEPLOY_CHECK_PLAN_EXAMPLES: &str = examples! {
    "an apply job: the downloaded plan is the reviewed one" => [
        "gang deploy check-plan --manifest manifest.json --plan-file infra.tfplan",
    ];
};
/// The `after_help` of `gang options`.
const OPTIONS_EXAMPLES: &str = examples! {
    "a laptop: every provisioner" => [
        "gang options",
    ];
    "a CI job: one provisioner, to check a configuration" => [
        "gang options aws",
    ];
};
/// The `after_help` of `gang doctor`.
const DOCTOR_EXAMPLES: &str = examples! {
    "a laptop: check that Bazel reaches the server and can log in" => [
        "gang doctor --server https://gang.example.com",
    ];
    "a CI job: the same check, printing problems only" => [
        "gang doctor -q --server https://gang.example.com",
    ];
};
/// The `after_help` of `gang local`.
const LOCAL_EXAMPLES: &str = examples! {
    "a laptop: start and stop the local farm" => [
        "gang local up",
        "gang local down",
    ];
    "a CI job: a configuration of the job's own" => [
        "gang local up --config ci/gang.local.toml",
    ];
};
/// The `after_help` of `gang local up`.
const LOCAL_UP_EXAMPLES: &str = examples! {
    "a laptop: start from the workspace's local configuration" => [
        "gang local up",
    ];
    "a CI job: a configuration of the job's own, with a longer wait" => [
        "gang local up --config ci/gang.local.toml --startup-timeout 2m",
    ];
};
/// The `after_help` of `gang local down`.
const LOCAL_DOWN_EXAMPLES: &str = examples! {
    "a laptop: stop the workspace's local server" => [
        "gang local down",
    ];
    "a CI job: a configuration of the job's own, with a longer wait" => [
        "gang local down --config ci/gang.local.toml --stop-timeout 1m",
    ];
};

/// Command-line interface of the gang binary.
#[derive(Debug, Parser)]
#[command(
    name = "gang",
    version,
    about = "Bazel remote execution on workers that exist only while there is work.",
    after_help = ROOT_EXAMPLES
)]
pub struct Cli {
    /// Print results and problems only.
    #[arg(short, long, global = true, display_order = 100)]
    pub quiet: bool,
    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands of the gang binary.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Log in to a Gang server and store the client identity.
    #[command(override_usage = "gang login --server <URL> [OPTIONS]", after_help = LOGIN_EXAMPLES)]
    Login(LoginArgs),
    /// Answer one Bazel credential request read from standard input, printing the reply.
    #[command(hide = true, after_help = GET_EXAMPLES)]
    Get,
    /// Print the Bazel settings of a Gang server.
    #[command(after_help = BAZELRC_EXAMPLES)]
    Bazelrc(BazelrcArgs),
    /// Revoke one certificate, or one principal.
    #[command(after_help = REVOKE_EXAMPLES)]
    Revoke(RevokeArgs),
    /// Restore a principal that was revoked.
    #[command(after_help = RESTORE_EXAMPLES)]
    Restore(RestoreArgs),
    /// Run the Gang server.
    #[command(after_help = SERVER_EXAMPLES)]
    Server(ConfigArgs),
    /// Run a Gang worker.
    #[command(hide = true, after_help = WORKER_EXAMPLES)]
    Worker(WorkerArgs),
    /// Run a command in the sandbox; used through the sandbox entry point, not by people.
    #[command(hide = true, after_help = EXEC_EXAMPLES)]
    Exec(ExecArgs),
    /// Run a Gang deployment on this machine.
    #[command(subcommand, after_help = LOCAL_EXAMPLES)]
    Local(LocalCommand),
    /// List the options each provisioner accepts in `[fleets.options]`.
    #[command(after_help = OPTIONS_EXAMPLES)]
    Options(OptionsArgs),
    /// Check that Bazel reaches a Gang server and can log in.
    #[command(after_help = DOCTOR_EXAMPLES)]
    Doctor(DoctorArgs),
    /// Print the CA pin of the configured signer.
    #[command(after_help = CA_PIN_EXAMPLES)]
    CaPin(ConfigArgs),
    /// Decide one step of the deploy workflow.
    #[command(subcommand, hide = true, after_help = DEPLOY_EXAMPLES)]
    Deploy(DeployCommand),
}

impl Command {
    /// The name an error of this command is reported under.
    pub(crate) fn subject(&self) -> &'static str {
        match self {
            Self::Login(_) => "gang login",
            Self::Get => "gang get",
            Self::Bazelrc(_) => "gang bazelrc",
            Self::Revoke(_) => "gang revoke",
            Self::Restore(_) => "gang restore",
            Self::Server(_) => "gang server",
            Self::Worker(_) => "gang worker",
            Self::Exec(_) => "gang exec",
            Self::Local(LocalCommand::Up(_)) => "gang local up",
            Self::Local(LocalCommand::Down(_)) => "gang local down",
            Self::Options(_) => "gang options",
            Self::Doctor(_) => "gang doctor",
            Self::CaPin(_) => "gang ca-pin",
            Self::Deploy(command) => command.subject(),
        }
    }
}

/// Arguments of `gang options`.
#[derive(Debug, Args)]
pub struct OptionsArgs {
    /// Only this provisioner's options; all provisioners when omitted.
    pub provisioner: Option<String>,
}

/// Arguments of `gang doctor`.
#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Base URL of the Gang server; defaults to the server of the stored login.
    #[arg(long, value_name = "URL")]
    pub server: Option<Url>,
    /// SHA-256 pin of the server's certificate authority; defaults to the stored login's pin when
    /// the server is taken from that login.
    #[arg(long, value_name = "SHA256")]
    pub ca_pin: Option<CaPin>,
    /// Bazel program asked for its effective options.
    #[arg(long, value_name = "PROGRAM", default_value = DEFAULT_BAZEL_PROGRAM)]
    pub bazel: OsString,
    /// Arguments of the Bazel invocation, given after `--`, for example `-- --config=ci`.
    #[arg(last = true, value_name = "BAZEL_ARG", allow_hyphen_values = true)]
    pub bazel_args: Vec<OsString>,
}

/// The `local` subcommands.
#[derive(Debug, Subcommand)]
pub enum LocalCommand {
    /// Start the local Gang server.
    #[command(after_help = LOCAL_UP_EXAMPLES)]
    Up(LocalUpArgs),
    /// Stop the local Gang server and terminate its workers.
    #[command(after_help = LOCAL_DOWN_EXAMPLES)]
    Down(LocalDownArgs),
}

/// Arguments of `gang login`.
#[derive(Debug, Args)]
pub struct LoginArgs {
    /// Base URL of the Gang server.
    #[arg(long, value_name = "URL")]
    pub server: Url,
    /// SHA-256 pin of the server's certificate authority; the system roots are trusted when omitted.
    #[arg(long, value_name = "SHA256")]
    pub ca_pin: Option<CaPin>,
    /// Identity provider to log in with.
    #[arg(long, value_name = "NAME")]
    pub provider: Option<ProviderName>,
    /// Log in with a workload proof instead of an interactive browser session.
    #[arg(long, requires_all = ["provider", "proof"])]
    pub workload: bool,
    /// Where the workload proof comes from.
    #[command(flatten)]
    pub proof: ProofArgs,
}

/// Where a workload login reads its proof from: exactly one of a file or a command.
#[derive(Debug, Args)]
#[group(id = "proof", multiple = false)]
pub struct ProofArgs {
    /// File that holds the workload proof.
    #[arg(long, value_name = "FILE", requires = "workload")]
    pub token: Option<PathBuf>,
    /// Command that prints the workload proof on standard output. It takes every argument after
    /// it, so it must be the last option on the command line.
    #[arg(
        long,
        value_name = "ARGV",
        num_args = 1..,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        requires = "workload"
    )]
    pub token_command: Option<Vec<String>>,
}

impl ProofArgs {
    /// Builds the proof source; exactly one of a file or a command.
    fn source(self) -> Result<ProofSource> {
        match (self.token, self.token_command) {
            (Some(path), None) => Ok(ProofSource::File { path }),
            (None, Some(argv)) => Ok(ProofSource::Command { argv }),
            _ => Err(MainError::WorkloadLoginIncomplete),
        }
    }
}

/// The login flow selected by [`LoginArgs::mode`].
#[derive(Debug)]
pub(crate) enum LoginMode {
    /// A browser login through the identity provider.
    Interactive {
        /// Base URL of the Gang server.
        server: Url,
        /// Pin of the server's certificate authority; `None` trusts the system roots.
        ca_pin: Option<CaPin>,
        /// Identity provider, when chosen on the command line.
        provider: Option<ProviderName>,
    },
    /// A login with a workload proof.
    Workload {
        /// Base URL of the Gang server.
        server: Url,
        /// Pin of the server's certificate authority; `None` trusts the system roots.
        ca_pin: Option<CaPin>,
        /// Identity provider that vouches for the proof.
        provider: ProviderName,
        /// Where the proof is read from each time a token is minted.
        proof: ProofSource,
    },
}

impl LoginArgs {
    /// Selects the login flow; a workload login needs a provider and a proof source.
    pub(crate) fn mode(self) -> Result<LoginMode> {
        let Self {
            server,
            ca_pin,
            provider,
            workload,
            proof,
        } = self;
        if !workload {
            return Ok(LoginMode::Interactive {
                server,
                ca_pin,
                provider,
            });
        }
        let proof = proof.source()?;
        match provider {
            Some(provider) => Ok(LoginMode::Workload {
                server,
                ca_pin,
                provider,
                proof,
            }),
            None => Err(MainError::WorkloadLoginIncomplete),
        }
    }
}

/// Arguments of `gang bazelrc`.
#[derive(Debug, Args)]
pub struct BazelrcArgs {
    /// Base URL of the Gang server.
    #[arg(long, value_name = "URL")]
    pub server: Url,
    /// SHA-256 pin of the server's certificate authority; the system roots are trusted when omitted.
    #[arg(long, value_name = "SHA256")]
    pub ca_pin: Option<CaPin>,
    /// Command Bazel runs as the credential helper for the server's host.
    #[arg(long, value_name = "NAME", default_value = DEFAULT_CREDENTIAL_HELPER)]
    pub helper: String,
}

/// Arguments of `gang revoke`.
#[derive(Debug, Args)]
#[group(id = "target", required = true, multiple = false)]
pub struct RevokeArgs {
    /// Serial of the certificate to revoke.
    #[arg(long, value_name = "HEX")]
    pub serial: Option<CertSerial>,
    /// Principal to revoke, as `provider:principal`.
    #[arg(long, value_name = "PROVIDER:PRINCIPAL")]
    pub principal: Option<Identity>,
}

/// What `gang revoke` acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RevokeTarget {
    /// One certificate, by serial.
    Certificate(CertSerial),
    /// A principal and every certificate it holds.
    Principal(Identity),
}

impl RevokeArgs {
    /// Builds the revocation target; exactly one of a serial or a principal.
    pub(crate) fn target(self) -> Result<RevokeTarget> {
        match (self.serial, self.principal) {
            (Some(serial), None) => Ok(RevokeTarget::Certificate(serial)),
            (None, Some(principal)) => Ok(RevokeTarget::Principal(principal)),
            _ => Err(MainError::RevocationTargetIncomplete),
        }
    }
}

/// Arguments of `gang restore`.
#[derive(Debug, Args)]
pub struct RestoreArgs {
    /// Principal to restore, as `provider:principal`.
    #[arg(long, value_name = "PROVIDER:PRINCIPAL")]
    pub principal: Identity,
}

/// Arguments of `gang server` and `gang worker`.
#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Configuration file to run from.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
}

/// Arguments of `gang worker`: the configuration file and the endpoints the server resolved for
/// it, which take precedence over the configured values.
#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// Configuration file to run from.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
    /// Public endpoint of the server, used to enroll the worker; overrides `endpoint` in the
    /// configuration file.
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<Url>,
    /// Worker endpoint of the scheduler that workers dial; overrides the scheduler's
    /// `worker_endpoint` in the configuration file.
    #[arg(long, value_name = "URL")]
    pub worker_endpoint: Option<Url>,
}

/// Arguments of the hidden `gang exec`: the sandboxed command and its arguments, verbatim.
#[derive(Debug, Args)]
pub struct ExecArgs {
    /// The command to run and its arguments.
    #[arg(
        required = true,
        num_args = 1..,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "COMMAND"
    )]
    pub command: Vec<OsString>,
}

/// Arguments of `gang local up`.
#[derive(Debug, Args)]
pub struct LocalUpArgs {
    /// Configuration file; defaults to the workspace's local configuration.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
    /// Run the server without TLS and without authentication, for loopback use only.
    #[arg(long)]
    pub insecure: bool,
    /// How long to wait for the server to print its CA pin.
    #[arg(long, value_name = "DURATION", default_value = DEFAULT_STARTUP_TIMEOUT)]
    pub startup_timeout: NonZeroDuration,
    /// How often to check the server's progress.
    #[arg(long, value_name = "DURATION", default_value = DEFAULT_POLL_INTERVAL)]
    pub poll_interval: NonZeroDuration,
}

/// Arguments of `gang local down`.
#[derive(Debug, Args)]
pub struct LocalDownArgs {
    /// Configuration file; defaults to the workspace's local configuration.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
    /// How long to wait for the server to exit after it is signalled.
    #[arg(long, value_name = "DURATION", default_value = DEFAULT_STOP_TIMEOUT)]
    pub stop_timeout: NonZeroDuration,
    /// How often to check whether the server has exited.
    #[arg(long, value_name = "DURATION", default_value = DEFAULT_POLL_INTERVAL)]
    pub poll_interval: NonZeroDuration,
}

/// The hidden `gang deploy` commands: the decisions of the deploy workflow.
#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    /// Print `has_changes=` and `blocked=` for a `tofu show -json` plan; exit 3 when it is blocked.
    #[command(after_help = DEPLOY_POLICY_EXAMPLES)]
    Policy,
    /// Print whether applying a `tofu show -json` plan needs the controller to reload its configuration.
    #[command(after_help = DEPLOY_RESTART_NEEDED_EXAMPLES)]
    RestartNeeded,
    /// Print the store request of a saved plan, read from its `tofu show -json` on standard input.
    #[command(after_help = DEPLOY_STORE_REQUEST_EXAMPLES)]
    StoreRequest(StoreRequestArgs),
    /// Print the manifest of a stored plan, read from its store request on standard input.
    #[command(after_help = DEPLOY_MANIFEST_EXAMPLES)]
    Manifest(ManifestArgs),
    /// Print the plan comment payload for a `tofu show -json` plan; empty input means the plan did not complete.
    #[command(after_help = DEPLOY_COMMENT_EXAMPLES)]
    Comment(CommentArgs),
    /// Print the id of the plan comment among the pull request comments, if there is one.
    #[command(after_help = DEPLOY_COMMENT_ID_EXAMPLES)]
    CommentId,
    /// Print `<number> <head sha>` of the pull request merged as a commit, from the commit's pull requests.
    #[command(after_help = DEPLOY_MERGED_PR_EXAMPLES)]
    MergedPr(MergedPrArgs),
    /// Print the manifest the plan comment carries, from the pull request comments.
    #[command(after_help = DEPLOY_PLAN_COMMENT_EXAMPLES)]
    PlanComment,
    /// Print the workflow file name from `GITHUB_WORKFLOW_REF`.
    #[command(after_help = DEPLOY_WORKFLOW_FILE_EXAMPLES)]
    WorkflowFile(WorkflowFileArgs),
    /// Print the manifest of the reviewed plan of a main commit, from the merge evidence.
    #[command(after_help = DEPLOY_RESOLVE_EXAMPLES)]
    Resolve(ResolveArgs),
    /// Check the versions of the stored plan object, from `aws s3api list-object-versions`.
    #[command(after_help = DEPLOY_CHECK_VERSIONS_EXAMPLES)]
    CheckVersions(ManifestFileArgs),
    /// Check the stored plan object's metadata and age, from `aws s3api head-object`.
    #[command(after_help = DEPLOY_CHECK_OBJECT_EXAMPLES)]
    CheckObject(CheckObjectArgs),
    /// Check the digest of the downloaded plan file.
    #[command(after_help = DEPLOY_CHECK_PLAN_EXAMPLES)]
    CheckPlan(CheckPlanArgs),
}

impl DeployCommand {
    /// The name an error of this command is reported under.
    pub(crate) fn subject(&self) -> &'static str {
        match self {
            Self::Policy => "gang deploy policy",
            Self::RestartNeeded => "gang deploy restart-needed",
            Self::StoreRequest(_) => "gang deploy store-request",
            Self::Manifest(_) => "gang deploy manifest",
            Self::Comment(_) => "gang deploy comment",
            Self::CommentId => "gang deploy comment-id",
            Self::MergedPr(_) => "gang deploy merged-pr",
            Self::PlanComment => "gang deploy plan-comment",
            Self::WorkflowFile(_) => "gang deploy workflow-file",
            Self::Resolve(_) => "gang deploy resolve",
            Self::CheckVersions(_) => "gang deploy check-versions",
            Self::CheckObject(_) => "gang deploy check-object",
            Self::CheckPlan(_) => "gang deploy check-plan",
        }
    }
}

/// Arguments of `gang deploy store-request`: the merge the plan job planned.
#[derive(Debug, Args)]
pub struct StoreRequestArgs {
    /// The repository: `GITHUB_REPOSITORY`.
    #[arg(long, value_name = "OWNER/NAME")]
    pub repository: Repository,
    /// The repository's numeric id: `GITHUB_REPOSITORY_ID`.
    #[arg(long, value_name = "ID")]
    pub repository_id: RepositoryId,
    /// The pull request: `PR_NUMBER`.
    #[arg(long, value_name = "NUMBER")]
    pub pr_number: PrNumber,
    /// The head of the pull request: `PR_HEAD_SHA`.
    #[arg(long, value_name = "SHA")]
    pub pr_head_sha: Sha,
    /// The merge commit the workflow runs for: `GITHUB_SHA`.
    #[arg(long, value_name = "SHA")]
    pub sha: Sha,
    /// The commit checked out: `git rev-parse HEAD`.
    #[arg(long, value_name = "SHA")]
    pub head: Sha,
    /// A parent of the checked-out commit; once per parent, in order.
    #[arg(long = "parent", value_name = "SHA")]
    pub parents: Vec<Sha>,
    /// The tree of the checked-out commit.
    #[arg(long, value_name = "SHA")]
    pub tree_sha: Sha,
    /// The workflow run: `GITHUB_RUN_ID`.
    #[arg(long, value_name = "ID")]
    pub run_id: RunId,
    /// The attempt of the run: `GITHUB_RUN_ATTEMPT`.
    #[arg(long, value_name = "ATTEMPT")]
    pub run_attempt: RunAttempt,
    /// The key prefix of the plan store.
    #[arg(long, value_name = "PREFIX")]
    pub prefix: PlanPrefix,
    /// How long the stored plan stays usable, as a duration such as `24h`.
    #[arg(long, value_name = "DURATION", value_parser = max_age)]
    pub max_age: MaxAge,
    /// The time of storing, in seconds since the Unix epoch.
    #[arg(long, value_name = "SECONDS")]
    pub now: UnixTime,
    /// The saved plan file, whose digest the manifest records.
    #[arg(long, value_name = "FILE")]
    pub plan_file: PathBuf,
}

/// Arguments of `gang deploy manifest`.
#[derive(Debug, Args)]
pub struct ManifestArgs {
    /// The object version the plan store returned for the uploaded plan.
    #[arg(long, value_name = "VERSION")]
    pub plan_version: PlanVersion,
}

/// Arguments of `gang deploy comment`: where the planned tree comes from, exactly one of
/// `--tree` and `--manifest`.
#[derive(Debug, Clone)]
pub enum CommentArgs {
    /// `--tree`: the tree of the checked-out commit, when no plan was stored.
    Tree(Sha),
    /// `--manifest`: the manifest of the stored plan.
    Manifest(PathBuf),
}

const COMMENT_TREE: &str = "tree";
const COMMENT_MANIFEST: &str = "manifest";

impl Args for CommentArgs {
    fn augment_args(cmd: clap::Command) -> clap::Command {
        cmd.arg(
            Arg::new(COMMENT_TREE)
                .long("tree")
                .value_name("SHA")
                .value_parser(clap::value_parser!(Sha))
                .help("The tree of the checked-out commit, when no plan was stored"),
        )
        .arg(
            Arg::new(COMMENT_MANIFEST)
                .long("manifest")
                .value_name("FILE")
                .value_parser(clap::value_parser!(PathBuf))
                .help("The manifest of the stored plan"),
        )
        .group(
            ArgGroup::new("source")
                .args([COMMENT_TREE, COMMENT_MANIFEST])
                .required(true)
                .multiple(false),
        )
    }

    fn augment_args_for_update(cmd: clap::Command) -> clap::Command {
        Self::augment_args(cmd)
    }
}

impl FromArgMatches for CommentArgs {
    fn from_arg_matches(matches: &ArgMatches) -> std::result::Result<Self, clap::Error> {
        if let Some(tree) = matches.get_one::<Sha>(COMMENT_TREE) {
            Ok(Self::Tree(tree.clone()))
        } else if let Some(manifest) = matches.get_one::<PathBuf>(COMMENT_MANIFEST) {
            Ok(Self::Manifest(manifest.clone()))
        } else {
            Err(clap::Error::new(
                clap::error::ErrorKind::MissingRequiredArgument,
            ))
        }
    }

    fn update_from_arg_matches(
        &mut self,
        matches: &ArgMatches,
    ) -> std::result::Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

/// Arguments of `gang deploy merged-pr`.
#[derive(Debug, Args)]
pub struct MergedPrArgs {
    /// The merge commit: `GITHUB_SHA`.
    #[arg(long, value_name = "SHA")]
    pub sha: Sha,
    /// The repository: `GITHUB_REPOSITORY`.
    #[arg(long, value_name = "OWNER/NAME")]
    pub repository: Repository,
}

/// Arguments of `gang deploy workflow-file`.
#[derive(Debug, Args)]
pub struct WorkflowFileArgs {
    /// The ref of the running workflow: `GITHUB_WORKFLOW_REF`.
    #[arg(long = "ref", value_name = "REF")]
    pub workflow_ref: String,
}

/// Arguments of `gang deploy resolve`: the main commit the apply job runs for.
#[derive(Debug, Args)]
pub struct ResolveArgs {
    /// The ref the workflow runs for: `GITHUB_REF`.
    #[arg(long = "ref", value_name = "REF")]
    pub git_ref: String,
    /// The commit checked out: `git rev-parse HEAD`.
    #[arg(long, value_name = "SHA")]
    pub head: Sha,
    /// The commit the workflow runs for: `GITHUB_SHA`.
    #[arg(long, value_name = "SHA")]
    pub sha: Sha,
    /// A parent of the checked-out commit; once per parent, in order.
    #[arg(long = "parent", value_name = "SHA")]
    pub parents: Vec<Sha>,
    /// The tree of the checked-out commit.
    #[arg(long, value_name = "SHA")]
    pub tree_sha: Sha,
    /// The repository: `GITHUB_REPOSITORY`.
    #[arg(long, value_name = "OWNER/NAME")]
    pub repository: Repository,
    /// The repository's numeric id: `GITHUB_REPOSITORY_ID`.
    #[arg(long, value_name = "ID")]
    pub repository_id: RepositoryId,
    /// The key prefix of the plan store.
    #[arg(long, value_name = "PREFIX")]
    pub prefix: PlanPrefix,
    /// The ref of the running workflow: `GITHUB_WORKFLOW_REF`.
    #[arg(long, value_name = "REF")]
    pub workflow_ref: String,
}

/// Arguments of `gang deploy check-versions`.
#[derive(Debug, Args)]
pub struct ManifestFileArgs {
    /// The manifest of the reviewed plan.
    #[arg(long, value_name = "FILE")]
    pub manifest: PathBuf,
}

/// Arguments of `gang deploy check-object`.
#[derive(Debug, Args)]
pub struct CheckObjectArgs {
    /// The manifest of the reviewed plan.
    #[arg(long, value_name = "FILE")]
    pub manifest: PathBuf,
    /// The time of the check, in seconds since the Unix epoch.
    #[arg(long, value_name = "SECONDS")]
    pub now: UnixTime,
    /// How old the stored plan may be, as a duration such as `24h`.
    #[arg(long, value_name = "DURATION", value_parser = max_age)]
    pub max_age: MaxAge,
}

/// Arguments of `gang deploy check-plan`.
#[derive(Debug, Args)]
pub struct CheckPlanArgs {
    /// The manifest of the reviewed plan.
    #[arg(long, value_name = "FILE")]
    pub manifest: PathBuf,
    /// The downloaded plan file.
    #[arg(long, value_name = "FILE")]
    pub plan_file: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    use clap::CommandFactory;
    use clap::error::ErrorKind;
    use gang_client::ProofSource;
    use gang_types::{CertSerial, Principal, ProviderName};

    use super::*;

    const SERVER: &str = "https://example.com";
    const PIN: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn try_parse(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
        Cli::try_parse_from(args)
    }

    fn parse(args: &[&str]) -> Cli {
        try_parse(args).expect("the command line parses")
    }

    fn command(args: &[&str]) -> Command {
        parse(args).command
    }

    fn rejection(args: &[&str]) -> ErrorKind {
        try_parse(args)
            .expect_err("the command line is rejected")
            .kind()
    }

    fn login(extra: &[&str]) -> LoginArgs {
        let mut args = vec!["gang", "login", "--server", SERVER, "--ca-pin", PIN];
        args.extend_from_slice(extra);
        let Command::Login(login) = command(&args) else {
            panic!("expected the login command");
        };
        login
    }

    fn revoke(extra: &[&str]) -> RevokeArgs {
        let mut args = vec!["gang", "revoke"];
        args.extend_from_slice(extra);
        let Command::Revoke(revoke) = command(&args) else {
            panic!("expected revoke");
        };
        revoke
    }

    fn bazelrc(extra: &[&str]) -> BazelrcArgs {
        let mut args = vec!["gang", "bazelrc", "--server", SERVER];
        args.extend_from_slice(extra);
        let Command::Bazelrc(bazelrc) = command(&args) else {
            panic!("expected bazelrc");
        };
        bazelrc
    }

    fn example_provider() -> ProviderName {
        ProviderName::try_new("example-idp").expect("valid provider")
    }

    #[test]
    fn worker_endpoint_flags_parse() {
        let Command::Worker(args) = command(&[
            "gang",
            "worker",
            "--config",
            "worker.toml",
            "--endpoint",
            "https://example.com",
            "--worker-endpoint",
            "grpcs://example.com:8443",
        ]) else {
            panic!("expected the worker command");
        };
        assert_eq!(
            args.endpoint.as_ref().map(ToString::to_string).as_deref(),
            Some("https://example.com/")
        );
        assert_eq!(
            args.worker_endpoint
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("grpcs://example.com:8443")
        );
    }

    #[test]
    fn worker_endpoint_flags_reject_a_malformed_url() {
        assert_eq!(
            rejection(&[
                "gang",
                "worker",
                "--config",
                "w.toml",
                "--endpoint",
                "not a url",
            ]),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn interactive_login_parses_and_selects_interactive_mode() {
        let args = login(&[]);
        assert_eq!(args.server.host_str(), Some("example.com"));
        assert_eq!(
            args.ca_pin.as_ref().map(ToString::to_string).as_deref(),
            Some(PIN)
        );
        assert!(args.provider.is_none());
        assert!(!args.workload);
        assert!(args.proof.token.is_none());
        assert!(args.proof.token_command.is_none());
        assert!(matches!(
            args.mode(),
            Ok(LoginMode::Interactive {
                ca_pin: Some(_),
                provider: None,
                ..
            })
        ));
    }

    #[test]
    fn interactive_login_keeps_its_provider() {
        let args = login(&["--provider", "example-idp"]);
        assert_eq!(
            args.provider.as_ref().map(ProviderName::as_str),
            Some("example-idp")
        );
        assert!(matches!(
            args.mode(),
            Ok(LoginMode::Interactive {
                provider: Some(_),
                ..
            })
        ));
    }

    #[test]
    fn login_without_ca_pin_trusts_the_system_roots() {
        let Command::Login(args) = command(&["gang", "login", "--server", SERVER]) else {
            panic!("expected the login command");
        };
        assert!(args.ca_pin.is_none());
        assert!(matches!(
            args.mode(),
            Ok(LoginMode::Interactive { ca_pin: None, .. })
        ));
    }

    #[test]
    fn workload_login_from_file_keeps_the_path() {
        let args = login(&[
            "--workload",
            "--provider",
            "example-idp",
            "--token",
            "token.txt",
        ]);
        assert!(args.workload);
        assert_eq!(args.proof.token, Some(PathBuf::from("token.txt")));
        let Ok(LoginMode::Workload {
            provider, proof, ..
        }) = args.mode()
        else {
            panic!("expected workload mode");
        };
        assert_eq!(provider, example_provider());
        assert_eq!(
            proof,
            ProofSource::File {
                path: PathBuf::from("token.txt"),
            }
        );
    }

    #[test]
    fn workload_login_from_command_takes_the_arguments_after_it() {
        let args = login(&[
            "--workload",
            "--provider",
            "example-idp",
            "--token-command",
            "print-proof",
            "--format",
            "raw",
        ]);
        let Ok(LoginMode::Workload { proof, .. }) = args.mode() else {
            panic!("expected workload mode");
        };
        assert_eq!(
            proof,
            ProofSource::Command {
                argv: vec![
                    "print-proof".to_owned(),
                    "--format".to_owned(),
                    "raw".to_owned(),
                ],
            }
        );
    }

    #[test]
    fn workload_login_without_provider_is_a_clap_error() {
        assert_eq!(
            rejection(&[
                "gang",
                "login",
                "--server",
                SERVER,
                "--workload",
                "--token",
                "token.txt",
            ]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn workload_login_without_proof_is_a_clap_error() {
        assert_eq!(
            rejection(&[
                "gang",
                "login",
                "--server",
                SERVER,
                "--workload",
                "--provider",
                "example-idp",
            ]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn token_without_workload_is_a_clap_error() {
        assert_eq!(
            rejection(&[
                "gang",
                "login",
                "--server",
                SERVER,
                "--provider",
                "example-idp",
                "--token",
                "token.txt",
            ]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn token_and_token_command_together_are_a_clap_error() {
        assert_eq!(
            rejection(&[
                "gang",
                "login",
                "--server",
                SERVER,
                "--workload",
                "--provider",
                "example-idp",
                "--token",
                "token.txt",
                "--token-command",
                "print-proof",
            ]),
            ErrorKind::ArgumentConflict
        );
    }

    #[test]
    fn workload_mode_without_provider_is_an_error() {
        let mut args = login(&[]);
        args.workload = true;
        args.proof.token = Some(PathBuf::from("token.txt"));
        assert!(matches!(
            args.mode(),
            Err(MainError::WorkloadLoginIncomplete)
        ));
    }

    #[test]
    fn workload_mode_without_proof_is_an_error() {
        let mut args = login(&["--provider", "example-idp"]);
        args.workload = true;
        assert!(matches!(
            args.mode(),
            Err(MainError::WorkloadLoginIncomplete)
        ));
    }

    #[test]
    fn bazelrc_parses_its_server_with_defaults() {
        let args = bazelrc(&[]);
        assert_eq!(args.server.host_str(), Some("example.com"));
        assert!(args.ca_pin.is_none());
        assert_eq!(args.helper, "gang");
    }

    #[test]
    fn bazelrc_takes_a_pin_and_a_helper() {
        let args = bazelrc(&["--ca-pin", PIN, "--helper", "gang-helper"]);
        assert_eq!(
            args.ca_pin.as_ref().map(ToString::to_string).as_deref(),
            Some(PIN)
        );
        assert_eq!(args.helper, "gang-helper");
    }

    #[test]
    fn bazelrc_without_a_server_is_a_clap_error() {
        assert_eq!(
            rejection(&["gang", "bazelrc"]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn get_takes_no_arguments() {
        assert!(matches!(command(&["gang", "get"]), Command::Get));
        assert_eq!(
            rejection(&["gang", "get", "extra"]),
            ErrorKind::UnknownArgument
        );
    }

    #[test]
    fn cert_group_is_removed() {
        assert_eq!(
            rejection(&["gang", "cert", "ensure"]),
            ErrorKind::InvalidSubcommand
        );
        assert_eq!(
            rejection(&["gang", "cert", "revoke", "--serial", "00ff"]),
            ErrorKind::InvalidSubcommand
        );
    }

    #[test]
    fn revoke_serial_targets_a_certificate() {
        let target = revoke(&["--serial", "ff"]).target();
        assert_eq!(
            target.expect("a serial target"),
            RevokeTarget::Certificate(CertSerial::try_new("ff").expect("valid serial"))
        );
    }

    #[test]
    fn revoke_principal_targets_a_principal() {
        let target = revoke(&["--principal", "example-idp:user-1"]).target();
        assert_eq!(
            target.expect("a principal target"),
            RevokeTarget::Principal(Identity::new(
                example_provider(),
                Principal::try_new("user-1").expect("valid principal"),
            ))
        );
    }

    #[test]
    fn principal_splits_at_the_first_colon() {
        let target = revoke(&["--principal", "example-idp:user:1"]).target();
        assert_eq!(
            target.expect("a principal target"),
            RevokeTarget::Principal(Identity::new(
                example_provider(),
                Principal::try_new("user:1").expect("valid principal"),
            ))
        );
    }

    #[test]
    fn principal_without_a_separator_is_a_clap_error() {
        assert_eq!(
            rejection(&["gang", "revoke", "--principal", "user-1"]),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn principal_with_an_empty_provider_is_a_clap_error() {
        assert_eq!(
            rejection(&["gang", "revoke", "--principal", ":user-1"]),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn revoke_without_a_target_is_a_clap_error() {
        assert_eq!(
            rejection(&["gang", "revoke"]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn revoke_serial_with_principal_is_a_clap_error() {
        assert_eq!(
            rejection(&[
                "gang",
                "revoke",
                "--serial",
                "ff",
                "--principal",
                "example-idp:user-1",
            ]),
            ErrorKind::ArgumentConflict
        );
    }

    #[test]
    fn revoke_rejects_the_separate_provider_flag() {
        assert_eq!(
            rejection(&[
                "gang",
                "revoke",
                "--provider",
                "example-idp",
                "--principal",
                "user-1",
            ]),
            ErrorKind::UnknownArgument
        );
    }

    #[test]
    fn revoke_target_rejects_an_incomplete_target() {
        let mut args = revoke(&["--serial", "ff"]);
        args.serial = None;
        assert!(matches!(
            args.target(),
            Err(MainError::RevocationTargetIncomplete)
        ));
    }

    #[test]
    fn restore_takes_a_principal() {
        let Command::Restore(args) =
            command(&["gang", "restore", "--principal", "example-idp:user-1"])
        else {
            panic!("expected restore");
        };
        assert_eq!(
            args.principal,
            Identity::new(
                example_provider(),
                Principal::try_new("user-1").expect("valid principal"),
            )
        );
    }

    #[test]
    fn restore_without_a_principal_is_a_clap_error() {
        assert_eq!(
            rejection(&["gang", "restore"]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn durations_parse_with_units() {
        let Command::Local(LocalCommand::Up(args)) = command(&[
            "gang",
            "local",
            "up",
            "--startup-timeout",
            "30s",
            "--poll-interval",
            "100ms",
        ]) else {
            panic!("expected local up");
        };
        assert_eq!(args.startup_timeout, NonZeroDuration::from_secs(30));
        assert_eq!(args.poll_interval, NonZeroDuration::from_millis(100));
    }

    #[test]
    fn durations_default_when_omitted() {
        let Command::Local(LocalCommand::Down(args)) = command(&["gang", "local", "down"]) else {
            panic!("expected local down");
        };
        assert_eq!(args.stop_timeout, NonZeroDuration::from_secs(30));
        assert_eq!(args.poll_interval, NonZeroDuration::from_millis(100));
        assert!(args.config.is_none());
    }

    #[test]
    fn zero_duration_is_rejected() {
        assert_eq!(
            rejection(&["gang", "local", "up", "--startup-timeout", "0s"]),
            ErrorKind::ValueValidation
        );
        assert_eq!(
            rejection(&["gang", "local", "down", "--poll-interval", "0ms"]),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn malformed_duration_is_rejected() {
        assert_eq!(
            rejection(&["gang", "local", "up", "--poll-interval", "soon"]),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn local_up_parses_its_config() {
        let Command::Local(LocalCommand::Up(args)) =
            command(&["gang", "local", "up", "--config", "local.toml"])
        else {
            panic!("expected local up");
        };
        assert_eq!(args.config, Some(PathBuf::from("local.toml")));
        assert!(!args.insecure);
    }

    #[test]
    fn local_up_takes_the_insecure_flag() {
        let Command::Local(LocalCommand::Up(args)) =
            command(&["gang", "local", "up", "--insecure"])
        else {
            panic!("expected local up");
        };
        assert!(args.insecure);
    }

    #[test]
    fn exec_keeps_separator_delimited_arguments_verbatim() {
        let Command::Exec(args) = command(&["gang", "exec", "--", "sh", "-c", "exit 3"]) else {
            panic!("expected exec");
        };
        assert_eq!(
            args.command,
            vec![
                OsString::from("sh"),
                OsString::from("-c"),
                OsString::from("exit 3"),
            ]
        );
    }

    #[test]
    fn exec_keeps_flag_looking_arguments_verbatim() {
        let Command::Exec(args) = command(&["gang", "exec", "echo", "--version"]) else {
            panic!("expected exec");
        };
        assert_eq!(
            args.command,
            vec![OsString::from("echo"), OsString::from("--version")]
        );
    }

    #[test]
    fn exec_requires_a_command() {
        assert_eq!(
            rejection(&["gang", "exec"]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn ca_pin_takes_a_configuration_file() {
        let Command::CaPin(args) = command(&["gang", "ca-pin", "--config", "x"]) else {
            panic!("expected ca-pin");
        };
        assert_eq!(args.config, PathBuf::from("x"));
    }

    #[test]
    fn options_takes_an_optional_provisioner() {
        let Command::Options(all) = command(&["gang", "options"]) else {
            panic!("expected options");
        };
        assert_eq!(all.provisioner, None);
        let Command::Options(one) = command(&["gang", "options", "aws"]) else {
            panic!("expected options");
        };
        assert_eq!(one.provisioner.as_deref(), Some("aws"));
    }

    #[test]
    fn doctor_takes_no_arguments_by_default() {
        let Command::Doctor(args) = command(&["gang", "doctor"]) else {
            panic!("expected doctor");
        };
        assert!(args.server.is_none());
        assert!(args.ca_pin.is_none());
        assert_eq!(args.bazel, OsString::from("bazel"));
        assert!(args.bazel_args.is_empty());
    }

    #[test]
    fn doctor_takes_a_server_and_a_pin() {
        let Command::Doctor(args) =
            command(&["gang", "doctor", "--server", SERVER, "--ca-pin", PIN])
        else {
            panic!("expected doctor");
        };
        assert_eq!(
            args.server.as_ref().and_then(Url::host_str),
            Some("example.com")
        );
        assert_eq!(
            args.ca_pin.as_ref().map(ToString::to_string).as_deref(),
            Some(PIN)
        );
    }

    #[test]
    fn doctor_forwards_bazel_arguments_in_order() {
        let Command::Doctor(args) = command(&[
            "gang",
            "doctor",
            "--bazel",
            "bazelisk",
            "--",
            "--config=ci",
            "--remote_cache=",
        ]) else {
            panic!("expected doctor");
        };
        assert_eq!(args.bazel, OsString::from("bazelisk"));
        assert_eq!(
            args.bazel_args,
            vec![
                OsString::from("--config=ci"),
                OsString::from("--remote_cache="),
            ]
        );
    }

    #[test]
    fn hidden_commands_are_absent_from_help() {
        let help = Cli::command().render_help().to_string();
        assert!(
            help.lines()
                .any(|line| line.split_whitespace().next() == Some("login"))
        );
        for name in ["exec", "get", "worker", "deploy"] {
            let listed = help
                .lines()
                .any(|line| line.split_whitespace().next() == Some(name));
            assert!(!listed, "{name} must not be listed in gang --help");
        }
    }

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn store_request(extra: &[&str]) -> std::result::Result<Cli, clap::Error> {
        let mut args = vec![
            "gang",
            "deploy",
            "store-request",
            "--repository",
            "example-org/gang",
            "--repository-id",
            "42",
            "--sha",
            SHA,
            "--head",
            SHA,
            "--parent",
            SHA,
            "--parent",
            SHA,
            "--tree-sha",
            SHA,
            "--run-id",
            "99",
            "--run-attempt",
            "1",
            "--prefix",
            "plans",
            "--now",
            "1767312000",
            "--plan-file",
            "infra.tfplan",
        ];
        args.extend_from_slice(extra);
        try_parse(&args)
    }

    #[test]
    fn deploy_policy_and_restart_needed_take_no_arguments() {
        assert!(matches!(
            command(&["gang", "deploy", "policy"]),
            Command::Deploy(DeployCommand::Policy)
        ));
        assert!(matches!(
            command(&["gang", "deploy", "restart-needed"]),
            Command::Deploy(DeployCommand::RestartNeeded)
        ));
        assert_eq!(
            rejection(&["gang", "deploy", "policy", "plan.json"]),
            ErrorKind::UnknownArgument
        );
    }

    #[test]
    fn deploy_workflow_file_requires_a_ref() {
        let workflow_ref = "example-org/gang/.github/workflows/ci.yml@refs/heads/main";
        let Command::Deploy(DeployCommand::WorkflowFile(args)) =
            command(&["gang", "deploy", "workflow-file", "--ref", workflow_ref])
        else {
            panic!("expected gang deploy workflow-file");
        };
        assert_eq!(args.workflow_ref, workflow_ref);
        assert_eq!(
            rejection(&["gang", "deploy", "workflow-file"]),
            ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn deploy_store_request_takes_every_parent_in_order() {
        let cli = store_request(&["--pr-head-sha", SHA, "--pr-number", "7", "--max-age", "24h"])
            .expect("the command line parses");
        let Command::Deploy(DeployCommand::StoreRequest(args)) = cli.command else {
            panic!("expected gang deploy store-request");
        };
        assert_eq!(args.parents.len(), 2);
        assert_eq!(args.pr_number.get(), 7);
        assert_eq!(args.max_age.as_secs(), 86_400);
    }

    #[test]
    fn deploy_store_request_rejects_a_non_numeric_pr_number() {
        let error = store_request(&[
            "--pr-head-sha",
            SHA,
            "--pr-number",
            "abc",
            "--max-age",
            "24h",
        ])
        .expect_err("the command line is rejected");
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("`abc` is not numeric"));
    }

    #[test]
    fn deploy_store_request_rejects_a_non_numeric_max_age() {
        let error = store_request(&["--pr-head-sha", SHA, "--pr-number", "7", "--max-age", "abc"])
            .expect_err("the command line is rejected");
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
    }

    #[test]
    fn deploy_store_request_rejects_a_max_age_without_unit_or_zero() {
        for value in ["86400", "0s"] {
            let error =
                store_request(&["--pr-head-sha", SHA, "--pr-number", "7", "--max-age", value])
                    .expect_err("the command line is rejected");
            assert_eq!(error.kind(), ErrorKind::ValueValidation);
        }
    }

    #[test]
    fn deploy_store_request_rejects_an_uppercase_sha() {
        let upper = SHA.to_uppercase();
        let error = store_request(&[
            "--pr-number",
            "7",
            "--max-age",
            "24h",
            "--pr-head-sha",
            &upper,
        ])
        .expect_err("the command line is rejected");
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(
            error
                .to_string()
                .contains("is not a 40-character lowercase hex sha")
        );
    }

    #[test]
    fn deploy_comment_takes_exactly_one_tree_source() {
        assert!(matches!(
            command(&["gang", "deploy", "comment", "--tree", SHA]),
            Command::Deploy(DeployCommand::Comment(CommentArgs::Tree(_)))
        ));
        assert!(matches!(
            command(&["gang", "deploy", "comment", "--manifest", "manifest.json"]),
            Command::Deploy(DeployCommand::Comment(CommentArgs::Manifest(path)))
                if path == Path::new("manifest.json")
        ));
        assert_eq!(
            rejection(&["gang", "deploy", "comment"]),
            ErrorKind::MissingRequiredArgument
        );
        assert_eq!(
            rejection(&[
                "gang",
                "deploy",
                "comment",
                "--tree",
                SHA,
                "--manifest",
                "manifest.json"
            ]),
            ErrorKind::ArgumentConflict
        );
    }

    #[test]
    fn deploy_manifest_rejects_a_none_plan_version() {
        assert_eq!(
            rejection(&["gang", "deploy", "manifest", "--plan-version", "None"]),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn deploy_resolve_rejects_an_absolute_prefix() {
        let error = try_parse(&[
            "gang",
            "deploy",
            "resolve",
            "--ref",
            "refs/heads/main",
            "--head",
            SHA,
            "--sha",
            SHA,
            "--parent",
            SHA,
            "--tree-sha",
            SHA,
            "--repository",
            "example-org/gang",
            "--repository-id",
            "42",
            "--prefix",
            "/plans",
            "--workflow-ref",
            "example-org/gang/.github/workflows/infra.yml@refs/heads/main",
        ])
        .expect_err("the command line is rejected");
        assert!(
            error
                .to_string()
                .contains("plan prefix must not start with /")
        );
    }
}
