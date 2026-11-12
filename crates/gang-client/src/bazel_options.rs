//! What Bazel reports about its own configuration: the effective value of every command option and
//! where each value came from (an rc file section, the Bazel client, or nothing Bazel can name).
//! The answer comes from Bazel's build event protocol: `bazel build --nobuild` writes the events,
//! and nothing is built.

use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Arc;

use gang_clock::Clock;
use gang_os::{RunOutput, fs};
use gang_types::{NonZeroBytes, NonZeroDuration};
use serde::Deserialize;

use crate::api::truncate_chars;
use crate::errors::BazelError;
use crate::options::ClientOptions;

/// The Bazel flag that names the remote executor.
pub(crate) const REMOTE_EXECUTOR: &str = "remote_executor";
/// The Bazel flag that names the remote cache.
pub(crate) const REMOTE_CACHE: &str = "remote_cache";
/// The Bazel flag that names the CA certificate Bazel trusts for remote endpoints.
pub(crate) const TLS_CERTIFICATE: &str = "tls_certificate";
/// The Bazel flag that names a credential helper, possibly scoped to a host.
pub(crate) const CREDENTIAL_HELPER: &str = "credential_helper";

/// The build event label of the command line that holds Bazel's effective options.
const CANONICAL: &str = "canonical";
/// The build event label of the command line that holds how each option was set.
const ORIGINAL: &str = "original";
/// The section of a command line that holds command options.
const COMMAND_OPTIONS: &str = "command options";
/// The `rc_source` value of options set by the Bazel client itself.
const CLIENT_SOURCE: &str = "client";

/// Where the value of a Bazel option came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionSource {
    /// Set in `section` (`build`, `common`, `build:ci`, ...) of the rc file at `path`.
    RcFile {
        /// The rc file that holds the setting.
        path: PathBuf,
        /// The config section of that file that holds the setting.
        section: String,
    },
    /// Set by the Bazel client itself (rc source `client`).
    Client,
    /// No rc entry carries this value: the command line or a flag expansion.
    Unknown,
}

/// The effective value of one Bazel option and where it was set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setting {
    /// The value Bazel uses; empty for a flag without a value.
    pub value: String,
    /// Where the value was set.
    pub source: OptionSource,
}

/// Bazel's effective command options, in the order Bazel reports them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EffectiveOptions {
    /// Flag name without leading dashes, paired with its setting, in canonical order.
    settings: Vec<(String, Setting)>,
}

impl EffectiveOptions {
    /// Builds options from settings already in canonical order.
    pub(crate) fn new(settings: Vec<(String, Setting)>) -> Self {
        Self { settings }
    }

    /// Every canonical entry of `flag`, in canonical order. A multi-valued flag yields several.
    pub fn values<'a>(&'a self, flag: &'a str) -> impl Iterator<Item = &'a Setting> + 'a {
        self.settings
            .iter()
            .filter(move |(name, _)| name.as_str() == flag)
            .map(|(_, setting)| setting)
    }

    /// The last canonical entry of `flag`, or `None` when Bazel reports no such flag.
    #[must_use]
    pub fn value(&self, flag: &str) -> Option<&Setting> {
        self.settings
            .iter()
            .rev()
            .find(|(name, _)| name.as_str() == flag)
            .map(|(_, setting)| setting)
    }

    /// Parses build event JSON lines. `Ok(None)` when no canonical structured command line is
    /// present. Blank lines are skipped.
    pub(crate) fn parse(events: &str) -> Result<Option<Self>, serde_json::Error> {
        let mut canonical_line = None;
        let mut original_line = None;
        for line in events.lines().filter(|line| !line.trim().is_empty()) {
            let event: BuildEvent = serde_json::from_str(line)?;
            let Some(command_line) = event.structured_command_line else {
                continue;
            };
            let slot = match command_line.command_line_label.as_str() {
                CANONICAL => &mut canonical_line,
                ORIGINAL => &mut original_line,
                _ => continue,
            };
            if slot.is_none() {
                *slot = Some(command_line);
            }
        }
        let Some(canonical_line) = canonical_line else {
            return Ok(None);
        };
        let requested = requested_configs(&canonical_line);
        let provenance = Provenance::read(original_line.as_ref(), &requested);
        let settings = canonical_line
            .sections
            .iter()
            .filter(|section| section.section_label == COMMAND_OPTIONS)
            .flat_map(|section| &section.option_list.option)
            .filter(|option| option.option_name != "rc_source")
            .filter(|option| !option.option_name.is_empty())
            .filter(|option| option.option_name != "default_override")
            .map(|option| {
                let value = option
                    .option_value
                    .as_deref()
                    .unwrap_or_default()
                    .to_owned();
                let source = provenance.attribute(&option.option_name, &value);
                (option.option_name.clone(), Setting { value, source })
            })
            .collect();
        Ok(Some(Self::new(settings)))
    }
}

/// The values of the canonical `config` options: the configs Bazel was asked to apply.
fn requested_configs(canonical: &CommandLine) -> Vec<String> {
    canonical
        .sections
        .iter()
        .filter(|section| section.section_label == COMMAND_OPTIONS)
        .flat_map(|section| &section.option_list.option)
        .filter(|option| option.option_name == "config")
        .map(|option| option.option_value.clone().unwrap_or_default())
        .collect()
}

/// Keeps the overrides of sections that apply to this build: `common`, `always`, `build`, and
/// `build:<config>` for each applicable config.
fn applicable_overrides(overrides: Vec<Override>, requested: &[String]) -> Vec<Override> {
    let configs = applicable_configs(&overrides, requested);
    overrides
        .into_iter()
        .filter(|found| applies(&found.section, &configs))
        .collect()
}

/// The requested configs plus every config named by `--config` in an applicable override, until
/// no new config appears.
fn applicable_configs(overrides: &[Override], requested: &[String]) -> Vec<String> {
    let mut configs = requested.to_vec();
    loop {
        let named: Vec<&str> = overrides
            .iter()
            .filter(|found| found.flag == "config" && applies(&found.section, &configs))
            .map(|found| found.value.as_str())
            .filter(|config| !configs.iter().any(|known| known == *config))
            .collect();
        if named.is_empty() {
            return configs;
        }
        configs.extend(named.into_iter().map(str::to_owned));
    }
}

/// Whether a section's overrides apply: `common`, `always`, `build`, or `build:<config>` for an
/// applicable config.
fn applies(section: &str, configs: &[String]) -> bool {
    match section {
        "common" | "always" | "build" => true,
        _ => section
            .strip_prefix("build:")
            .is_some_and(|config| configs.iter().any(|known| known == config)),
    }
}

/// The part of the `original` command line that says where options were set.
struct Provenance {
    /// The `rc_source` entries in order: index 0 is the Bazel client, then the rc files.
    sources: Vec<String>,
    /// The `default_override` settings in the order Bazel reports them, rc words joined.
    overrides: Vec<Override>,
}

impl Provenance {
    /// Reads the rc sources and overrides of the `original` command line, if there is one.
    fn read(original: Option<&CommandLine>, requested: &[String]) -> Self {
        let mut sources = Vec::new();
        let mut words = Vec::new();
        let options = original
            .into_iter()
            .flat_map(|line| &line.sections)
            .flat_map(|section| &section.option_list.option);
        for option in options {
            match option.option_name.as_str() {
                "rc_source" => {
                    sources.push(
                        option
                            .option_value
                            .as_deref()
                            .unwrap_or_default()
                            .to_owned(),
                    );
                }
                "default_override" => {
                    words.extend(option.option_value.as_deref().and_then(Word::parse));
                }
                _ => {}
            }
        }
        Self {
            sources,
            overrides: applicable_overrides(join_words(words), requested),
        }
    }

    /// The source of `flag=value`: the last override that sets exactly that, resolved through
    /// the rc source it names. `Unknown` when no override matches or its index is out of range.
    fn attribute(&self, flag: &str, value: &str) -> OptionSource {
        let Some(found) = self
            .overrides
            .iter()
            .rev()
            .find(|found| found.flag == flag && found.value == value)
        else {
            return OptionSource::Unknown;
        };
        match self.sources.get(found.index).map(String::as_str) {
            Some(CLIENT_SOURCE) => OptionSource::Client,
            Some(path) => OptionSource::RcFile {
                path: PathBuf::from(path),
                section: found.section.clone(),
            },
            None => OptionSource::Unknown,
        }
    }
}

/// One setting named by `default_override` entries: the rc source index and section that set
/// `--flag=value`.
struct Override {
    /// Index into the `rc_source` entries.
    index: usize,
    /// The config section that set the flag, such as `build:ci`.
    section: String,
    /// The flag name without dashes.
    flag: String,
    /// The value the flag was set to.
    value: String,
}

/// One rc word of a `default_override` entry: `<idx>:<section>=<option text>`.
struct Word {
    /// Index into the `rc_source` entries.
    index: usize,
    /// The config section the word was set in, such as `build:ci`.
    section: String,
    /// The option text: `--name=value`, or a bare `--name` whose value is the next word.
    text: String,
}

impl Word {
    /// Parses `<idx>:<section>=<option text>`. `None` when any part is missing or malformed.
    fn parse(entry: &str) -> Option<Self> {
        let (index, rest) = entry.split_once(':')?;
        let (section, text) = rest.split_once('=')?;
        Some(Self {
            index: index.parse().ok()?,
            section: section.to_owned(),
            text: text.to_owned(),
        })
    }

    /// Whether this is a bare `--name` whose value is the next word.
    fn takes_next(&self) -> bool {
        self.text.starts_with("--") && !self.text.contains('=')
    }

    /// Whether `next` supplies this word's value: same rc source and section, and not an option.
    fn continued_by(&self, next: &Word) -> bool {
        next.index == self.index && next.section == self.section && !next.text.starts_with('-')
    }
}

/// Joins each bare `--name` word with the value word that follows it, when one does.
fn join_words(words: Vec<Word>) -> Vec<Override> {
    let mut words = words.into_iter().peekable();
    let mut overrides = Vec::new();
    while let Some(word) = words.next() {
        let value = if word.takes_next() {
            words.next_if(|next| word.continued_by(next))
        } else {
            None
        };
        overrides.push(Override::new(word, value));
    }
    overrides
}

impl Override {
    /// Builds the setting of `word`, taking its value from `value` when a value word was joined.
    fn new(word: Word, value: Option<Word>) -> Self {
        let (flag, inline) = split_option(&word.text);
        let value = value.map_or_else(|| inline.to_owned(), |next| next.text);
        Self {
            index: word.index,
            section: word.section,
            flag: flag.to_owned(),
            value,
        }
    }
}

/// Splits `--name=value` (or `name=value`) at its first `=`. No `=` means an empty value.
fn split_option(option: &str) -> (&str, &str) {
    let option = option.strip_prefix("--").unwrap_or(option);
    option.split_once('=').unwrap_or((option, ""))
}

/// One line of the build event protocol. Only the structured command line matters here.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuildEvent {
    structured_command_line: Option<CommandLine>,
}

/// A structured command line: either Bazel's effective options or how they were set.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandLine {
    #[serde(default)]
    command_line_label: String,
    #[serde(default)]
    sections: Vec<Section>,
}

/// A named section of a command line, such as `command options`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Section {
    #[serde(default)]
    section_label: String,
    #[serde(default)]
    option_list: OptionList,
}

/// The options of one section.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OptionList {
    #[serde(default)]
    option: Vec<OptionEntry>,
}

/// One option of a command line. Options without a value have no `optionValue`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OptionEntry {
    #[serde(default)]
    option_name: String,
    option_value: Option<String>,
}

/// Something that can report Bazel's effective options.
pub trait BazelProbe: Send + Sync {
    /// Asks Bazel for its effective options.
    fn effective_options(
        &self,
    ) -> impl Future<Output = Result<EffectiveOptions, BazelError>> + Send;
}

/// Asks a Bazel program for its effective options by running `bazel build --nobuild` with a build
/// event file, in the working directory of this process.
pub struct BazelCommand {
    /// The program to run, such as `bazel`.
    program: OsString,
    /// Arguments placed after `build --nobuild`, such as `--config=ci`.
    args: Vec<OsString>,
    /// How long the program may take before it is abandoned.
    timeout: NonZeroDuration,
    /// The most characters of the program's error output quoted in an error.
    max_detail_chars: NonZeroUsize,
    /// The cap on each output stream.
    max_output: NonZeroBytes,
    /// The clock driving the timeout.
    clock: Arc<dyn Clock>,
}

impl BazelCommand {
    /// A command that runs `program` with `args`, bounded by the Bazel timeout and error detail
    /// limit of `options`.
    pub fn new(
        program: impl Into<OsString>,
        args: Vec<OsString>,
        options: &ClientOptions,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            program: program.into(),
            args,
            timeout: options.bazel_timeout,
            max_detail_chars: options.max_error_message_chars,
            max_output: options.max_response_size,
            clock,
        }
    }
}

impl BazelProbe for BazelCommand {
    async fn effective_options(&self) -> Result<EffectiveOptions, BazelError> {
        let program = self.program.to_string_lossy().into_owned();
        let dir = tempfile::tempdir().map_err(|source| BazelError::Spawn {
            program: program.clone(),
            source,
        })?;
        let events = dir.path().join("events.json");
        let output = self.run(&program, &events).await?;
        // A missing or unreadable events file means Bazel reported nothing, so it parses as empty.
        let text = take_events(&events);
        match EffectiveOptions::parse(&text) {
            Ok(Some(options)) => Ok(options),
            Ok(None) => Err(no_command_line(
                program,
                output.status,
                &output.stderr,
                self.max_detail_chars,
            )),
            Err(source) => Err(BazelError::Events { program, source }),
        }
    }
}

impl BazelCommand {
    /// Runs Bazel to completion in a process group of its own, so that a timeout or a cancelled
    /// wait kills everything it started.
    async fn run(&self, program: &str, events: &Path) -> Result<RunOutput, BazelError> {
        let mut command = tokio::process::Command::new(&self.program);
        command
            .arg("build")
            .arg("--nobuild")
            .arg(format!("--build_event_json_file={}", events.display()))
            .args(&self.args);
        self.clock
            .timeout(
                self.timeout.get(),
                gang_os::run_bounded(&mut command, None, self.max_output),
            )
            .await
            .map_err(|_| BazelError::TimedOut {
                program: program.to_owned(),
                seconds: self.timeout.get().as_secs(),
            })?
            .map_err(|error| BazelError::Spawn {
                program: program.to_owned(),
                source: match error {
                    gang_os::RunError::Spawn { source, .. }
                    | gang_os::RunError::Io { source, .. } => source,
                    other @ gang_os::RunError::StdoutTooLarge { .. } => {
                        std::io::Error::other(other)
                    }
                },
            })
    }
}

/// Reads the events file and removes it. A missing or unreadable file reads as empty: Bazel
/// reported no events either way. Removal is best effort: the temporary directory holding the
/// file is removed on drop anyway.
fn take_events(path: &Path) -> String {
    let text = fs::read_to_string_optional(path)
        .ok()
        .flatten()
        .unwrap_or_default();
    let _ = fs::remove_optional(path);
    text
}

/// The error for a run that reported no command line, quoting what its stderr says.
fn no_command_line(
    program: String,
    status: ExitStatus,
    stderr: &[u8],
    max_chars: NonZeroUsize,
) -> BazelError {
    BazelError::NoCommandLine {
        program,
        status: status.to_string(),
        detail: failure_detail(&String::from_utf8_lossy(stderr), max_chars),
    }
}

/// The line of `stderr` that best explains a failure: the first `ERROR:` line, else the last
/// non-empty line, trimmed and cut to `max_chars` characters followed by `...`.
pub(crate) fn failure_detail(stderr: &str, max_chars: NonZeroUsize) -> String {
    let lines: Vec<&str> = stderr.lines().map(str::trim).collect();
    let line = lines
        .iter()
        .copied()
        .find(|line| line.starts_with("ERROR:"))
        .or_else(|| lines.iter().rev().copied().find(|line| !line.is_empty()))
        .unwrap_or_default();
    truncate_chars(line, max_chars).into_owned()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io;
    use std::num::NonZeroUsize;
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::process::ExitStatus;
    use std::sync::Arc;

    use gang_clock::TestClock;
    use gang_os::FileLock;
    use serde_json::{Value, json};

    use super::{
        BazelCommand, BazelProbe, EffectiveOptions, OptionSource, Setting, failure_detail,
        no_command_line, take_events,
    };
    use crate::errors::BazelError;
    use crate::options::ClientOptions;

    const WORKSPACE: &str = "/work/app/.bazelrc";
    const HOME: &str = "/home/dev/.bazelrc";

    fn option(name: &str, value: Option<&str>) -> Value {
        match value {
            Some(value) => json!({"optionName": name, "optionValue": value}),
            None => json!({"optionName": name}),
        }
    }

    fn event(label: &str, sections: &[(&str, Vec<Value>)]) -> String {
        let sections: Vec<Value> = sections
            .iter()
            .map(|(section, options)| {
                json!({"sectionLabel": section, "optionList": {"option": options}})
            })
            .collect();
        json!({
            "id": {"structuredCommandLine": {"commandLineLabel": label}},
            "structuredCommandLine": {"commandLineLabel": label, "sections": sections},
        })
        .to_string()
    }

    fn canonical() -> String {
        event(
            "canonical",
            &[
                (
                    "startup options",
                    vec![option("output_base", Some("/var/cache/bazel"))],
                ),
                (
                    "command options",
                    vec![
                        option("rc_source", Some("client")),
                        option("default_override", Some("0:common=--color=yes")),
                        option("config", Some("ci")),
                        option("remote_executor", Some("grpc://exec.example:1")),
                        option("remote_cache", Some("grpc://home.example:1")),
                        option(
                            "credential_helper",
                            Some("cache.example.com=/opt/gang/helper"),
                        ),
                        option("credential_helper", Some("/opt/ci/helper")),
                        option("keep_going", None),
                        option("color", Some("yes")),
                        option("sandbox", Some("true")),
                        option("jobs", Some("4")),
                    ],
                ),
            ],
        )
    }

    fn original() -> String {
        event(
            "original",
            &[(
                "command options",
                vec![
                    option("rc_source", Some("client")),
                    option("rc_source", Some(WORKSPACE)),
                    option("rc_source", Some(HOME)),
                    option(
                        "default_override",
                        Some("1:build=--remote_executor=grpc://exec.example:1"),
                    ),
                    // Overridden by the later home entry below: the last match wins.
                    option(
                        "default_override",
                        Some("1:build:ci=--remote_cache=grpc://home.example:1"),
                    ),
                    option(
                        "default_override",
                        Some("2:build=--remote_cache=grpc://home.example:1"),
                    ),
                    option(
                        "default_override",
                        Some("1:common=--credential_helper=cache.example.com=/opt/gang/helper"),
                    ),
                    option(
                        "default_override",
                        Some("1:build:ci=--credential_helper=/opt/ci/helper"),
                    ),
                    option("default_override", Some("1:build=--keep_going")),
                    option("default_override", Some("0:common=--color=yes")),
                    option("default_override", Some("9:build=--jobs=4")),
                    option("default_override", Some("bogus")),
                ],
            )],
        )
    }

    fn started() -> String {
        json!({"id": {"buildStarted": {}}, "started": {"command": "build"}}).to_string()
    }

    fn rc(path: &str, section: &str) -> OptionSource {
        OptionSource::RcFile {
            path: PathBuf::from(path),
            section: section.to_owned(),
        }
    }

    fn entry(flag: &str, value: &str, source: OptionSource) -> (String, Setting) {
        (
            flag.to_owned(),
            Setting {
                value: value.to_owned(),
                source,
            },
        )
    }

    fn expected() -> EffectiveOptions {
        EffectiveOptions::new(vec![
            entry("config", "ci", OptionSource::Unknown),
            entry(
                "remote_executor",
                "grpc://exec.example:1",
                rc(WORKSPACE, "build"),
            ),
            entry("remote_cache", "grpc://home.example:1", rc(HOME, "build")),
            entry(
                "credential_helper",
                "cache.example.com=/opt/gang/helper",
                rc(WORKSPACE, "common"),
            ),
            entry(
                "credential_helper",
                "/opt/ci/helper",
                rc(WORKSPACE, "build:ci"),
            ),
            entry("keep_going", "", rc(WORKSPACE, "build")),
            entry("color", "yes", OptionSource::Client),
            entry("sandbox", "true", OptionSource::Unknown),
            entry("jobs", "4", OptionSource::Unknown),
        ])
    }

    fn parsed() -> EffectiveOptions {
        let events = format!("{}\n{}\n{}\n", started(), canonical(), original());
        EffectiveOptions::parse(&events)
            .expect("events are valid")
            .expect("canonical line is present")
    }

    fn limit(chars: usize) -> NonZeroUsize {
        NonZeroUsize::new(chars).expect("limit is non-zero")
    }

    #[test]
    fn parses_effective_options_with_their_sources() {
        assert_eq!(parsed(), expected());
    }

    #[test]
    fn values_lists_every_entry_and_value_takes_the_last() {
        let options = parsed();

        let helpers: Vec<&str> = options
            .values("credential_helper")
            .map(|setting| setting.value.as_str())
            .collect();

        assert_eq!(
            helpers,
            ["cache.example.com=/opt/gang/helper", "/opt/ci/helper"]
        );
        assert_eq!(
            options
                .value("credential_helper")
                .map(|setting| setting.value.as_str()),
            Some("/opt/ci/helper")
        );
        assert_eq!(options.values("missing").count(), 0);
        assert_eq!(options.value("missing"), None);
    }

    #[test]
    fn without_a_canonical_command_line_there_are_no_options() {
        assert_eq!(
            EffectiveOptions::parse(&original()).expect("events are valid"),
            None
        );
        assert_eq!(
            EffectiveOptions::parse("").expect("empty input is valid"),
            None
        );
    }

    #[test]
    fn blank_lines_are_ignored() {
        let padded = format!("\n   \n{}\n\n\t\n{}\n\n", canonical(), original());

        assert_eq!(
            EffectiveOptions::parse(&padded).expect("blank lines are skipped"),
            Some(expected())
        );
    }

    #[test]
    fn invalid_json_is_an_error() {
        assert!(EffectiveOptions::parse("{not json").is_err());

        let truncated = format!("{}\n{{\"id\":", canonical());
        assert!(EffectiveOptions::parse(&truncated).is_err());
    }

    #[test]
    fn malformed_default_override_is_ignored() {
        let canonical_line = event(
            "canonical",
            &[(
                "command options",
                vec![option("remote_cache", Some("grpc://x"))],
            )],
        );
        let original_line = event(
            "original",
            &[(
                "command options",
                vec![
                    option("rc_source", Some(WORKSPACE)),
                    option("default_override", Some("no-index=--remote_cache=grpc://x")),
                    option("default_override", Some("x:build=--remote_cache=grpc://x")),
                ],
            )],
        );
        let events = format!("{canonical_line}\n{original_line}\n");

        let options = EffectiveOptions::parse(&events)
            .expect("events are valid")
            .expect("canonical line is present");

        assert_eq!(
            options.value("remote_cache").map(|setting| &setting.source),
            Some(&OptionSource::Unknown)
        );
    }

    #[test]
    fn entries_without_an_option_name_are_skipped() {
        let canonical_line = event(
            "canonical",
            &[
                (
                    "startup options",
                    vec![json!({"combinedForm": "--max_idle_secs=120"})],
                ),
                (
                    "command options",
                    vec![
                        json!({"combinedForm": "--max_idle_secs=120"}),
                        option("remote_cache", Some("grpc://home.example:1")),
                    ],
                ),
            ],
        );
        let original_line = event(
            "original",
            &[
                (
                    "startup options",
                    vec![json!({"combinedForm": "--max_idle_secs=120"})],
                ),
                (
                    "command options",
                    vec![
                        option("rc_source", Some("client")),
                        option("rc_source", Some(HOME)),
                        option(
                            "default_override",
                            Some("1:build=--remote_cache=grpc://home.example:1"),
                        ),
                    ],
                ),
            ],
        );
        let events = format!("{canonical_line}\n{original_line}\n");

        let options = EffectiveOptions::parse(&events)
            .expect("entries without a name are valid")
            .expect("canonical line is present");

        assert_eq!(options.values("").count(), 0);
        assert_eq!(options.value("max_idle_secs"), None);
        assert_eq!(
            options.value("remote_cache"),
            Some(&Setting {
                value: "grpc://home.example:1".to_owned(),
                source: rc(HOME, "build"),
            })
        );
    }

    #[test]
    fn events_without_a_command_line_label_are_skipped() {
        let started = r#"{"id":{"started":{}},"children":[{"structuredCommandLine":{"commandLineLabel":"original"}}],"started":{"uuid":"x"}}"#;
        let tool = r#"{"id":{"structuredCommandLine":{"commandLineLabel":"tool"}},"structuredCommandLine":{}}"#;
        let events = format!("{started}\n{tool}\n{}\n{}\n", canonical(), original());

        let options = EffectiveOptions::parse(&events)
            .expect("events without labels are valid")
            .expect("canonical line is present");

        assert_eq!(options, expected());
    }

    #[test]
    fn space_separated_rc_words_keep_their_rc_file_and_section() {
        let canonical_line = event(
            "canonical",
            &[(
                "command options",
                vec![option("remote_cache", Some("grpc://x"))],
            )],
        );
        let original_line = event(
            "original",
            &[(
                "command options",
                vec![
                    option("rc_source", Some("client")),
                    option("rc_source", Some("/home/u/.bazelrc")),
                    option("default_override", Some("0:common=--isatty=0")),
                    option("default_override", Some("1:build=--remote_cache")),
                    option("default_override", Some("1:build=grpc://x")),
                ],
            )],
        );
        let events = format!("{canonical_line}\n{original_line}\n");

        let options = EffectiveOptions::parse(&events)
            .expect("transcript is valid")
            .expect("canonical line is present");

        assert_eq!(
            options.value("remote_cache"),
            Some(&Setting {
                value: "grpc://x".to_owned(),
                source: rc("/home/u/.bazelrc", "build"),
            })
        );
    }

    /// The source Bazel attributes to `remote_cache=grpc://a` for a transcript with the given
    /// canonical options and default overrides; rc sources are client, workspace, then home.
    fn remote_cache_source(canonical: Vec<Value>, overrides: &[&str]) -> OptionSource {
        let canonical_line = event("canonical", &[("command options", canonical)]);
        let mut entries = vec![
            option("rc_source", Some("client")),
            option("rc_source", Some("/ws/.bazelrc")),
            option("rc_source", Some("/home/u/.bazelrc")),
        ];
        entries.extend(
            overrides
                .iter()
                .copied()
                .map(|text| option("default_override", Some(text))),
        );
        let original_line = event("original", &[("command options", entries)]);
        let events = format!("{canonical_line}\n{original_line}\n");

        let options = EffectiveOptions::parse(&events)
            .expect("transcript is valid")
            .expect("canonical line is present");
        options
            .value("remote_cache")
            .expect("remote_cache is set")
            .source
            .clone()
    }

    #[test]
    fn overrides_of_sections_that_do_not_apply_are_ignored() {
        let source = remote_cache_source(
            vec![option("remote_cache", Some("grpc://a"))],
            &[
                "1:build=--remote_cache=grpc://a",
                "2:test=--remote_cache=grpc://a",
                "2:build:foo=--remote_cache=grpc://a",
            ],
        );

        assert_eq!(source, rc("/ws/.bazelrc", "build"));
    }

    #[test]
    fn requested_config_sections_apply() {
        let source = remote_cache_source(
            vec![
                option("remote_cache", Some("grpc://a")),
                option("config", Some("foo")),
            ],
            &[
                "1:build=--remote_cache=grpc://a",
                "2:test=--remote_cache=grpc://a",
                "2:build:foo=--remote_cache=grpc://a",
            ],
        );

        assert_eq!(source, rc("/home/u/.bazelrc", "build:foo"));
    }

    #[test]
    fn config_requested_by_an_applicable_override_applies() {
        let source = remote_cache_source(
            vec![option("remote_cache", Some("grpc://a"))],
            &[
                "1:build=--remote_cache=grpc://a",
                "1:build=--config=foo",
                "2:build:foo=--remote_cache=grpc://a",
            ],
        );

        assert_eq!(source, rc("/home/u/.bazelrc", "build:foo"));
    }

    #[test]
    fn command_takes_timeout_and_detail_limit_from_options() {
        let options = ClientOptions::default();

        let command = BazelCommand::new(
            "bazel",
            vec![OsString::from("--bazelrc=/x")],
            &options,
            Arc::new(TestClock::default()),
        );

        assert_eq!(command.timeout, options.bazel_timeout);
        assert_eq!(command.max_detail_chars, options.max_error_message_chars);
    }

    #[test]
    fn take_events_reads_the_file_and_removes_it() {
        let dir = tempfile::tempdir().expect("creating a temporary directory");
        let path = dir.path().join("events.json");
        std::fs::write(&path, "{}\n").expect("writing the events file");

        assert_eq!(take_events(&path), "{}\n");
        assert!(!path.exists());
    }

    #[test]
    fn take_events_of_a_missing_file_is_empty() {
        let dir = tempfile::tempdir().expect("creating a temporary directory");

        assert_eq!(take_events(&dir.path().join("events.json")), "");
    }

    #[test]
    fn detail_is_the_first_error_line() {
        let stderr =
            "Starting local Bazel server\nERROR: cannot reach remote cache\nmore context\n";

        assert_eq!(
            failure_detail(stderr, limit(200)),
            "ERROR: cannot reach remote cache"
        );
    }

    #[test]
    fn detail_falls_back_to_the_last_non_empty_line() {
        assert_eq!(
            failure_detail("first\n\nlast line\n\n", limit(200)),
            "last line"
        );
    }

    #[test]
    fn detail_is_empty_without_output() {
        assert_eq!(failure_detail("\n  \n", limit(200)), "");
        assert_eq!(failure_detail("", limit(200)), "");
    }

    #[test]
    fn detail_is_trimmed() {
        assert_eq!(failure_detail("   ERROR: x   \n", limit(200)), "ERROR: x");
    }

    #[test]
    fn detail_longer_than_the_limit_is_cut_with_an_ellipsis() {
        assert_eq!(failure_detail("ERROR: abcdefghijkl", limit(5)), "ERROR...");
    }

    #[test]
    fn detail_of_exactly_the_limit_is_kept_whole() {
        assert_eq!(failure_detail("ERROR: abcde", limit(12)), "ERROR: abcde");
    }

    #[test]
    fn detail_limit_counts_characters() {
        assert_eq!(failure_detail("héllo wörld", limit(3)), "hél...");
    }

    /// The fake Bazel that Bazel builds and exposes through `FAKE_BAZEL`.
    fn fake_bazel() -> PathBuf {
        let raw =
            std::env::var_os("FAKE_BAZEL").expect("FAKE_BAZEL must be set by the Bazel test_env");
        std::env::current_dir()
            .expect("a working directory")
            .join(raw)
    }

    /// A fake-Bazel argument `--<name>=<path>`.
    fn fake_arg(name: &str, path: &Path) -> OsString {
        let mut arg = OsString::from(format!("--{name}="));
        arg.push(path);
        arg
    }

    /// Runs `future` on a real-time current-thread runtime while holding the fork gate. Use when
    /// the future waits on child-process I/O: a paused clock would jump to the timeout while idle.
    fn run_real<T>(future: impl Future<Output = T>) -> T {
        let _gate = gang_testkit::fork_gate::spawner();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("building a runtime")
            .block_on(future)
    }

    /// Yields until `path` exists. Gives up after a bounded number of polls, so a fake that never
    /// publishes its grandchild's lock fails the test instead of hanging it.
    async fn wait_for_file(path: &Path) -> bool {
        for _ in 0..1_000_000 {
            if path.exists() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    }

    #[test]
    fn effective_options_parses_the_events_and_removes_them() {
        let dir = tempfile::tempdir().expect("creating a temporary directory");
        let fixture = dir.path().join("events.fixture");
        std::fs::write(
            &fixture,
            format!("{}\n{}\n{}\n", started(), canonical(), original()),
        )
        .expect("writing the events fixture");
        let seen = dir.path().join("events.seen");
        let command = BazelCommand::new(
            fake_bazel(),
            vec![
                fake_arg("fake_events_from", &fixture),
                fake_arg("fake_record_events_path", &seen),
            ],
            &ClientOptions::default(),
            Arc::new(TestClock::default()),
        );
        let options =
            run_real(command.effective_options()).expect("the fake bazel reports its options");
        assert_eq!(options, expected());
        let events =
            std::fs::read_to_string(&seen).expect("the fake bazel recorded its events file");
        assert!(
            !Path::new(&events).exists(),
            "the events file outlives its read"
        );
    }

    #[test]
    fn timeout_kills_the_whole_process_group() {
        let dir = tempfile::tempdir().expect("creating a temporary directory");
        let lock = dir.path().join("grandchild.lock");
        run_real(async {
            let clock = Arc::new(TestClock::default());
            let options = ClientOptions::default();
            let command = BazelCommand::new(
                fake_bazel(),
                vec![fake_arg("fake_grandchild_lock", &lock)],
                &options,
                clock.clone(),
            );
            let (result, published) = tokio::join!(command.effective_options(), async {
                let published = wait_for_file(&lock).await;
                clock.advance(options.bazel_timeout.get());
                published
            });
            assert!(
                matches!(result, Err(BazelError::TimedOut { .. })),
                "{result:?}"
            );
            assert!(published, "the fake bazel started its grandchild");
        });
        let _gate = gang_testkit::fork_gate::lock_user();
        // The grandchild's exit closes the descriptor that holds the lock, so this returns once
        // the timeout has killed it; a surviving grandchild keeps it blocked.
        let freed = FileLock::exclusive_blocking(&lock).expect("the grandchild lock can be taken");
        drop(freed);
    }

    #[test]
    fn no_command_line_quotes_the_first_error_line() {
        let error = no_command_line(
            "bazel".to_owned(),
            ExitStatus::from_raw(1 << 8),
            b"warning: x\nERROR: no workspace\nmore\n",
            limit(200),
        );
        assert!(
            matches!(&error, BazelError::NoCommandLine { program, status, detail }
                if program == "bazel" && status == "exit status: 1" && detail == "ERROR: no workspace"),
            "{error:?}"
        );
    }

    #[test]
    fn missing_program_is_a_spawn_error() {
        let missing = "/nonexistent/gang-bazel-under-test";
        let command = BazelCommand::new(
            missing,
            Vec::new(),
            &ClientOptions::default(),
            Arc::new(TestClock::default()),
        );
        let error = run_real(command.effective_options())
            .expect_err("a program that does not exist cannot run");
        assert!(
            matches!(&error, BazelError::Spawn { program, source }
                if program == missing && source.kind() == io::ErrorKind::NotFound),
            "{error:?}"
        );
    }
}
