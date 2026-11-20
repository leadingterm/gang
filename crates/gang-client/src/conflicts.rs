//! Locate and safely relocate effective rc settings that conflict with Gang.
//!
//! Every conflict is planned once, from one read of its rc file: the words to move are token
//! indices of one entry, and the entry's replacement bytes are fixed when the conflict is found.
//! All conflicts of one file are applied together as an [`RcEdit`], so the edit of one entry never
//! shifts the others.

use std::ops::Range;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use gang_wire::Server;
use serde_json::Value;

use crate::bazel_options::{
    EffectiveOptions, OptionSource, REMOTE_CACHE, REMOTE_CACHE_HEADER, REMOTE_EXEC_HEADER,
    REMOTE_EXECUTOR, REMOTE_HEADER,
};
use crate::errors::ClientError;
use crate::rc_file::{Entry, RcFile};
use crate::server_probe::same_server;

/// Config containing the settings for another remote service.
pub const NOT_GANG_CONFIG: &str = "not-gang";
/// Header flags which can leak credentials to a different remote service.
const HEADERS: [&str; 3] = [REMOTE_HEADER, REMOTE_CACHE_HEADER, REMOTE_EXEC_HEADER];

/// Consequence of directing uploads away from the executor's cache.
pub(crate) const CAS_MISMATCH: &str =
    "actions upload to another cache and fail with \"missing from CAS\"";

/// Whether two paths name the same rc file, following symbolic links.
pub(crate) fn same_rc(left: &Path, right: &Path) -> bool {
    left == right
        || matches!(
            (std::fs::metadata(left), std::fs::metadata(right)),
            (Ok(left), Ok(right)) if (left.dev(), left.ino()) == (right.dev(), right.ino())
        )
}

/// An entry whose settings conflict with Gang's endpoints.
#[derive(Debug, Clone)]
pub struct Conflict {
    /// Source rc file.
    pub path: PathBuf,
    /// First physical line, numbered from one.
    pub line: usize,
    /// Bazel section containing the entry.
    pub(crate) section: String,
    /// Reasons this entry conflicts.
    pub reasons: Vec<ConflictReason>,
    /// The entry's bytes when it was found; the edit applies only to identical bytes.
    source: Vec<u8>,
    /// The bytes that replace the entry.
    replacement: Vec<u8>,
}

/// Why an rc entry conflicts with Gang.
#[derive(Debug, Clone)]
pub enum ConflictReason {
    /// The executor points elsewhere.
    Executor,
    /// The cache points elsewhere.
    Cache {
        /// Whether the executor is Gang's.
        executor_is_gang: bool,
    },
    /// A header from another service would be sent to Gang.
    Header {
        /// Header option without leading dashes.
        flag: String,
    },
}

impl ConflictReason {
    /// Human-readable consequence of this conflict.
    #[must_use]
    pub fn effect(&self) -> String {
        match self {
            Self::Executor => "actions run on another service's executor".into(),
            Self::Cache { executor_is_gang: true } => CAS_MISMATCH.into(),
            Self::Cache { .. } => "actions use another service's cache".into(),
            Self::Header { .. } => "another service's credential header would be sent to Gang's endpoints; it moves with that service's flags".into(),
        }
    }
}

/// Find effective endpoint and header settings originating outside the owned rc file. Each rc
/// file is read once.
#[must_use]
pub fn find_conflicts(
    server: &Server<Value>,
    options: &EffectiveOptions,
    owned: &Path,
) -> Vec<Conflict> {
    let mut files = RcFiles::default();
    let mut found = Vec::new();
    let executor_is_gang = options
        .value(REMOTE_EXECUTOR)
        .is_some_and(|s| same_server(&s.value, &server.remote_executor));
    for (flag, expected) in [
        (REMOTE_EXECUTOR, &server.remote_executor),
        (REMOTE_CACHE, &server.remote_cache),
    ] {
        if let Some(setting) = options.value(flag)
            && !same_server(&setting.value, expected)
        {
            let reason = if flag == REMOTE_EXECUTOR {
                ConflictReason::Executor
            } else {
                ConflictReason::Cache { executor_is_gang }
            };
            let search = Search {
                flag,
                value: None,
                with_headers: true,
            };
            locate(
                &mut files,
                &mut found,
                &setting.source,
                owned,
                &search,
                reason,
            );
        }
    }
    for flag in HEADERS {
        for setting in options.values(flag) {
            let search = Search {
                flag,
                value: Some(setting.value.as_str()),
                with_headers: false,
            };
            let reason = ConflictReason::Header { flag: flag.into() };
            locate(
                &mut files,
                &mut found,
                &setting.source,
                owned,
                &search,
                reason,
            );
        }
    }
    found
        .into_iter()
        .filter_map(|found| {
            let (contents, parsed) = files.get(&found.path)?;
            found.into_conflict(contents, parsed)
        })
        .collect()
}

/// Every conflict of one rc file, planned and applied as one edit.
#[derive(Debug, Clone)]
pub struct RcEdit {
    /// The rc file.
    pub path: PathBuf,
    /// Its conflicts, each in a different entry.
    pub conflicts: Vec<Conflict>,
}

/// Groups conflicts by rc file, in the order the files first appear.
#[must_use]
pub fn rc_edits(conflicts: Vec<Conflict>) -> Vec<RcEdit> {
    let mut edits: Vec<RcEdit> = Vec::new();
    for conflict in conflicts {
        match edits.iter_mut().find(|edit| edit.path == conflict.path) {
            Some(edit) => edit.conflicts.push(conflict),
            None => edits.push(RcEdit {
                path: conflict.path.clone(),
                conflicts: vec![conflict],
            }),
        }
    }
    edits
}

impl RcEdit {
    /// Applies every conflict's edit to `contents` at once.
    ///
    /// # Errors
    /// Returns [`ClientError::RcEntryChanged`] when an entry no longer holds the bytes it had when
    /// the conflict was found.
    pub fn edit(&self, contents: &[u8]) -> Result<Vec<u8>, ClientError> {
        let parsed = RcFile::parse(contents);
        let mut spans: Vec<(Range<usize>, &[u8])> = Vec::with_capacity(self.conflicts.len());
        for conflict in &self.conflicts {
            let entry = parsed
                .entries
                .iter()
                .find(|entry| {
                    entry.line == conflict.line && contents[entry.span.clone()] == conflict.source
                })
                .ok_or_else(|| ClientError::RcEntryChanged {
                    path: self.path.clone(),
                    line: conflict.line,
                })?;
            spans.push((entry.span.clone(), &conflict.replacement));
        }
        spans.sort_by_key(|(span, _)| span.start);
        spans.dedup_by_key(|(span, _)| span.start);
        let mut result = Vec::with_capacity(contents.len() + 64);
        let mut cursor = 0;
        for (span, replacement) in spans {
            result.extend_from_slice(&contents[cursor..span.start]);
            result.extend_from_slice(replacement);
            cursor = span.end;
        }
        result.extend_from_slice(&contents[cursor..]);
        Ok(result)
    }
}

impl Conflict {
    /// Describe the exact edit and its effects.
    #[must_use]
    pub fn fix(&self) -> String {
        let lines = |bytes: &[u8]| {
            String::from_utf8_lossy(bytes)
                .replace("\\\r\n", " ")
                .replace("\\\n", " ")
                .lines()
                .map(|line| format!("`{line}`"))
                .collect::<Vec<_>>()
                .join(" and ")
        };
        format!(
            "{}:{}: change {} to {} (applies only with --config={NOT_GANG_CONFIG}); {}",
            self.path.display(),
            self.line,
            lines(&self.source),
            lines(&self.replacement),
            self.reasons
                .iter()
                .map(ConflictReason::effect)
                .collect::<Vec<_>>()
                .join("; ")
        )
    }
}

/// An rc file's bytes and their parse.
type Parsed = (Vec<u8>, RcFile);

/// Rc files read during one search, each read and parsed once; `None` when unreadable.
#[derive(Default)]
struct RcFiles {
    /// Path, then its bytes and parse.
    files: Vec<(PathBuf, Option<Parsed>)>,
}

impl RcFiles {
    /// The bytes and parse of `path`, reading it on first use.
    fn get(&mut self, path: &Path) -> Option<(&[u8], &RcFile)> {
        let index = if let Some(index) = self.files.iter().position(|(known, _)| known == path) {
            index
        } else {
            let parsed = std::fs::read(path).ok().map(|bytes| {
                let parsed = RcFile::parse(&bytes);
                (bytes, parsed)
            });
            self.files.push((path.to_path_buf(), parsed));
            self.files.len() - 1
        };
        self.files[index]
            .1
            .as_ref()
            .map(|(bytes, parsed)| (bytes.as_slice(), parsed))
    }
}

/// The option an effective setting came from.
struct Search<'a> {
    /// Option name without dashes.
    flag: &'a str,
    /// The value that must match, or any value.
    value: Option<&'a str>,
    /// Whether the entry's header words move with it.
    with_headers: bool,
}

/// A conflicting entry found in an rc file, before its edit is planned.
struct Found {
    /// The rc file.
    path: PathBuf,
    /// Index of the entry in the parsed file.
    entry: usize,
    /// The entry's section.
    section: String,
    /// Indices of the words to move.
    indices: Vec<usize>,
    /// Why the entry conflicts.
    reasons: Vec<ConflictReason>,
}

/// Identify the last matching entry in the reported section and merge its reasons.
fn locate(
    files: &mut RcFiles,
    found: &mut Vec<Found>,
    source: &OptionSource,
    owned: &Path,
    search: &Search<'_>,
    reason: ConflictReason,
) {
    let OptionSource::RcFile { path, section } = source else {
        return;
    };
    if same_rc(path, owned) {
        return;
    }
    let Some((_, parsed)) = files.get(path) else {
        return;
    };
    let Some(index) = parsed.entries.iter().rposition(|entry| {
        entry
            .tokens
            .first()
            .is_some_and(|token| token.value == section.as_bytes())
            && occurrences(entry, search.flag).iter().any(|occurrence| {
                search
                    .value
                    .is_none_or(|value| occurrence.value == value.as_bytes())
            })
    }) else {
        return;
    };
    let entry = &parsed.entries[index];
    let mut flags = vec![search.flag];
    if search.with_headers {
        flags.extend(HEADERS);
    }
    let indices = flags
        .into_iter()
        .flat_map(|flag| occurrences(entry, flag))
        .flat_map(|occurrence| occurrence.indices);
    if let Some(existing) = found
        .iter_mut()
        .find(|known| known.path == *path && known.entry == index)
    {
        existing.indices.extend(indices);
        existing.reasons.push(reason);
    } else {
        found.push(Found {
            path: path.clone(),
            entry: index,
            section: section.clone(),
            indices: indices.collect(),
            reasons: vec![reason],
        });
    }
}

/// One occurrence of an option in an entry.
struct Occurrence<'e> {
    /// Indices of its words: the option, and its value when that is a separate word.
    indices: Range<usize>,
    /// The option's value.
    value: &'e [u8],
}

/// Every occurrence of `--<flag>=<value>` or `--<flag> <value>` in an entry, after its section.
fn occurrences<'e>(entry: &'e Entry, flag: &str) -> Vec<Occurrence<'e>> {
    let option = format!("--{flag}");
    let mut found = Vec::new();
    for (i, token) in entry.tokens.iter().enumerate().skip(1) {
        let Some(rest) = token.value.strip_prefix(option.as_bytes()) else {
            continue;
        };
        if rest.is_empty() {
            let end = (i + 2).min(entry.tokens.len());
            let value = entry
                .tokens
                .get(i + 1)
                .map_or(&[][..], |next| next.value.as_slice());
            found.push(Occurrence {
                indices: i..end,
                value,
            });
        } else if let Some(value) = rest.strip_prefix(b"=") {
            found.push(Occurrence {
                indices: i..i + 1,
                value,
            });
        }
    }
    found
}

impl Found {
    /// Plans the entry's edit, or `None` when the entry is already in Gang's opt-in section.
    fn into_conflict(mut self, contents: &[u8], parsed: &RcFile) -> Option<Conflict> {
        let entry = parsed.entries.get(self.entry)?;
        self.indices.sort_unstable();
        self.indices.dedup();
        let command = entry.tokens[0]
            .value
            .split(|b| *b == b':')
            .next()
            .unwrap_or_default();
        let mut target = command.to_vec();
        target.push(b':');
        target.extend_from_slice(NOT_GANG_CONFIG.as_bytes());
        if entry.tokens[0].value == target {
            return None;
        }
        Some(Conflict {
            replacement: replacement(contents, entry, &self.indices, &target),
            source: contents[entry.span.clone()].to_vec(),
            path: self.path,
            line: entry.line,
            section: self.section,
            reasons: self.reasons,
        })
    }
}

/// The bytes replacing `entry` once the words at `indices` move to section `target`. When every
/// word after the section moves, only the section changes; otherwise the words leave the entry,
/// each with one adjacent blank, and a new `target` line holding them follows the entry.
fn replacement(contents: &[u8], entry: &Entry, indices: &[usize], target: &[u8]) -> Vec<u8> {
    let section = &entry.tokens[0].span;
    if indices.len() == entry.tokens.len() - 1 {
        let mut result = contents[entry.span.start..section.start].to_vec();
        result.extend_from_slice(target);
        result.extend_from_slice(&contents[section.end..entry.span.end]);
        return result;
    }
    let mut result = Vec::with_capacity(entry.span.len() + target.len() + 2);
    let mut cursor = entry.span.start;
    for &i in indices {
        let token = &entry.tokens[i].span;
        let blank = |at: usize| matches!(contents.get(at), Some(b' ' | b'\t'));
        let (start, end) = if token.start > cursor && blank(token.start - 1) {
            (token.start - 1, token.end)
        } else if token.end < entry.span.end && blank(token.end) {
            (token.start, token.end + 1)
        } else {
            (token.start, token.end)
        };
        let start = start.max(cursor);
        result.extend_from_slice(&contents[cursor..start]);
        cursor = end;
    }
    result.extend_from_slice(&contents[cursor..entry.span.end]);
    let own = &contents[entry.span.clone()];
    let ending: &[u8] = if own.ends_with(b"\r\n") {
        b"\r\n"
    } else if own.ends_with(b"\n") {
        b"\n"
    } else {
        let first = contents.iter().position(|b| *b == b'\n');
        let ending: &[u8] = if first.is_some_and(|n| n > 0 && contents[n - 1] == b'\r') {
            b"\r\n"
        } else {
            b"\n"
        };
        result.extend_from_slice(ending);
        ending
    };
    result.extend_from_slice(target);
    for &i in indices {
        result.push(b' ');
        result.extend_from_slice(&contents[entry.tokens[i].span.clone()]);
    }
    result.extend_from_slice(ending);
    result
}

/// Whether an rc file may be edited automatically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Writable {
    /// A regular, owned, writable file.
    Yes,
    /// A symlink, which must not be followed for editing.
    Symlink {
        /// Link target as recorded in the filesystem.
        target: PathBuf,
    },
    /// The current user does not own the file.
    NotOwned,
    /// The file is not writable or cannot be inspected.
    ReadOnly,
}

/// Inspect a path without following symlinks before checking ownership and access.
#[must_use]
pub fn writable(path: &Path) -> Writable {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Writable::ReadOnly;
    };
    if metadata.file_type().is_symlink() {
        return Writable::Symlink {
            target: std::fs::read_link(path).unwrap_or_default(),
        };
    }
    if !metadata.is_file() {
        return Writable::ReadOnly;
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Writable::NotOwned;
    }
    if rustix::fs::access(path, rustix::fs::Access::WRITE_OK).is_err() {
        return Writable::ReadOnly;
    }
    Writable::Yes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bazel_options::Setting;

    /// The server whose endpoints are Gang's.
    fn server() -> Server<Value> {
        serde_json::from_str(r#"{"name":"gang","remoteExecutor":"grpcs://gang","remoteCache":"grpcs://gang","defaultIsa":"x86-64","actionEnvironment":"host:test","authentication":{"none":{}}}"#).expect("server")
    }

    /// Writes `contents` to an rc file and finds the conflicts of `settings`, each
    /// `(flag, value, section)` attributed to that file.
    fn find(
        contents: &[u8],
        settings: &[(&str, &str, &str)],
    ) -> (tempfile::TempDir, Vec<Conflict>) {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("rc");
        std::fs::write(&path, contents).expect("write");
        let options = EffectiveOptions::new(
            settings
                .iter()
                .map(|(flag, value, section)| {
                    (
                        (*flag).to_owned(),
                        Setting {
                            value: (*value).to_owned(),
                            source: OptionSource::RcFile {
                                path: path.clone(),
                                section: (*section).to_owned(),
                            },
                        },
                    )
                })
                .collect(),
        );
        let conflicts = find_conflicts(&server(), &options, Path::new("/owned"));
        (dir, conflicts)
    }

    /// Applies every conflict found in `contents` as one edit.
    fn edited(contents: &[u8], conflicts: Vec<Conflict>) -> Vec<u8> {
        let mut edits = rc_edits(conflicts);
        assert_eq!(edits.len(), 1, "one rc file");
        edits.remove(0).edit(contents).expect("edit")
    }

    /// A realistic CRLF file keeps unrelated settings and comments byte-for-byte.
    #[test]
    fn moves_only_conflicting_tokens_and_preserves_crlf() {
        let contents = b"# rc\r\nbuild --disk_cache=/tmp --remote_upload_local_results\r\nbuild:ci --jobs=4\r\nbuild --remote_cache=grpcs://cache.example.com \"--remote_header=Authorization=Bearer x\" --jobs=8 # keep\r\n";
        let (_dir, conflicts) = find(
            contents,
            &[
                (REMOTE_CACHE, "grpcs://cache.example.com", "build"),
                (REMOTE_HEADER, "Authorization=Bearer x", "build"),
            ],
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].line, 4);
        assert_eq!(conflicts[0].reasons.len(), 2);
        assert_eq!(
            edited(contents, conflicts),
            b"# rc\r\nbuild --disk_cache=/tmp --remote_upload_local_results\r\nbuild:ci --jobs=4\r\nbuild --jobs=8 # keep\r\nbuild:not-gang --remote_cache=grpcs://cache.example.com \"--remote_header=Authorization=Bearer x\"\r\n"
        );
    }

    /// A sole conflicting option only changes the section, including on continued entries.
    #[test]
    fn changes_only_section_for_entire_entry() {
        let contents = b"build \\\n  --remote_cache grpcs://other # keep\n";
        let (_dir, conflicts) = find(contents, &[(REMOTE_CACHE, "grpcs://other", "build")]);
        assert!(
            !conflicts[0].fix().contains("\\ "),
            "continuation must not leave a backslash in the fix"
        );
        assert_eq!(
            edited(contents, conflicts),
            b"build:not-gang \\\n  --remote_cache grpcs://other # keep\n"
        );
    }

    /// A configured section moves to `<command>:not-gang`: Bazel configs do not nest, so
    /// `build:ci:not-gang` would define a config `--config=not-gang` never selects.
    #[test]
    fn configured_sections_move_to_the_command_config() {
        let contents =
            b"build:ci --remote_cache=grpcs://other\nbuild:ci --remote_cache=grpcs://ci --jobs=4\n";
        let (_dir, conflicts) = find(contents, &[(REMOTE_CACHE, "grpcs://ci", "build:ci")]);
        let fix = conflicts[0].fix();
        assert!(
            fix.contains("to `build:ci --jobs=4` and `build:not-gang --remote_cache=grpcs://ci`"),
            "split configured entry must quote each output line"
        );
        assert_eq!(
            edited(contents, conflicts),
            b"build:ci --remote_cache=grpcs://other\nbuild:ci --jobs=4\nbuild:not-gang --remote_cache=grpcs://ci\n"
        );
        let contents = b"common:ci --remote_header=A=b\n";
        let (_dir, conflicts) = find(contents, &[(REMOTE_HEADER, "A=b", "common:ci")]);
        let fix = conflicts[0].fix();
        assert!(
            fix.contains("to `common:not-gang --remote_header=A=b`"),
            "a common:ci entry must move to common:not-gang: {fix}"
        );
    }

    /// Repeated options on one line move together, in their original order.
    #[test]
    fn repeated_space_separated_options_keep_their_order() {
        let contents = b"build --remote_header A=1 --remote_header B=2 --jobs=3\n";
        let (_dir, conflicts) = find(
            contents,
            &[
                (REMOTE_HEADER, "A=1", "build"),
                (REMOTE_HEADER, "B=2", "build"),
            ],
        );
        assert_eq!(conflicts.len(), 1);
        let fix = conflicts[0].fix();
        assert!(
            fix.contains(
                "to `build --jobs=3` and `build:not-gang --remote_header A=1 --remote_header B=2`"
            ),
            "split entry must quote each output line"
        );
        assert_eq!(
            edited(contents, conflicts),
            b"build --jobs=3\nbuild:not-gang --remote_header A=1 --remote_header B=2\n"
        );
    }

    /// Two conflicting entries of one file are edited together, without line shifts.
    #[test]
    fn conflicts_of_one_file_apply_as_one_edit() {
        let contents =
            b"build --remote_cache=grpcs://other --jobs=2\nbuild --disk_cache=/d\nbuild --remote_header=A=b --jobs=4\n";
        let (_dir, conflicts) = find(
            contents,
            &[
                (REMOTE_CACHE, "grpcs://other", "build"),
                (REMOTE_HEADER, "A=b", "build"),
            ],
        );
        assert_eq!(conflicts.len(), 2);
        assert_eq!(
            edited(contents, conflicts),
            b"build --jobs=2\nbuild:not-gang --remote_cache=grpcs://other\nbuild --disk_cache=/d\nbuild --jobs=4\nbuild:not-gang --remote_header=A=b\n"
        );
    }

    /// A changed entry is refused rather than edited from a stale plan.
    #[test]
    fn stale_entries_are_refused() {
        let contents = b"build --remote_cache=grpcs://other --jobs=2\n";
        let (_dir, conflicts) = find(contents, &[(REMOTE_CACHE, "grpcs://other", "build")]);
        let edit = rc_edits(conflicts).remove(0);
        assert!(matches!(
            edit.edit(b"build --remote_cache=grpcs://other --jobs=3\n"),
            Err(ClientError::RcEntryChanged { line: 1, .. })
        ));
    }

    /// A non-UTF-8 word elsewhere in the entry does not hide its conflict.
    #[test]
    fn non_utf8_words_do_not_hide_conflicts() {
        let (_dir, conflicts) = find(
            b"build --remote_header=A=b --disk_cache=/\xff\n",
            &[(REMOTE_HEADER, "A=b", "build")],
        );
        assert_eq!(
            conflicts.len(),
            1,
            "a header next to a non-UTF-8 word must still be found"
        );
    }

    /// The last matching section wins; an owned file is excluded.
    #[test]
    fn finds_the_effective_entry_and_skips_the_owned_file() {
        let contents = b"build --remote_cache=old\nbuild:ci --remote_cache=other\nbuild --remote_cache=grpcs://cache.example.com\n";
        let (dir, conflicts) = find(
            contents,
            &[(REMOTE_CACHE, "grpcs://cache.example.com", "build")],
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].line, 3);
        let path = dir.path().join("rc");
        let options = EffectiveOptions::new(vec![(
            REMOTE_CACHE.into(),
            Setting {
                value: "grpcs://cache.example.com".into(),
                source: OptionSource::RcFile {
                    path: path.clone(),
                    section: "build".into(),
                },
            },
        )]);
        assert!(find_conflicts(&server(), &options, &path).is_empty());
    }
}

#[cfg(test)]
mod writable_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    /// Symlinks must never be offered as writable targets.
    #[test]
    fn distinguishes_regular_files_and_symlinks() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let file = dir.path().join("file");
        let link = dir.path().join("link");
        std::fs::write(&file, b"").expect("write");
        symlink(&file, &link).expect("symlink");
        assert_eq!(writable(&file), Writable::Yes);
        assert_eq!(writable(&link), Writable::Symlink { target: file });
        assert_eq!(writable(dir.path()), Writable::ReadOnly);
    }
}
