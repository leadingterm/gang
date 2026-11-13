//! The `key=value` report a probe prints about the system it ran in, and what each probe's report
//! means.
//!
//! [`ProbeReport`] is the generic parser. A probe whose report a verdict reads has a typed form of
//! it, built with [`TryFrom`] from a [`ProbeReport`] and printed back as `key=value` lines by its
//! [`Display`](fmt::Display), so that the keys of a report are written in this file only, besides
//! the probe's own script.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use crate::errors::{E2eError, Result};

/// The keys a probe reported, each with its value.
///
/// The text form is one `key=value` per line. Blank lines are ignored. The split happens at the
/// first `=`, so a value may contain `=`; a value may be empty.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProbeReport {
    entries: BTreeMap<String, String>,
}

impl FromStr for ProbeReport {
    type Err = E2eError;

    /// Parses the `key=value` text of a probe.
    ///
    /// # Errors
    ///
    /// A non-blank line without `=`, or a key that appears more than once.
    fn from_str(text: &str) -> Result<Self> {
        let mut entries = BTreeMap::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| parse_error(format!("line `{line}` has no `=`")))?;
            if entries.insert(key.to_owned(), value.to_owned()).is_some() {
                return Err(parse_error(format!("repeated key `{key}`")));
            }
        }
        Ok(Self { entries })
    }
}

impl ProbeReport {
    /// The value reported for `key`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when the report has no such key.
    pub fn get(&self, key: &str) -> Result<&str> {
        self.entries
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| parse_error(format!("missing key `{key}`")))
    }

    /// The value reported for `key`, which must be one of the `options`' texts.
    fn choice<T: Copy>(&self, key: &str, options: [(&str, T); 2]) -> Result<T> {
        let value = self.get(key)?;
        options
            .iter()
            .find_map(|&(text, choice)| (text == value).then_some(choice))
            .ok_or_else(|| {
                parse_error(format!(
                    "key `{key}` is `{value}`, expected `{}` or `{}`",
                    options[0].0, options[1].0
                ))
            })
    }

    /// Reads and parses the report stored at `path`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Io`] when the file cannot be read, or the errors of parsing its text.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        text.parse()
    }
}

fn parse_error(detail: String) -> E2eError {
    E2eError::Parse {
        subject: "probe report",
        source: detail.into(),
    }
}

/// The namespaces an action runs in, as `readlink` prints them (`net:[4026531992]`), or
/// `unavailable` where `/proc` cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespaces {
    /// The net namespace.
    pub net: String,
    /// The mnt namespace.
    pub mnt: String,
}

impl TryFrom<&ProbeReport> for Namespaces {
    type Error = E2eError;

    /// Reads the typed report from the keys of `report`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when the report lacks the `net` or `mnt` key.
    fn try_from(report: &ProbeReport) -> Result<Self> {
        Ok(Self {
            net: report.get("net")?.to_owned(),
            mnt: report.get("mnt")?.to_owned(),
        })
    }
}

impl fmt::Display for Namespaces {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "net={}", self.net)?;
        writeln!(f, "mnt={}", self.mnt)
    }
}

/// What the isolation probe sees of its surroundings: its namespaces, its working directory, and
/// the directories above it. The worker's CAS must not be reachable through them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolationReport {
    /// The namespaces of the action.
    pub namespaces: Namespaces,
    /// The working directory.
    pub cwd: String,
    /// The name of the working directory.
    pub cwd_name: String,
    /// The name of the parent of the working directory.
    pub parent_name: String,
    /// The entries of the parent directory.
    pub parent_entries: Vec<String>,
    /// The name of the grandparent directory.
    pub grandparent_name: String,
    /// The entries of the grandparent directory.
    pub grandparent_entries: Vec<String>,
    /// The entries of the great-grandparent directory.
    pub great_grandparent_entries: Vec<String>,
    /// Whether the working directory is writable.
    pub cwd_writable: bool,
}

/// The entries of a comma-terminated `ls` listing, without the empty tail.
fn listing(report: &ProbeReport, key: &str) -> Result<Vec<String>> {
    Ok(report
        .get(key)?
        .split(',')
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect())
}

/// Writes `entries` as a comma-terminated listing.
fn write_listing(f: &mut fmt::Formatter<'_>, key: &str, entries: &[String]) -> fmt::Result {
    write!(f, "{key}=")?;
    for entry in entries {
        write!(f, "{entry},")?;
    }
    writeln!(f)
}

impl TryFrom<&ProbeReport> for IsolationReport {
    type Error = E2eError;

    /// Reads the typed report from the keys of `report`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when the report lacks one of its keys, or `cwd_writable` is neither
    /// `yes` nor `no`.
    fn try_from(report: &ProbeReport) -> Result<Self> {
        Ok(Self {
            namespaces: Namespaces::try_from(report)?,
            cwd: report.get("cwd")?.to_owned(),
            cwd_name: report.get("cwd_name")?.to_owned(),
            parent_name: report.get("parent_name")?.to_owned(),
            parent_entries: listing(report, "parent_entries")?,
            grandparent_name: report.get("grandparent_name")?.to_owned(),
            grandparent_entries: listing(report, "grandparent_entries")?,
            great_grandparent_entries: listing(report, "great_grandparent_entries")?,
            cwd_writable: report.choice("cwd_writable", [("yes", true), ("no", false)])?,
        })
    }
}

impl fmt::Display for IsolationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.namespaces)?;
        writeln!(f, "cwd={}", self.cwd)?;
        writeln!(f, "cwd_name={}", self.cwd_name)?;
        writeln!(f, "parent_name={}", self.parent_name)?;
        write_listing(f, "parent_entries", &self.parent_entries)?;
        writeln!(f, "grandparent_name={}", self.grandparent_name)?;
        write_listing(f, "grandparent_entries", &self.grandparent_entries)?;
        write_listing(
            f,
            "great_grandparent_entries",
            &self.great_grandparent_entries,
        )?;
        writeln!(
            f,
            "cwd_writable={}",
            if self.cwd_writable { "yes" } else { "no" }
        )
    }
}

/// What the poisoning probe did to its input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoisonReport {
    /// Whether it modified the input (`attempt=modified`) or was refused (`attempt=refused`).
    pub modified: bool,
}

impl TryFrom<&ProbeReport> for PoisonReport {
    type Error = E2eError;

    /// Reads the typed report from the keys of `report`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when `attempt` is missing, or neither `modified` nor `refused`.
    fn try_from(report: &ProbeReport) -> Result<Self> {
        Ok(Self {
            modified: report.choice("attempt", [("modified", true), ("refused", false)])?,
        })
    }
}

impl fmt::Display for PoisonReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let attempt = if self.modified { "modified" } else { "refused" };
        writeln!(f, "attempt={attempt}")
    }
}

/// What the probe that runs after the poisoning one saw of the shared input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VictimReport {
    /// Whether the input carries the poisoning action's modification.
    pub poisoned: bool,
}

impl TryFrom<&ProbeReport> for VictimReport {
    type Error = E2eError;

    /// Reads the typed report from the keys of `report`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when `poisoned` is missing, or neither `yes` nor `no`.
    fn try_from(report: &ProbeReport) -> Result<Self> {
        Ok(Self {
            poisoned: report.choice("poisoned", [("yes", true), ("no", false)])?,
        })
    }
}

impl fmt::Display for VictimReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "poisoned={}", if self.poisoned { "yes" } else { "no" })
    }
}

/// What the input root probe read directly and through a relative symlink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputRootReport {
    /// The input's path relative to the working directory.
    pub shared_path: String,
    /// What the action read from the input, empty when it could not.
    pub sibling: String,
    /// What the action read through the relative symlink, empty when it could not.
    pub symlink: String,
    /// The input's link count, or `None` when it could not be read.
    pub sibling_links: Option<u64>,
}

impl TryFrom<&ProbeReport> for InputRootReport {
    type Error = E2eError;

    /// Reads the typed report from the keys of `report`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when a key is missing or the link count is invalid.
    fn try_from(report: &ProbeReport) -> Result<Self> {
        let links = report.get("sibling_links")?;
        let sibling_links = if links == "unreadable" {
            None
        } else {
            Some(links.parse().map_err(|_| {
                parse_error(format!(
                    "key `sibling_links` is `{links}`, expected a number or `unreadable`"
                ))
            })?)
        };
        Ok(Self {
            shared_path: report.get("shared_path")?.to_owned(),
            sibling: report.get("sibling")?.to_owned(),
            symlink: report.get("symlink")?.to_owned(),
            sibling_links,
        })
    }
}

impl fmt::Display for InputRootReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "shared_path={}", self.shared_path)?;
        writeln!(f, "sibling={}", self.sibling)?;
        writeln!(f, "symlink={}", self.symlink)?;
        match self.sibling_links {
            Some(links) => writeln!(f, "sibling_links={links}"),
            None => writeln!(f, "sibling_links=unreadable"),
        }
    }
}

/// What the disk neighbour saw of its own file once its writes ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskNeighbourReport {
    /// Whether the file held everything the neighbour wrote (`neighbour=ok`, else `bad`).
    pub intact: bool,
}

impl TryFrom<&ProbeReport> for DiskNeighbourReport {
    type Error = E2eError;

    /// Reads the typed report from the keys of `report`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Parse`] when `neighbour` is missing, or neither `ok` nor `bad`.
    fn try_from(report: &ProbeReport) -> Result<Self> {
        Ok(Self {
            intact: report.choice("neighbour", [("ok", true), ("bad", false)])?,
        })
    }
}

impl fmt::Display for DiskNeighbourReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "neighbour={}", if self.intact { "ok" } else { "bad" })
    }
}
