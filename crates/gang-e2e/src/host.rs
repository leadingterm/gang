//! The namespaces the harness itself runs in, against which isolation is judged.

use std::fs;

/// The identities of the net and mount namespaces of the harness process.
///
/// Each is the target of the namespace link under `/proc/self/ns`, such as `net:[4026531992]`.
/// A value is `None` where the link cannot be read (on platforms without namespaces).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    /// The network namespace of the harness process.
    pub net: Option<String>,
    /// The mount namespace of the harness process.
    pub mnt: Option<String>,
}

impl Host {
    /// Observes the namespaces of the current process.
    #[must_use]
    pub fn observe() -> Self {
        Self {
            net: namespace("/proc/self/ns/net"),
            mnt: namespace("/proc/self/ns/mnt"),
        }
    }
}

fn namespace(link: &str) -> Option<String> {
    fs::read_link(link)
        .ok()
        .map(|target| target.to_string_lossy().into_owned())
}
