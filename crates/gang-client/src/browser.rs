//! Detects whether this process can open a local browser.

use std::ffi::OsString;

/// Availability of a browser on the machine running the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBrowser {
    /// A browser can be opened locally.
    Available,
    /// No local graphical browser is available.
    Unavailable,
}

impl LocalBrowser {
    /// Detects browser availability from the process environment.
    #[must_use]
    pub fn detect(env: impl Fn(&str) -> Option<OsString>) -> Self {
        let set = |name| env(name).is_some_and(|value| !value.is_empty());
        #[cfg(target_os = "macos")]
        let available = !set("SSH_CONNECTION");
        #[cfg(not(target_os = "macos"))]
        let available = set("DISPLAY") || set("WAYLAND_DISPLAY");
        if available {
            Self::Available
        } else {
            Self::Unavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LocalBrowser;
    use std::ffi::OsString;

    #[test]
    fn local_browser_detection_follows_the_platform_rule() {
        let detect = |values: &[(&str, &str)]| {
            LocalBrowser::detect(|key| {
                values
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| OsString::from(value))
            })
        };
        #[cfg(target_os = "macos")]
        {
            assert_eq!(detect(&[]), LocalBrowser::Available);
            assert_eq!(detect(&[("SSH_CONNECTION", "")]), LocalBrowser::Available);
            assert_eq!(
                detect(&[("SSH_CONNECTION", "remote")]),
                LocalBrowser::Unavailable
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(detect(&[]), LocalBrowser::Unavailable);
            assert_eq!(
                detect(&[("DISPLAY", ""), ("WAYLAND_DISPLAY", "")]),
                LocalBrowser::Unavailable
            );
            assert_eq!(detect(&[("DISPLAY", ":0")]), LocalBrowser::Available);
            assert_eq!(
                detect(&[("WAYLAND_DISPLAY", "wayland-0")]),
                LocalBrowser::Available
            );
        }
    }
}
