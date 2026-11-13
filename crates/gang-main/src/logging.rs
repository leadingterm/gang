//! Process-wide tracing subscriber that writes to stderr.

use std::ffi::OsStr;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::SubscriberInitExt;

use crate::errors::{MainError, Result};

/// Environment variable holding the log filter directive.
pub(crate) const LOG_ENV: &str = "GANG_LOG";
/// Filter directive used when `LOG_ENV` is unset or invalid.
const DEFAULT_DIRECTIVE: &str = "info";

/// Installs the global subscriber; fails if one is already set.
///
/// `ansi` is the color decision of stderr, where the logs go: when it is false the logs never
/// hold escape codes.
pub(crate) fn init(ansi: bool) -> Result<()> {
    subscriber(
        ansi,
        filter(std::env::var_os(LOG_ENV).as_deref()),
        std::io::stderr,
    )
    .try_init()
    .map_err(|error| MainError::Logging(Box::new(error)))
}

/// Builds the filter from the value of `LOG_ENV`.
///
/// A valid directive is used as given; an absent, non-UTF-8, or invalid one falls back to
/// `DEFAULT_DIRECTIVE`.
fn filter(directive: Option<&OsStr>) -> EnvFilter {
    directive
        .and_then(OsStr::to_str)
        .and_then(|directive| EnvFilter::try_new(directive).ok())
        .unwrap_or_else(|| EnvFilter::new(DEFAULT_DIRECTIVE))
}

/// Builds a formatting subscriber that writes events to `writer`.
///
/// `ansi` controls whether the output carries escape codes.
fn subscriber<W>(ansi: bool, filter: EnvFilter, writer: W) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(ansi)
        .with_writer(writer)
        .finish()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tracing_subscriber::fmt::MakeWriter;

    use super::{filter, subscriber};

    /// In-memory sink shared between the subscriber and the test body.
    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl SharedBuffer {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock()).into_owned()
        }
    }

    impl io::Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedBuffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn plain_output_has_no_escape_codes() {
        let buffer = SharedBuffer::default();
        let sub = subscriber(false, filter(None), buffer.clone());
        gang_testkit::with_subscriber(sub, || {
            tracing::info!("plain message marker");
        });
        let output = buffer.contents();
        assert!(output.contains("plain message marker"), "{output:?}");
        assert!(!output.contains('\u{1b}'), "{output:?}");
    }

    #[test]
    fn ansi_output_has_escape_codes() {
        let buffer = SharedBuffer::default();
        let sub = subscriber(true, filter(None), buffer.clone());
        gang_testkit::with_subscriber(sub, || {
            tracing::info!("ansi message marker");
        });
        let output = buffer.contents();
        assert!(output.contains("ansi message marker"), "{output:?}");
        assert!(output.contains("\u{1b}["), "{output:?}");
    }

    #[test]
    fn warn_directive_drops_info_and_keeps_warn() {
        let buffer = SharedBuffer::default();
        let sub = subscriber(false, filter(Some(OsStr::new("warn"))), buffer.clone());
        gang_testkit::with_subscriber(sub, || {
            tracing::info!("dropped info marker");
            tracing::warn!("kept warn marker");
        });
        let output = buffer.contents();
        assert!(!output.contains("dropped info marker"), "{output:?}");
        assert!(output.contains("kept warn marker"), "{output:?}");
    }

    #[test]
    fn invalid_directive_falls_back_to_info() {
        let buffer = SharedBuffer::default();
        let sub = subscriber(
            false,
            filter(Some(OsStr::new("not a [valid directive"))),
            buffer.clone(),
        );
        gang_testkit::with_subscriber(sub, || {
            tracing::info!("fallback info marker");
        });
        let output = buffer.contents();
        assert!(output.contains("fallback info marker"), "{output:?}");
    }

    #[test]
    fn non_utf8_directive_falls_back_to_info() {
        let buffer = SharedBuffer::default();
        let sub = subscriber(
            false,
            filter(Some(OsStr::from_bytes(b"\xff"))),
            buffer.clone(),
        );
        gang_testkit::with_subscriber(sub, || {
            tracing::info!("non-utf8 fallback marker");
        });
        let output = buffer.contents();
        assert!(output.contains("non-utf8 fallback marker"), "{output:?}");
    }
}
