//! The ordered calls a test double received, shared by the doubles of one test.

use std::sync::Arc;

use parking_lot::Mutex;

/// The ordered calls the test doubles of one test received; clones share the log.
#[derive(Debug, Clone, Default)]
pub struct CallLog(Arc<Mutex<Vec<&'static str>>>);

impl CallLog {
    /// Appends `call` to the log.
    pub fn record(&self, call: &'static str) {
        self.0.lock().push(call);
    }

    /// Returns the calls recorded so far, in the order they were received.
    #[must_use]
    pub fn calls(&self) -> Vec<&'static str> {
        self.0.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::CallLog;

    #[test]
    fn calls_are_kept_in_the_order_they_were_recorded() {
        let log = CallLog::default();
        log.record("first");
        log.record("second");

        assert_eq!(log.calls(), ["first", "second"]);
    }

    #[test]
    fn clones_share_one_log() {
        let log = CallLog::default();
        let other = log.clone();
        other.record("call");

        assert_eq!(log.calls(), ["call"]);
    }
}
