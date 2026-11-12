//! The browser sign-ins that have started and not yet completed, keyed by their OAuth `state`.
//! Expiry is read from the monotonic clock, so a wall-clock step cannot end or extend a sign-in.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Tick;
use gang_types::LoginSession;

use crate::ttl_map::TtlMap;

/// A browser sign-in that has started and awaits its callback.
pub(crate) struct Attempt {
    /// The plugin's state for the sign-in.
    pub(crate) session: LoginSession,
    /// The path the browser is sent to after a successful sign-in.
    pub(crate) next: http::HeaderValue,
}

/// A bounded table of pending sign-ins that expire after a fixed time to live. The cache's order
/// is the insertion order, which is also the expiry order because the time to live is fixed.
pub(crate) struct LoginAttempts {
    entries: TtlMap<String, Attempt>,
}

impl LoginAttempts {
    /// Creates an empty table that holds at most `capacity` attempts for `ttl` each.
    pub(crate) fn new(capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            entries: TtlMap::new(capacity, ttl),
        }
    }

    /// Stores `attempt` under `state` until `now + ttl`. When full, drops expired attempts first,
    /// then the oldest. Re-inserting a state replaces it.
    pub(crate) fn insert(&mut self, state: String, attempt: Attempt, now: Tick) {
        self.entries.insert(state, attempt, now);
    }

    /// Removes and returns the attempt under `state`; `None` when absent or expired at `now`
    /// (an expired attempt is removed).
    pub(crate) fn take(&mut self, state: &str, now: Tick) -> Option<Attempt> {
        self.entries.take_live(state, now)
    }

    /// The number of stored attempts, expired ones included until they are evicted or taken.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use gang_clock::Tick;
    use gang_types::LoginSession;

    use super::{Attempt, LoginAttempts};

    const TTL: Duration = Duration::from_secs(10);

    fn at(seconds: u64) -> Tick {
        Tick::ORIGIN + Duration::from_secs(seconds)
    }

    fn table(capacity: usize) -> LoginAttempts {
        LoginAttempts::new(
            NonZeroUsize::new(capacity).expect("test capacity is non-zero"),
            TTL,
        )
    }

    /// An attempt whose `next` names it, so a taken attempt identifies the one stored.
    fn attempt(next: &str) -> Attempt {
        Attempt {
            session: LoginSession::new(format!("session-{next}")),
            next: http::HeaderValue::from_str(next).expect("test path is a valid header value"),
        }
    }

    #[test]
    fn capacity_is_never_exceeded() {
        let mut attempts = table(2);
        for n in 0..5 {
            attempts.insert(format!("state-{n}"), attempt("/ui"), at(n));
            assert!(attempts.len() <= 2);
        }
        assert_eq!(attempts.len(), 2);
    }

    #[test]
    fn expired_attempts_are_evicted_before_the_oldest_live_one() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));
        attempts.insert("b".into(), attempt("b"), at(5));
        attempts.insert("c".into(), attempt("c"), at(11));

        assert_eq!(attempts.len(), 2);
        assert_eq!(
            attempts.take("b", at(11)).map(|a| a.next),
            Some(http::HeaderValue::from_static("b"))
        );
        assert_eq!(
            attempts.take("c", at(11)).map(|a| a.next),
            Some(http::HeaderValue::from_static("c"))
        );
    }

    #[test]
    fn oldest_attempt_is_evicted_when_all_are_live() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));
        attempts.insert("b".into(), attempt("b"), at(1));
        attempts.insert("c".into(), attempt("c"), at(2));

        assert!(attempts.take("a", at(2)).is_none());
        assert_eq!(
            attempts.take("b", at(2)).map(|a| a.next),
            Some(http::HeaderValue::from_static("b"))
        );
        assert_eq!(
            attempts.take("c", at(2)).map(|a| a.next),
            Some(http::HeaderValue::from_static("c"))
        );
    }

    #[test]
    fn take_is_single_use() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));

        assert!(attempts.take("a", at(1)).is_some());
        assert!(attempts.take("a", at(1)).is_none());
    }

    #[test]
    fn take_after_ttl_is_none_and_removes_the_attempt() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));

        assert!(attempts.take("a", at(10)).is_none());
        assert_eq!(attempts.len(), 0);
    }

    #[test]
    fn replacing_a_state_does_not_grow_the_table() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));
        attempts.insert("b".into(), attempt("b"), at(1));
        attempts.insert("a".into(), attempt("a2"), at(2));

        assert_eq!(attempts.len(), 2);
        assert_eq!(
            attempts.take("a", at(2)).map(|a| a.next),
            Some(http::HeaderValue::from_static("a2"))
        );
    }

    #[test]
    fn replacing_a_state_refreshes_its_order_and_expiry() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));
        attempts.insert("b".into(), attempt("b"), at(1));
        attempts.insert("a".into(), attempt("a2"), at(9));

        // The replacement expires at 19s, so it survives past the original 10s expiry.
        assert_eq!(
            attempts.take("a", at(15)).map(|a| a.next),
            Some(http::HeaderValue::from_static("a2"))
        );
    }

    #[test]
    fn replacing_a_state_moves_it_to_the_back_of_the_eviction_order() {
        let mut attempts = table(2);
        attempts.insert("a".into(), attempt("a"), at(0));
        attempts.insert("b".into(), attempt("b"), at(1));
        attempts.insert("a".into(), attempt("a2"), at(2));
        attempts.insert("c".into(), attempt("c"), at(3));

        assert!(attempts.take("b", at(3)).is_none());
        assert_eq!(
            attempts.take("a", at(3)).map(|a| a.next),
            Some(http::HeaderValue::from_static("a2"))
        );
        assert_eq!(
            attempts.take("c", at(3)).map(|a| a.next),
            Some(http::HeaderValue::from_static("c"))
        );
    }

    #[test]
    fn len_tracks_removals_including_lazy_expiry() {
        let mut attempts = table(4);
        attempts.insert("a".into(), attempt("a"), at(0));
        attempts.insert("b".into(), attempt("b"), at(1));
        attempts.insert("c".into(), attempt("c"), at(2));
        assert_eq!(attempts.len(), 3);

        assert!(attempts.take("b", at(2)).is_some());
        assert_eq!(attempts.len(), 2);

        assert!(attempts.take("a", at(10)).is_none());
        assert_eq!(attempts.len(), 1);

        assert!(attempts.take("missing", at(2)).is_none());
        assert_eq!(attempts.len(), 1);
    }
}
