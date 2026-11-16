//! Counts the connections a listener refuses and decides when to log them, so refusals under load
//! produce at most one log line per interval.

use std::fmt;
use std::num::NonZeroUsize;

use gang_clock::Tick;
use gang_types::NonZeroDuration;

use crate::serve::Listener;

/// The connections one listener has refused, and when it may next log them.
#[derive(Debug)]
pub(crate) struct Refusals {
    /// The listener whose refusals these are.
    listener: Listener,
    /// The listener's connection limit.
    max_connections: NonZeroUsize,
    /// Shortest time between two log lines.
    interval: NonZeroDuration,
    /// Refusals since the listener started.
    total: u64,
    /// Refusals since the last log line.
    unlogged: u64,
    /// When the next line may be logged; `None` until the first refusal.
    next_line: Option<Tick>,
}

/// What one log line of refusals reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RefusalLine {
    /// The listener whose refusals these are.
    pub(crate) listener: Listener,
    /// The listener's connection limit.
    pub(crate) max_connections: NonZeroUsize,
    /// Refusals since the previous line, this one included.
    pub(crate) since_last: u64,
    /// Refusals since the listener started.
    pub(crate) total: u64,
}

impl Refusals {
    /// No refusals yet on `listener`, whose limit is `max_connections`; lines at most once per `interval`.
    pub(crate) const fn new(
        listener: Listener,
        max_connections: NonZeroUsize,
        interval: NonZeroDuration,
    ) -> Self {
        Self {
            listener,
            max_connections,
            interval,
            total: 0,
            unlogged: 0,
            next_line: None,
        }
    }

    /// Counts a refusal at `now`, and returns the line to log unless one was logged less than an
    /// interval before `now`.
    pub(crate) fn refuse(&mut self, now: Tick) -> Option<RefusalLine> {
        self.total = self.total.saturating_add(1);
        self.unlogged = self.unlogged.saturating_add(1);
        if self.next_line.is_some_and(|due| now < due) {
            return None;
        }
        self.next_line = Some(now + self.interval.get());
        let line = RefusalLine {
            listener: self.listener,
            max_connections: self.max_connections,
            since_last: self.unlogged,
            total: self.total,
        };
        self.unlogged = 0;
        Some(line)
    }
}

impl fmt::Display for RefusalLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the {} listener refused {} connections beyond its limit of {} since its previous line, {} in all",
            self.listener.name(),
            self.since_last,
            self.max_connections,
            self.total
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(60);
    const LIMIT: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1023);

    /// The reading `seconds` after the clock's origin.
    fn at(seconds: u64) -> Tick {
        Tick::from_elapsed(Duration::from_secs(seconds))
    }

    /// The line a refusal logs.
    const fn line(since_last: u64, total: u64) -> RefusalLine {
        RefusalLine {
            listener: Listener::Public,
            max_connections: LIMIT,
            since_last,
            total,
        }
    }

    #[test]
    fn the_first_refusal_is_logged_then_at_most_one_line_per_interval() {
        let mut refusals = Refusals::new(Listener::Public, LIMIT, INTERVAL);
        assert_eq!(refusals.refuse(at(0)), Some(line(1, 1)));
        assert_eq!(refusals.refuse(at(1)), None);
        assert_eq!(refusals.refuse(at(59)), None);
        assert_eq!(refusals.refuse(at(60)), Some(line(3, 4)));
        assert_eq!(refusals.refuse(at(119)), None);
        assert_eq!(refusals.refuse(at(500)), Some(line(2, 6)));
    }

    #[test]
    fn a_line_names_the_listener_its_limit_and_both_counts() {
        let mut refusals = Refusals::new(Listener::Worker, LIMIT, INTERVAL);
        refusals.refuse(at(0));
        refusals.refuse(at(1));
        let line = refusals.refuse(at(60)).map(|line| line.to_string());
        assert_eq!(
            line.as_deref(),
            Some(
                "the worker listener refused 2 connections beyond its limit of 1024 since its previous line, 3 in all"
            )
        );
    }
}
