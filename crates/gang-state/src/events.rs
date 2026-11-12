//! The events of a sliding window that a holder of an older window has not seen yet.

use gang_wire::Event;

/// Returns the events of `window` recorded after the last event of `known`, matched by name.
///
/// Both slices are ordered oldest first. Returns `None` when `known` is empty or when its last
/// event is no longer in `window`, because then the two windows do not overlap and the caller
/// has no overlap to resume from.
pub(crate) fn newer<'a>(known: &[Event], window: &'a [Event]) -> Option<&'a [Event]> {
    let last = known.last()?;
    let index = window.iter().rposition(|event| event.name == last.name)?;
    window.get(index + 1..)
}

#[cfg(test)]
mod tests {
    use gang_clock::Timestamp;
    use gang_wire::{Event, EventChange};

    use super::newer;

    /// A scaling event named `name`, the only field that the rule reads.
    fn event(name: &str) -> Event {
        Event {
            name: name.to_owned(),
            create_time: Timestamp::from_unix_millis(0),
            fleet: "fleets/alpha".to_owned(),
            cause: None,
            change: EventChange::ScaledOut { from: 1, to: 2 },
        }
    }

    fn names(events: &[Event]) -> Vec<&str> {
        events.iter().map(|event| event.name.as_str()).collect()
    }

    #[test]
    fn overlap_returns_the_tail_after_the_last_known_event() {
        let known = [event("events/1"), event("events/2")];
        let window = [event("events/2"), event("events/3"), event("events/4")];
        let tail = newer(&known, &window);
        assert_eq!(tail.map(names), Some(vec!["events/3", "events/4"]));
    }

    #[test]
    fn last_known_event_at_the_end_of_the_window_gives_an_empty_tail() {
        let known = [event("events/1"), event("events/2")];
        let window = [event("events/1"), event("events/2")];
        assert_eq!(newer(&known, &window), Some(&[][..]));
    }

    #[test]
    fn last_known_event_that_left_the_window_gives_none() {
        let known = [event("events/1"), event("events/2")];
        let window = [event("events/3"), event("events/4")];
        assert_eq!(newer(&known, &window), None);
    }

    #[test]
    fn empty_known_gives_none() {
        let window = [event("events/1"), event("events/2")];
        assert_eq!(newer(&[], &window), None);
    }
}
