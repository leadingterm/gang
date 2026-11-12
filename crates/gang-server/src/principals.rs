//! The principals the server refuses, and when it last wrote each principal's sighting to the
//! ledger.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Tick;
use gang_types::Identity;
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::ttl_map::TtlMap;

/// The revoked principals and how many times they changed.
pub(crate) struct Revoked {
    /// How many actual membership changes occurred through [`RevokedPrincipals::revoke`] or
    /// [`RevokedPrincipals::restore`].
    pub(crate) epoch: u64,
    identities: HashSet<Identity>,
}

impl Revoked {
    /// Whether `identity` is revoked.
    #[must_use]
    pub(crate) fn contains(&self, identity: &Identity) -> bool {
        self.identities.contains(identity)
    }
}

/// The principals that have been revoked. Clones share one set and one epoch.
#[derive(Clone)]
pub(crate) struct RevokedPrincipals(Arc<watch::Sender<Revoked>>);

impl RevokedPrincipals {
    /// An empty set: no principal is revoked.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::from_identities(std::iter::empty())
    }

    /// The set holding exactly `identities`.
    #[must_use]
    pub(crate) fn from_identities(identities: impl IntoIterator<Item = Identity>) -> Self {
        let revoked = Revoked {
            epoch: 0,
            identities: identities.into_iter().collect(),
        };
        Self(Arc::new(watch::channel(revoked).0))
    }

    /// How many actual membership changes occurred through [`Self::revoke`] or [`Self::restore`].
    #[must_use]
    pub(crate) fn epoch(&self) -> u64 {
        self.0.borrow().epoch
    }

    /// A receiver that is woken each time the set changes.
    #[must_use]
    pub(crate) fn subscribe(&self) -> watch::Receiver<Revoked> {
        self.0.subscribe()
    }

    /// Whether `identity` is revoked.
    #[must_use]
    pub(crate) fn contains(&self, identity: &Identity) -> bool {
        self.0.borrow().contains(identity)
    }

    /// Marks `identity` revoked.
    pub(crate) fn revoke(&self, identity: Identity) {
        self.0.send_if_modified(|revoked| {
            let changed = revoked.identities.insert(identity);
            if changed {
                revoked.epoch += 1;
            }
            changed
        });
    }

    /// Puts `identity` back in service.
    pub(crate) fn restore(&self, identity: &Identity) {
        self.0.send_if_modified(|revoked| {
            let changed = revoked.identities.remove(identity);
            if changed {
                revoked.epoch += 1;
            }
            changed
        });
    }
}

/// Decides when a principal's sighting must be written to the ledger. When the bound is reached,
/// the oldest tracked principal is evicted and its next sighting may be written early.
pub(crate) struct PrincipalSightings {
    last_written: Mutex<TtlMap<Identity, ()>>,
}

impl PrincipalSightings {
    /// Tracks up to `capacity` principals for `interval` after each write.
    #[must_use]
    pub(crate) fn new(interval: Duration, capacity: NonZeroUsize) -> Self {
        Self {
            last_written: Mutex::new(TtlMap::new(capacity, interval)),
        }
    }

    /// Whether the sighting of `identity` at `now` must be written.
    ///
    /// True when the principal has never been seen, or when `interval` has elapsed since its last
    /// write; a true answer records `now` as the last write.
    #[must_use]
    pub(crate) fn due(&self, identity: &Identity, now: Tick) -> bool {
        let mut last_written = self.last_written.lock();
        if last_written.get_live(identity, now).is_some() {
            return false;
        }
        last_written.insert(identity.clone(), (), now);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_types::{Identity, Principal, ProviderName};

    use super::{PrincipalSightings, RevokedPrincipals};

    fn identity(principal: &str) -> Identity {
        Identity::new(
            ProviderName::try_new("example-idp").expect("provider name is valid"),
            Principal::try_new(principal).expect("principal is valid"),
        )
    }

    #[test]
    fn revoke_restore_and_contains_track_membership() {
        let revoked_principals = RevokedPrincipals::new();
        let ada = identity("ada@example.com");

        assert!(!revoked_principals.contains(&ada));
        revoked_principals.revoke(ada.clone());
        assert!(revoked_principals.contains(&ada));
        revoked_principals.restore(&ada);
        assert!(!revoked_principals.contains(&ada));
    }

    #[test]
    fn from_identities_starts_with_exactly_those_principals() {
        let ada = identity("ada@example.com");
        let grace = identity("grace@example.com");
        let revoked_principals = RevokedPrincipals::from_identities([ada.clone()]);

        assert!(revoked_principals.contains(&ada));
        assert!(!revoked_principals.contains(&grace));
    }

    #[test]
    fn clones_share_one_set() {
        let revoked_principals = RevokedPrincipals::new();
        let shared_revoked_principals = revoked_principals.clone();
        let ada = identity("ada@example.com");

        shared_revoked_principals.revoke(ada.clone());
        assert!(revoked_principals.contains(&ada));
        revoked_principals.restore(&ada);
        assert!(!shared_revoked_principals.contains(&ada));
    }

    #[test]
    fn epoch_advances_on_every_change_and_is_shared() {
        let revoked_principals = RevokedPrincipals::new();
        let shared_revoked_principals = revoked_principals.clone();
        let ada = identity("ada@example.com");
        let mut epochs = revoked_principals.subscribe();

        assert_eq!(revoked_principals.epoch(), 0);
        shared_revoked_principals.revoke(ada.clone());
        assert_eq!(revoked_principals.epoch(), 1);
        assert!(epochs.has_changed().expect("sender is alive"));
        revoked_principals.restore(&ada);
        assert_eq!(shared_revoked_principals.epoch(), 2);
        assert_eq!(epochs.borrow_and_update().epoch, 2);
        assert!(!epochs.has_changed().expect("sender is alive"));
    }

    #[test]
    fn duplicate_revoke_and_restore_do_not_notify() {
        let revoked_principals = RevokedPrincipals::new();
        let ada = identity("ada@example.com");
        revoked_principals.revoke(ada.clone());
        let mut changes = revoked_principals.subscribe();

        revoked_principals.revoke(ada.clone());
        assert_eq!(revoked_principals.epoch(), 1);
        assert!(!changes.has_changed().expect("sender is alive"));

        revoked_principals.restore(&ada);
        changes.borrow_and_update();
        revoked_principals.restore(&ada);
        assert_eq!(revoked_principals.epoch(), 2);
        assert!(!changes.has_changed().expect("sender is alive"));
    }

    #[test]
    fn first_sight_is_due_then_the_interval_gates_writes() {
        let sightings = PrincipalSightings::new(
            Duration::from_mins(1),
            NonZeroUsize::new(2).expect("two is non-zero"),
        );
        let ada = identity("ada@example.com");
        let start = Tick::ORIGIN;

        assert!(sightings.due(&ada, start));
        assert!(!sightings.due(&ada, start + Duration::from_secs(59)));
        assert!(sightings.due(&ada, start + Duration::from_mins(1)));
        assert!(!sightings.due(&ada, start + Duration::from_secs(61)));
    }

    #[test]
    fn principals_are_tracked_independently() {
        let sightings = PrincipalSightings::new(
            Duration::from_mins(1),
            NonZeroUsize::new(2).expect("two is non-zero"),
        );
        let ada = identity("ada@example.com");
        let grace = identity("grace@example.com");
        let start = Tick::ORIGIN;

        assert!(sightings.due(&ada, start));
        assert!(sightings.due(&grace, start));
    }

    #[test]
    fn capacity_evicts_the_oldest_sighting() {
        let sightings = PrincipalSightings::new(
            Duration::from_mins(1),
            std::num::NonZeroUsize::new(2).expect("two is non-zero"),
        );
        let now = gang_clock::Tick::ORIGIN;
        let ada = identity("ada@example.com");
        let grace = identity("grace@example.com");
        let lin = identity("lin@example.com");

        assert!(sightings.due(&ada, now));
        assert!(sightings.due(&grace, now));
        assert!(sightings.due(&lin, now));
        assert!(sightings.due(&ada, now));
    }

    #[test]
    fn monotonic_interval_is_unaffected_by_a_backward_wall_step() {
        use std::sync::atomic::{AtomicU64, Ordering};

        use gang_clock::{Clock, TestClock};

        /// A test clock whose wall reading can be stepped independently.
        struct SteppingClock {
            monotonic: TestClock,
            wall_millis: AtomicU64,
        }

        impl Clock for SteppingClock {
            fn wall(&self) -> Timestamp {
                Timestamp::from_unix_millis(self.wall_millis.load(Ordering::Relaxed))
            }

            fn monotonic(&self) -> Tick {
                self.monotonic.monotonic()
            }
        }

        let clock = SteppingClock {
            monotonic: TestClock::new(Timestamp::from_unix_millis(1_000_000)),
            wall_millis: AtomicU64::new(1_000_000),
        };
        let sightings = PrincipalSightings::new(
            Duration::from_mins(1),
            std::num::NonZeroUsize::new(2).expect("two is non-zero"),
        );
        let ada = identity("ada@example.com");
        let first_wall = clock.wall();
        assert!(sightings.due(&ada, clock.monotonic()));
        clock.wall_millis.store(900_000, Ordering::Relaxed);
        clock.monotonic.advance(Duration::from_mins(1));
        assert!(clock.wall() < first_wall);
        assert!(sightings.due(&ada, clock.monotonic()));
    }
}
