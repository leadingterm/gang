use std::collections::HashMap;
use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use gang_types::CertSerial;
use parking_lot::Mutex;

/// The set of revoked certificate serials, shared by every clone.
///
/// Revocation is permanent: a revoked serial stays revoked for the life of the set. Every
/// [`RevocationWatch`] of a serial is woken when that serial is revoked.
#[derive(Debug, Clone, Default)]
pub struct RevocationSet {
    state: Arc<Mutex<State>>,
}

/// Serials with a live watch or a revocation are kept; other entries are removed on drop.
#[derive(Debug, Default)]
struct State {
    entries: HashMap<CertSerial, Arc<SerialLatch>>,
}

/// A permanent revocation flag and independent read/write wakers for each watch.
#[derive(Debug, Default)]
struct SerialLatch {
    revoked: AtomicBool,
    wakers: Mutex<Vec<Option<[Option<Waker>; 2]>>>,
}

impl RevocationSet {
    /// Creates a set in which every serial in `serials` is already revoked.
    #[must_use]
    pub fn from_serials(serials: impl IntoIterator<Item = CertSerial>) -> Self {
        let set = Self::default();
        for serial in serials {
            set.revoke(serial);
        }
        set
    }

    /// Revokes `serial`, waking every watch of it. Revoking twice has no further effect.
    pub fn revoke(&self, serial: CertSerial) {
        let mut state = self.state.lock();
        let latch = state.entries.entry(serial).or_default();
        let mut slots = latch.wakers.lock();
        latch.revoked.store(true, Ordering::Release);
        let wakers = std::mem::take(&mut *slots);
        drop(slots);
        drop(state);
        for pair in wakers.into_iter().flatten() {
            for waker in pair.into_iter().flatten() {
                waker.wake();
            }
        }
    }

    /// Whether `serial` has been revoked.
    #[must_use]
    pub fn is_revoked(&self, serial: &CertSerial) -> bool {
        self.state
            .lock()
            .entries
            .get(serial)
            .is_some_and(|latch| latch.revoked.load(Ordering::Acquire))
    }

    /// Observes `serial` until it is revoked. Already-revoked serials are immediately ready.
    #[must_use]
    pub fn watch(&self, serial: &CertSerial) -> RevocationWatch {
        let mut state = self.state.lock();
        let latch = state.entries.entry(serial.clone()).or_default();
        let slot = if latch.revoked.load(Ordering::Acquire) {
            usize::MAX
        } else {
            let mut slots = latch.wakers.lock();
            if let Some(index) = slots.iter().position(Option::is_none) {
                slots[index] = Some([None, None]);
                index
            } else {
                slots.push(Some([None, None]));
                slots.len() - 1
            }
        };
        RevocationWatch {
            serial: serial.clone(),
            latch: Arc::clone(latch),
            set: Arc::clone(&self.state),
            slot,
        }
    }

    #[cfg(test)]
    fn entries(&self) -> usize {
        self.state.lock().entries.len()
    }
}

/// Observes the revocation of one certificate serial.
///
/// Dropping the watch releases its hold on the serial; an unrevoked serial with no remaining
/// watch is forgotten.
pub struct RevocationWatch {
    serial: CertSerial,
    latch: Arc<SerialLatch>,
    set: Arc<Mutex<State>>,
    slot: usize,
}

impl RevocationWatch {
    /// The certificate serial this watch observes.
    #[must_use]
    pub fn serial(&self) -> &CertSerial {
        &self.serial
    }

    /// Whether the serial has been revoked.
    #[must_use]
    pub fn is_revoked(&self) -> bool {
        self.latch.revoked.load(Ordering::Acquire)
    }

    /// Resolves once the serial is revoked, immediately if it already is.
    ///
    /// Cancel-safe: dropping the future loses no revocation.
    pub async fn revoked(&mut self) {
        poll_fn(|cx| self.poll_read_revoked(cx)).await;
    }

    /// Polls the read direction independently of the write direction.
    pub(crate) fn poll_read_revoked(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_slot(0, cx)
    }

    /// Polls the write direction independently of the read direction.
    pub(crate) fn poll_write_revoked(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_slot(1, cx)
    }

    /// Registers a direction's latest waker without losing a concurrent revocation.
    fn poll_slot(&self, direction: usize, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_revoked() {
            return Poll::Ready(());
        }
        let mut slots = self.latch.wakers.lock();
        if self.is_revoked() {
            return Poll::Ready(());
        }
        let previous = if let Some(Some(pair)) = slots.get_mut(self.slot) {
            let waker = &mut pair[direction];
            if waker.as_ref().is_some_and(|old| old.will_wake(cx.waker())) {
                None
            } else {
                waker.replace(cx.waker().clone())
            }
        } else {
            None
        };
        drop(slots);
        drop(previous);
        Poll::Pending
    }

    /// Counts registered wakers for drop regression tests.
    #[cfg(test)]
    pub(crate) fn waker_count(&self) -> usize {
        self.latch
            .wakers
            .lock()
            .iter()
            .flatten()
            .flat_map(|pair| pair.iter())
            .filter(|waker| waker.is_some())
            .count()
    }
}

impl fmt::Debug for RevocationWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RevocationWatch")
            .field("serial", &self.serial)
            .field("revoked", &self.is_revoked())
            .finish_non_exhaustive()
    }
}

impl Drop for RevocationWatch {
    fn drop(&mut self) {
        let mut state = self.set.lock();
        let wakers = self
            .latch
            .wakers
            .lock()
            .get_mut(self.slot)
            .and_then(Option::take);
        if !self.is_revoked() && Arc::strong_count(&self.latch) == 2 {
            state.entries.remove(&self.serial);
        }
        drop(state);
        drop(wakers);
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use gang_types::CertSerial;
    use tokio::sync::oneshot;

    use super::{RevocationSet, RevocationWatch};
    use gang_testkit::park_then_signal;

    fn serial(hex: &str) -> CertSerial {
        CertSerial::try_new(hex).expect("fixture serial is lowercase hex")
    }

    /// True when `revoked()` does not resolve immediately.
    async fn is_pending(watch: &mut RevocationWatch) -> bool {
        tokio::select! {
            biased;
            () = watch.revoked() => false,
            () = std::future::ready(()) => true,
        }
    }

    /// True when `revoked()` resolves on its first poll.
    async fn is_ready(watch: &mut RevocationWatch) -> bool {
        tokio::select! {
            biased;
            () = watch.revoked() => true,
            () = std::future::ready(()) => false,
        }
    }

    #[derive(Default)]
    struct CountWakes(AtomicUsize);

    impl CountWakes {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn from_serials_revokes_exactly_the_given_serials() {
        let set = RevocationSet::from_serials([serial("aa"), serial("bb")]);

        assert!(set.is_revoked(&serial("aa")));
        assert!(set.is_revoked(&serial("bb")));
        assert!(!set.is_revoked(&serial("cc")));
    }

    #[test]
    fn revoke_is_idempotent() {
        let set = RevocationSet::default();

        set.revoke(serial("aa"));
        set.revoke(serial("aa"));

        assert!(set.is_revoked(&serial("aa")));
        assert_eq!(set.entries(), 1);
    }

    #[test]
    fn clones_share_state() {
        let set = RevocationSet::default();
        let clone = set.clone();

        clone.revoke(serial("aa"));

        assert!(set.is_revoked(&serial("aa")));
    }

    #[test]
    fn watch_reports_revocation_of_its_serial() {
        let set = RevocationSet::default();
        let watch = set.watch(&serial("aa"));
        assert!(!watch.is_revoked());

        set.revoke(serial("aa"));

        assert!(watch.is_revoked());
    }

    #[tokio::test]
    async fn watch_resolves_on_revoke() {
        let set = RevocationSet::default();
        let mut watch = set.watch(&serial("aa"));
        assert!(is_pending(&mut watch).await);

        let (armed_tx, armed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            park_then_signal(watch.revoked(), armed_tx).await;
        });
        armed_rx.await.expect("watch task parks before revocation");
        set.revoke(serial("aa"));

        task.await.expect("watch task completes after revocation");
    }

    #[tokio::test]
    async fn watch_after_revoke_resolves_immediately() {
        let set = RevocationSet::from_serials([serial("aa")]);
        let mut watch = set.watch(&serial("aa"));

        assert!(is_ready(&mut watch).await);
    }

    #[tokio::test]
    async fn unrelated_serial_stays_pending() {
        let set = RevocationSet::default();
        let mut watch = set.watch(&serial("bb"));

        set.revoke(serial("aa"));

        assert!(is_pending(&mut watch).await);
        assert!(!watch.is_revoked());
    }

    #[tokio::test]
    async fn many_watches_of_one_serial_all_wake() {
        let set = RevocationSet::default();
        let mut armed = Vec::new();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let mut watch = set.watch(&serial("aa"));
                let (armed_tx, armed_rx) = oneshot::channel();
                armed.push(armed_rx);
                tokio::spawn(async move {
                    park_then_signal(watch.revoked(), armed_tx).await;
                })
            })
            .collect();
        for armed_rx in armed {
            armed_rx
                .await
                .expect("every watch task parks before revocation");
        }

        set.revoke(serial("aa"));

        for task in tasks {
            task.await
                .expect("every watch task completes after revocation");
        }
    }

    #[tokio::test]
    async fn poll_read_revoked_pends_then_wakes_on_revoke() {
        let set = RevocationSet::default();
        let mut watch = set.watch(&serial("aa"));

        let first = poll_fn(|cx| Poll::Ready(watch.poll_read_revoked(cx))).await;
        assert!(first.is_pending());

        let (armed_tx, armed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            park_then_signal(poll_fn(|cx| watch.poll_read_revoked(cx)), armed_tx).await;
        });
        armed_rx.await.expect("poll task parks before revocation");
        set.revoke(serial("aa"));

        task.await.expect("poll task completes after revocation");
    }

    #[tokio::test]
    async fn poll_write_revoked_pends_then_wakes_on_revoke() {
        let set = RevocationSet::default();
        let mut watch = set.watch(&serial("aa"));

        let first = poll_fn(|cx| Poll::Ready(watch.poll_write_revoked(cx))).await;
        assert!(first.is_pending());

        let (armed_tx, armed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            park_then_signal(poll_fn(|cx| watch.poll_write_revoked(cx)), armed_tx).await;
        });
        armed_rx.await.expect("poll task parks before revocation");
        set.revoke(serial("aa"));

        task.await.expect("poll task completes after revocation");
    }

    #[tokio::test]
    async fn poll_revoked_stays_ready_after_revoke_in_both_directions() {
        let set = RevocationSet::from_serials([serial("aa")]);
        let mut watch = set.watch(&serial("aa"));

        let read = poll_fn(|cx| Poll::Ready(watch.poll_read_revoked(cx))).await;
        let write = poll_fn(|cx| Poll::Ready(watch.poll_write_revoked(cx))).await;
        let read_again = poll_fn(|cx| Poll::Ready(watch.poll_read_revoked(cx))).await;
        let write_again = poll_fn(|cx| Poll::Ready(watch.poll_write_revoked(cx))).await;

        assert!(read.is_ready());
        assert!(write.is_ready());
        assert!(read_again.is_ready());
        assert!(write_again.is_ready());
    }

    #[test]
    fn read_and_write_slots_wake_independently() {
        let set = RevocationSet::default();
        let mut watch = set.watch(&serial("aa"));
        let read_hits = Arc::new(CountWakes::default());
        let write_hits = Arc::new(CountWakes::default());
        let read_waker = Waker::from(Arc::clone(&read_hits));
        let write_waker = Waker::from(Arc::clone(&write_hits));

        assert!(
            watch
                .poll_read_revoked(&mut Context::from_waker(&read_waker))
                .is_pending()
        );
        assert!(
            watch
                .poll_write_revoked(&mut Context::from_waker(&write_waker))
                .is_pending()
        );

        set.revoke(serial("aa"));

        assert!(read_hits.count() > 0, "read waker is woken by revocation");
        assert!(write_hits.count() > 0, "write waker is woken by revocation");
    }

    #[tokio::test]
    async fn dropped_wait_loses_no_revocation() {
        let set = RevocationSet::default();
        let mut watch = set.watch(&serial("aa"));
        assert!(is_pending(&mut watch).await);

        set.revoke(serial("aa"));

        assert!(is_ready(&mut watch).await);
    }

    #[test]
    fn dropping_last_unrevoked_watch_prunes_its_entry() {
        let set = RevocationSet::default();
        let watch = set.watch(&serial("aa"));
        assert_eq!(set.entries(), 1);

        drop(watch);

        assert_eq!(set.entries(), 0);
    }

    #[test]
    fn entry_stays_until_every_watch_of_the_serial_is_dropped() {
        let set = RevocationSet::default();
        let first = set.watch(&serial("aa"));
        let second = set.watch(&serial("aa"));

        drop(first);
        assert_eq!(set.entries(), 1);

        drop(second);
        assert_eq!(set.entries(), 0);
    }

    #[test]
    fn revoked_entries_are_kept_after_watches_drop() {
        let set = RevocationSet::default();
        set.revoke(serial("aa"));
        assert_eq!(set.entries(), 1);

        let watch = set.watch(&serial("aa"));
        drop(watch);

        assert_eq!(set.entries(), 1);
        assert!(set.is_revoked(&serial("aa")));
    }

    #[test]
    fn revoking_a_watched_serial_keeps_its_entry_after_the_watch_drops() {
        let set = RevocationSet::default();
        let watch = set.watch(&serial("aa"));
        set.revoke(serial("aa"));

        drop(watch);

        assert_eq!(set.entries(), 1);
        assert!(set.is_revoked(&serial("aa")));
    }
}
