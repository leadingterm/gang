use std::sync::Arc;

use gang_clock::{Clock, Timestamp};
use gang_types::NonZeroDuration;
use parking_lot::Mutex;

use crate::errors::OidcError;

/// A remote document fetched lazily, trusted for a time-to-live, and refreshed once however many
/// callers ask.
///
/// The fetch always runs outside the state lock, so callers that need no fetch never wait for
/// one. A document that cannot be refreshed stays served; a failed fetch is not repeated for the
/// retry interval. When no document is held, concurrent callers wait for the one fetch in flight.
pub(crate) struct RefreshCache<T> {
    state: Mutex<State<T>>,
    /// Held for the length of a fetch, so only one runs at a time.
    turn: tokio::sync::Mutex<()>,
    ttl: NonZeroDuration,
    retry_interval: NonZeroDuration,
    /// The error for a caller refused by the backoff without a usable document.
    unavailable: fn() -> OidcError,
}

struct State<T> {
    fetched: Option<(Arc<T>, Timestamp)>,
    last_failure: Option<Timestamp>,
    last_forced: Option<Timestamp>,
}

/// Why a refresh is wanted.
#[derive(Clone, Copy)]
enum Want {
    /// The document is absent or past its time-to-live.
    Fresh,
    /// The held document lacks something the caller needs, and may be replaced if the last such
    /// refresh is at least this old.
    Replacement(NonZeroDuration),
}

/// What the state allows for one caller.
enum Verdict<T> {
    /// Serve this document without fetching.
    Serve(Arc<T>),
    /// The issuer is being left alone after a failure; fresh requests may use a stale document.
    Hold(Option<Arc<T>>),
    /// Fetch; the stale document, if any, is the fallback.
    Fetch(Option<Arc<T>>),
}

impl<T> RefreshCache<T> {
    pub(crate) fn new(
        ttl: NonZeroDuration,
        retry_interval: NonZeroDuration,
        unavailable: fn() -> OidcError,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                fetched: None,
                last_failure: None,
                last_forced: None,
            }),
            turn: tokio::sync::Mutex::new(()),
            ttl,
            retry_interval,
            unavailable,
        }
    }

    /// Returns the document, fetching it when none is held or the held one is past its
    /// time-to-live. A stale document is served instead of waiting for, or after a failure of,
    /// the fetch.
    ///
    /// # Errors
    ///
    /// The fetch's error when no document is held and the fetch fails, or the cache's
    /// unavailable error for callers refused by the backoff.
    pub(crate) async fn get<F>(
        &self,
        clock: &impl Clock,
        fetch: impl FnOnce() -> F,
    ) -> Result<Arc<T>, OidcError>
    where
        F: Future<Output = Result<T, OidcError>>,
    {
        self.refresh(clock, Want::Fresh, fetch).await
    }

    /// Replaces the held document because it lacks something the caller needs, unless the
    /// previous replacement is younger than `spacing`, in which case the held document is
    /// returned. A caller that arrives while a fetch is in flight waits for it and shares its
    /// result.
    ///
    /// # Errors
    ///
    /// The fetch's error when it fails, or the cache's unavailable error while backing off
    /// after a failed fetch; the held document stays served to everyone else.
    pub(crate) async fn replace<F>(
        &self,
        clock: &impl Clock,
        spacing: NonZeroDuration,
        fetch: impl FnOnce() -> F,
    ) -> Result<Arc<T>, OidcError>
    where
        F: Future<Output = Result<T, OidcError>>,
    {
        self.refresh(clock, Want::Replacement(spacing), fetch).await
    }

    async fn refresh<F>(
        &self,
        clock: &impl Clock,
        want: Want,
        fetch: impl FnOnce() -> F,
    ) -> Result<Arc<T>, OidcError>
    where
        F: Future<Output = Result<T, OidcError>>,
    {
        let asked = clock.wall();
        let fallback = match self.judge(asked, want, None) {
            Verdict::Serve(document) => return Ok(document),
            Verdict::Hold(stale) => return stale.ok_or_else(self.unavailable),
            Verdict::Fetch(stale) => stale,
        };
        let _turn = if let Ok(turn) = self.turn.try_lock() {
            turn
        } else {
            if let (Some(stale), Want::Fresh) = (&fallback, want) {
                return Ok(Arc::clone(stale));
            }
            let turn = self.turn.lock().await;
            match self.judge(clock.wall(), want, Some(asked)) {
                Verdict::Serve(document) => return Ok(document),
                Verdict::Hold(stale) => return stale.ok_or_else(self.unavailable),
                Verdict::Fetch(_) => turn,
            }
        };
        let fetched = fetch().await;
        let now = clock.wall();
        let mut slot = self.state.lock();
        match fetched {
            Ok(document) => {
                let document = Arc::new(document);
                slot.fetched = Some((Arc::clone(&document), now));
                slot.last_failure = None;
                slot.last_forced = match want {
                    Want::Fresh => None,
                    Want::Replacement(_) => Some(now),
                };
                Ok(document)
            }
            Err(error) => {
                slot.last_failure = Some(now);
                match want {
                    Want::Fresh => fallback.ok_or(error),
                    Want::Replacement(_) => Err(error),
                }
            }
        }
    }

    /// What the state allows at `now`. A document fetched at or after `fetched_since` (the moment
    /// the caller began waiting for a fetch in flight) is as good as one the caller would fetch.
    fn judge(&self, now: Timestamp, want: Want, fetched_since: Option<Timestamp>) -> Verdict<T> {
        let slot = self.state.lock();
        let held = slot.fetched.as_ref();
        let fallback = held.map(|(document, _)| Arc::clone(document));
        match (want, held) {
            (_, Some((document, at))) if fetched_since.is_some_and(|since| *at >= since) => {
                return Verdict::Serve(Arc::clone(document));
            }
            (Want::Fresh, Some((document, at))) if within(*at, now, self.ttl) => {
                return Verdict::Serve(Arc::clone(document));
            }
            (Want::Replacement(spacing), Some((document, _)))
                if slot
                    .last_forced
                    .is_some_and(|forced| within(forced, now, spacing)) =>
            {
                return Verdict::Serve(Arc::clone(document));
            }
            _ => {}
        }
        if slot
            .last_failure
            .is_some_and(|failed| within(failed, now, self.retry_interval))
        {
            return Verdict::Hold(match want {
                Want::Fresh => fallback,
                Want::Replacement(_) => None,
            });
        }
        Verdict::Fetch(fallback)
    }
}

fn within(since: Timestamp, now: Timestamp, span: NonZeroDuration) -> bool {
    now.saturating_duration_since(since) < span.get()
}

#[cfg(test)]
mod tests {
    use std::future::{Ready, ready};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gang_clock::{TestClock, Timestamp};
    use gang_testkit::park_then_signal;
    use gang_types::NonZeroDuration;
    use tokio::sync::{Notify, oneshot};
    use tokio::task::JoinHandle;

    use super::RefreshCache;
    use crate::errors::OidcError;

    type Fetched = Result<u32, OidcError>;
    type Got = Result<Arc<u32>, OidcError>;

    const TTL: NonZeroDuration = NonZeroDuration::from_mins(1);
    const RETRY: NonZeroDuration = NonZeroDuration::from_secs(10);
    const SPACING: NonZeroDuration = NonZeroDuration::from_secs(30);

    fn cache() -> RefreshCache<u32> {
        RefreshCache::<u32>::new(TTL, RETRY, || OidcError::DiscoveryUnavailable)
    }

    fn clock() -> Arc<TestClock> {
        Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_700_000_000_000,
        )))
    }

    fn counter() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    fn count(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }

    /// A fetch that counts itself and answers with 7.
    fn ok_fetch(calls: Arc<AtomicUsize>) -> impl FnOnce() -> Ready<Fetched> {
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(7))
        }
    }

    /// A fetch that counts itself and fails with a 500 from the issuer.
    fn failing_fetch(calls: Arc<AtomicUsize>) -> impl FnOnce() -> Ready<Fetched> {
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            ready(Err(OidcError::ServerError { status: 500 }))
        }
    }

    /// Signals `started`, then counts itself and answers 8 once `release` is notified.
    async fn held_fetch(
        calls: Arc<AtomicUsize>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    ) -> Fetched {
        started.notify_one();
        release.notified().await;
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(8)
    }

    /// Spawns a `get` whose fetch is held until the returned `Notify` is notified, and returns
    /// once that fetch has started.
    async fn spawn_held_get(
        cache: &Arc<RefreshCache<u32>>,
        clock: &Arc<TestClock>,
        calls: &Arc<AtomicUsize>,
    ) -> (JoinHandle<Got>, Arc<Notify>) {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let cache = Arc::clone(cache);
        let clock = Arc::clone(clock);
        let calls = Arc::clone(calls);
        let fetch_started = Arc::clone(&started);
        let fetch_release = Arc::clone(&release);
        let task = tokio::spawn(async move {
            cache
                .get(&*clock, || held_fetch(calls, fetch_started, fetch_release))
                .await
        });
        started.notified().await;
        (task, release)
    }

    #[tokio::test]
    async fn first_get_fetches_and_later_gets_within_the_ttl_reuse_the_value() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let first = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");
        assert_eq!(count(&calls), 1);

        clock.advance(Duration::from_secs(59));
        let second = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("cached value is served");
        assert_eq!(count(&calls), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn get_after_the_ttl_fetches_again() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let first = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");

        // The document is stale at exactly the time-to-live.
        clock.advance(TTL.get());
        let second = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("refetch succeeds");
        assert_eq!(count(&calls), 2);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn stale_value_is_served_when_the_refresh_fails() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let held = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");
        clock.advance(TTL.get());

        let served = cache
            .get(&*clock, failing_fetch(Arc::clone(&calls)))
            .await
            .expect("stale value is served");
        assert!(Arc::ptr_eq(&held, &served));
        assert_eq!(count(&calls), 2);

        // The failure is remembered: the stale value is served again without another fetch.
        let served = cache
            .get(&*clock, failing_fetch(Arc::clone(&calls)))
            .await
            .expect("stale value is served within the retry interval");
        assert!(Arc::ptr_eq(&held, &served));
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test]
    async fn failed_fetch_is_not_repeated_within_the_retry_interval() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let error = cache
            .get(&*clock, failing_fetch(Arc::clone(&calls)))
            .await
            .expect_err("fetch fails");
        assert!(matches!(error, OidcError::ServerError { status: 500 }));

        for _ in 0..3 {
            clock.advance(Duration::from_secs(3));
            let error = cache
                .get(&*clock, ok_fetch(Arc::clone(&calls)))
                .await
                .expect_err("the retry interval has not passed");
            assert!(matches!(error, OidcError::DiscoveryUnavailable));
        }
        assert_eq!(count(&calls), 1);

        clock.advance(Duration::from_secs(1));
        let error = cache
            .get(&*clock, failing_fetch(Arc::clone(&calls)))
            .await
            .expect_err("fetch fails again");
        assert!(matches!(error, OidcError::ServerError { status: 500 }));
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test]
    async fn failure_without_a_value_is_the_fetch_error_then_unavailable_within_the_retry_interval()
    {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let error = cache
            .get(&*clock, failing_fetch(Arc::clone(&calls)))
            .await
            .expect_err("fetch fails");
        assert!(matches!(error, OidcError::ServerError { status: 500 }));

        clock.advance(Duration::from_secs(5));
        let error = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect_err("the retry interval has not passed");
        assert!(matches!(error, OidcError::DiscoveryUnavailable));
        assert_eq!(count(&calls), 1);

        clock.advance(Duration::from_secs(5));
        let value = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("the retry interval has passed");
        assert_eq!(*value, 7);
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test]
    async fn stale_value_is_served_while_a_refresh_is_in_flight() {
        let cache = Arc::new(cache());
        let clock = clock();
        let calls = counter();

        let first = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");
        clock.advance(TTL.get());

        let (refresh, release) = spawn_held_get(&cache, &clock, &calls).await;
        let stale = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("stale value is served while refreshing");
        assert!(Arc::ptr_eq(&first, &stale));
        assert_eq!(count(&calls), 1);

        release.notify_one();
        let fresh = refresh
            .await
            .expect("refresh task joins")
            .expect("refresh succeeds");
        assert_eq!(*fresh, 8);
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test]
    async fn concurrent_gets_without_a_value_share_one_fetch() {
        let cache = Arc::new(cache());
        let clock = clock();
        let calls = counter();

        let (first, release) = spawn_held_get(&cache, &clock, &calls).await;
        let (parked_tx, parked_rx) = oneshot::channel();
        let second = {
            let cache = Arc::clone(&cache);
            let clock = Arc::clone(&clock);
            let calls = Arc::clone(&calls);
            tokio::spawn(park_then_signal(
                async move { cache.get(&*clock, ok_fetch(calls)).await },
                parked_tx,
            ))
        };
        parked_rx.await.expect("second caller parks");
        assert!(
            !second.is_finished(),
            "the second caller waits for the fetch in flight"
        );

        release.notify_one();
        let first = first
            .await
            .expect("first task joins")
            .expect("fetch succeeds");
        let second = second
            .await
            .expect("second task joins")
            .expect("second caller shares the result");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(count(&calls), 1);
    }

    #[tokio::test]
    async fn dropping_the_fetching_future_lets_the_next_caller_fetch() {
        let cache = Arc::new(cache());
        let clock = clock();
        let calls = counter();

        let (fetching, _release) = spawn_held_get(&cache, &clock, &calls).await;
        fetching.abort();
        let error = fetching.await.expect_err("the aborted task has no result");
        assert!(error.is_cancelled());
        assert_eq!(count(&calls), 0);

        let value = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("next caller fetches");
        assert_eq!(*value, 7);
        assert_eq!(count(&calls), 1);
    }

    #[tokio::test]
    async fn replace_refetches_once_per_spacing() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let regular = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");

        let first = cache
            .replace(&*clock, SPACING, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("replacement succeeds");
        assert_eq!(count(&calls), 2);
        assert!(!Arc::ptr_eq(&regular, &first));

        let again = cache
            .replace(&*clock, SPACING, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("held value is served within the spacing");
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(count(&calls), 2);

        clock.advance(SPACING.get());
        let third = cache
            .replace(&*clock, SPACING, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("replacement succeeds after the spacing");
        assert!(!Arc::ptr_eq(&first, &third));
        assert_eq!(count(&calls), 3);
    }

    #[tokio::test]
    async fn first_replace_after_a_regular_fetch_is_always_allowed() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let regular = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");
        let replaced = cache
            .replace(&*clock, SPACING, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("first replacement is allowed");
        assert!(!Arc::ptr_eq(&regular, &replaced));
        assert_eq!(count(&calls), 2);

        // A regular refetch after the time-to-live resets the replacement allowance: the
        // replacement right after it fetches although the previous replacement is within the
        // longer spacing.
        clock.advance(TTL.get());
        let refetched = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("refetch succeeds");
        assert_eq!(count(&calls), 3);

        let replaced_again = cache
            .replace(
                &*clock,
                NonZeroDuration::from_mins(2),
                ok_fetch(Arc::clone(&calls)),
            )
            .await
            .expect("replacement is allowed after the regular refetch");
        assert!(!Arc::ptr_eq(&refetched, &replaced_again));
        assert_eq!(count(&calls), 4);
    }

    #[tokio::test]
    async fn replace_failure_is_returned_and_the_held_value_keeps_being_served() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        let held = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");

        let error = cache
            .replace(&*clock, SPACING, failing_fetch(Arc::clone(&calls)))
            .await
            .expect_err("replacement fails");
        assert!(matches!(error, OidcError::ServerError { status: 500 }));
        assert_eq!(count(&calls), 2);

        let served = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("held value is still fresh");
        assert!(Arc::ptr_eq(&held, &served));
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test]
    async fn replacement_refused_by_the_backoff_is_unavailable_not_stale() {
        let cache = cache();
        let clock = clock();
        let calls = counter();

        cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("initial fetch succeeds");
        let error = cache
            .replace(&*clock, SPACING, failing_fetch(Arc::clone(&calls)))
            .await
            .expect_err("replacement fails");
        assert!(matches!(error, OidcError::ServerError { status: 500 }));

        clock.advance(Duration::from_secs(1));
        let error = cache
            .replace(&*clock, SPACING, ok_fetch(Arc::clone(&calls)))
            .await
            .expect_err("replacement is refused during backoff");
        assert!(matches!(error, OidcError::DiscoveryUnavailable));
        assert_eq!(count(&calls), 2);
    }

    #[tokio::test]
    async fn replace_waits_for_a_fetch_in_flight_and_shares_its_result() {
        let cache = Arc::new(cache());
        let clock = clock();
        let calls = counter();

        let held = cache
            .get(&*clock, ok_fetch(Arc::clone(&calls)))
            .await
            .expect("fetch succeeds");
        clock.advance(TTL.get());

        let (refresh, release) = spawn_held_get(&cache, &clock, &calls).await;
        let (parked_tx, parked_rx) = oneshot::channel();
        let waiting_replace = {
            let cache = Arc::clone(&cache);
            let clock = Arc::clone(&clock);
            let calls = Arc::clone(&calls);
            tokio::spawn(park_then_signal(
                async move { cache.replace(&*clock, SPACING, ok_fetch(calls)).await },
                parked_tx,
            ))
        };
        parked_rx.await.expect("replacement parks");
        assert!(
            !waiting_replace.is_finished(),
            "the replacement waits for the fetch in flight"
        );

        release.notify_one();
        let refreshed = refresh
            .await
            .expect("refresh task joins")
            .expect("refresh succeeds");
        let replaced = waiting_replace
            .await
            .expect("replacer task joins")
            .expect("replacement shares the result");
        assert!(Arc::ptr_eq(&refreshed, &replaced));
        assert!(!Arc::ptr_eq(&held, &replaced));
        // The initial get and the held refresh are the only fetches.
        assert_eq!(count(&calls), 2);
    }
}
