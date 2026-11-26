//! Request coalescing (`SingleFlight`) for deduplicating concurrent fetches and builds.
//!
//! Concurrent callers sharing a key execute the work function once and broadcast the cloned
//! outcome over a `tokio::sync::watch` channel. Completion removes the map entry before
//! publishing so future attempts are never poisoned by transient errors.
//!
//! If a leader future in [`SingleFlight::run`] is dropped before completion, its drop guard
//! clears the entry and notifies waiting followers so one promotes cleanly and re-drives the work
//! without deadlock. [`SingleFlight::run_detached`] spawns the work on Tokio so cancelling a
//! single caller does not abort other followers.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher as _, Hash, RandomState};
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{Mutex, watch};

const DEFAULT_SHARDS: usize = 64;

#[derive(Clone)]
enum FlightState<T> {
    Running,
    Ready(T),
    Cancelled,
}

struct Flight<T> {
    tx: watch::Sender<FlightState<T>>,
}

impl<T> Flight<T> {
    fn new() -> Self {
        let (tx, _rx) = watch::channel(FlightState::Running);
        Self { tx }
    }
}

type ShardMap<K, T> = HashMap<K, Arc<Flight<T>>>;
type Shard<K, T> = Mutex<ShardMap<K, T>>;

/// Deduplicates concurrent asynchronous operations keyed by `K`.
pub struct SingleFlight<K, T> {
    shards: Vec<Arc<Shard<K, T>>>,
    hash_builder: RandomState,
}

impl<K, T> fmt::Debug for SingleFlight<K, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SingleFlight")
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

impl<K, T> Default for SingleFlight<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, T> SingleFlight<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Creates a [`SingleFlight`] table with the default shard count.
    #[must_use]
    pub fn new() -> Self {
        let count = NonZeroUsize::new(DEFAULT_SHARDS).unwrap_or(NonZeroUsize::MIN);
        Self::with_shards(count)
    }

    /// Creates a [`SingleFlight`] table with `shards` independent lock shards.
    #[must_use]
    pub fn with_shards(shards: NonZeroUsize) -> Self {
        let count = shards.get();
        let mut buckets = Vec::with_capacity(count);
        for _ in 0..count {
            buckets.push(Arc::new(Mutex::new(HashMap::new())));
        }
        Self {
            shards: buckets,
            hash_builder: RandomState::new(),
        }
    }

    fn shard_for(&self, key: &K) -> &Arc<Shard<K, T>> {
        let hash = self.hash_builder.hash_one(key);
        let len = u64::try_from(self.shards.len()).unwrap_or(1);
        let index = usize::try_from(hash % len).unwrap_or(0);
        &self.shards[index]
    }

    /// Runs `work` for `key`, coalescing concurrent callers for the same key.
    ///
    /// Cancellation-safe: if the leader future is dropped before completing, a waiting follower
    /// wakes, promotes cleanly to leader, and re-drives its own `work` closure without deadlock.
    pub async fn run<F, Fut>(&self, key: K, work: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let shard = self.shard_for(&key);
        loop {
            let role = acquire_role(shard, &key).await;
            match role {
                Role::Leader(flight) => {
                    return drive_leader(Arc::clone(shard), key, flight, work()).await;
                }
                Role::Follower(rx) => {
                    if let Some(value) = wait_for_flight(rx).await {
                        return value;
                    }
                }
            }
        }
    }

    /// Runs `work` for `key` on a spawned Tokio task, coalescing concurrent callers.
    ///
    /// Because the first caller spawns the work onto Tokio, dropping a caller's future does not
    /// abort the in-flight work for other waiting followers. If the spawned task itself is
    /// aborted or panics, waiting followers promote cleanly and re-drive.
    pub async fn run_detached<F, Fut>(&self, key: K, work: F) -> T
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let shard = self.shard_for(&key);
        loop {
            let role = acquire_role(shard, &key).await;
            match role {
                Role::Leader(flight) => {
                    let shard_clone = Arc::clone(shard);
                    let key_clone = key.clone();
                    let fut = work();
                    let handle = tokio::spawn(drive_leader(shard_clone, key_clone, flight, fut));
                    return match handle.await {
                        Ok(value) => value,
                        Err(err) if err.is_panic() => {
                            std::panic::resume_unwind(err.into_panic());
                        }
                        Err(_) => {
                            // Owned `JoinHandle` is only cancelled on Tokio runtime shutdown.
                            std::future::pending().await
                        }
                    };
                }
                Role::Follower(rx) => {
                    if let Some(value) = wait_for_flight(rx).await {
                        return value;
                    }
                }
            }
        }
    }

    /// Returns the number of keys currently recorded as in flight across all shards.
    pub async fn in_flight_count(&self) -> usize {
        let mut total = 0_usize;
        for shard in &self.shards {
            let map = shard.lock().await;
            total = total.saturating_add(map.len());
        }
        total
    }
}

enum Role<T> {
    Leader(Arc<Flight<T>>),
    Follower(watch::Receiver<FlightState<T>>),
}

async fn acquire_role<K, T>(shard: &Shard<K, T>, key: &K) -> Role<T>
where
    K: Eq + Hash + Clone,
{
    let mut map = shard.lock().await;
    if let Some(existing) = map.get(key)
        && !matches!(*existing.tx.borrow(), FlightState::Cancelled)
    {
        return Role::Follower(existing.tx.subscribe());
    }
    let flight = Arc::new(Flight::new());
    map.insert(key.clone(), Arc::clone(&flight));
    Role::Leader(flight)
}

async fn wait_for_flight<T>(mut rx: watch::Receiver<FlightState<T>>) -> Option<T>
where
    T: Clone,
{
    loop {
        {
            let state = rx.borrow_and_update();
            match &*state {
                FlightState::Running => {}
                FlightState::Ready(value) => return Some(value.clone()),
                FlightState::Cancelled => return None,
            }
        }
        if rx.changed().await.is_err() {
            return None;
        }
    }
}

async fn drive_leader<K, T, Fut>(
    shard: Arc<Shard<K, T>>,
    key: K,
    flight: Arc<Flight<T>>,
    fut: Fut,
) -> T
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    Fut: Future<Output = T>,
{
    let mut guard = LeaderGuard {
        shard: Arc::clone(&shard),
        key: key.clone(),
        flight: Arc::clone(&flight),
        armed: true,
    };

    let result = fut.await;

    {
        let mut map = shard.lock().await;
        remove_if_same(&mut map, &key, &flight);
    }
    let _ = flight.tx.send_replace(FlightState::Ready(result.clone()));
    guard.armed = false;
    result
}

struct LeaderGuard<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    shard: Arc<Shard<K, T>>,
    key: K,
    flight: Arc<Flight<T>>,
    armed: bool,
}

impl<K, T> Drop for LeaderGuard<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut map) = self.shard.try_lock() {
            remove_if_same(&mut map, &self.key, &self.flight);
            drop(map);
            let _ = self.flight.tx.send_replace(FlightState::Cancelled);
        } else {
            let _ = self.flight.tx.send_replace(FlightState::Cancelled);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let shard = Arc::clone(&self.shard);
                let key = self.key.clone();
                let flight = Arc::clone(&self.flight);
                handle.spawn(async move {
                    let mut map = shard.lock().await;
                    remove_if_same(&mut map, &key, &flight);
                });
            }
        }
    }
}

fn remove_if_same<K, T>(map: &mut ShardMap<K, T>, key: &K, flight: &Arc<Flight<T>>)
where
    K: Eq + Hash,
{
    if let Some(current) = map.get(key)
        && Arc::ptr_eq(current, flight)
    {
        map.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::SingleFlight;

    #[tokio::test]
    async fn coalesces_concurrent_callers_for_same_key() {
        let flight = Arc::new(SingleFlight::<&'static str, Result<usize, String>>::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let leader = {
            let flight = Arc::clone(&flight);
            let calls = Arc::clone(&calls);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                flight
                    .run("blob-a", || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        entered.notify_one();
                        release.notified().await;
                        Ok(42)
                    })
                    .await
            })
        };

        entered.notified().await;

        let mut followers = Vec::new();
        for _ in 0..8 {
            let flight = Arc::clone(&flight);
            let calls = Arc::clone(&calls);
            followers.push(tokio::spawn(async move {
                flight
                    .run("blob-a", || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(99)
                    })
                    .await
            }));
        }

        tokio::task::yield_now().await;
        release.notify_waiters();

        assert_eq!(leader.await.expect("leader join"), Ok(42));
        for follower in followers {
            assert_eq!(follower.await.expect("follower join"), Ok(42));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn leader_cancellation_promotes_waiting_follower_without_deadlock() {
        let flight = Arc::new(SingleFlight::<&'static str, Result<usize, String>>::new());
        let leader_started = Arc::new(Notify::new());
        let follower_entered = Arc::new(Notify::new());
        let follower_release = Arc::new(Notify::new());
        let follower_calls = Arc::new(AtomicUsize::new(0));

        let mut leader = Box::pin(flight.run("key", || {
            let leader_started = Arc::clone(&leader_started);
            async move {
                leader_started.notify_one();
                std::future::pending::<Result<usize, String>>().await
            }
        }));

        // Poll leader once so it enters pending() and registers the in-flight slot.
        let noop_waker = std::task::Waker::noop();
        let mut noop_cx = std::task::Context::from_waker(noop_waker);
        assert!(leader.as_mut().poll(&mut noop_cx).is_pending());

        let follower1 = tokio::spawn({
            let flight = Arc::clone(&flight);
            let follower_calls = Arc::clone(&follower_calls);
            let follower_entered = Arc::clone(&follower_entered);
            let follower_release = Arc::clone(&follower_release);
            async move {
                flight
                    .run("key", || async move {
                        follower_calls.fetch_add(1, Ordering::SeqCst);
                        follower_entered.notify_one();
                        follower_release.notified().await;
                        Ok(77)
                    })
                    .await
            }
        });
        let follower2 = tokio::spawn({
            let flight = Arc::clone(&flight);
            let follower_calls = Arc::clone(&follower_calls);
            let follower_entered = Arc::clone(&follower_entered);
            let follower_release = Arc::clone(&follower_release);
            async move {
                flight
                    .run("key", || async move {
                        follower_calls.fetch_add(1, Ordering::SeqCst);
                        follower_entered.notify_one();
                        follower_release.notified().await;
                        Ok(77)
                    })
                    .await
            }
        });

        // Let both spawned followers register on the leader's watch channel.
        tokio::task::yield_now().await;

        // Cancel the leader future; one follower must promote and the other must follow it.
        drop(leader);

        tokio::time::timeout(Duration::from_secs(2), follower_entered.notified())
            .await
            .expect("promoted follower must start work without deadlock");
        tokio::task::yield_now().await;
        follower_release.notify_waiters();

        let (res1, res2) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(follower1, follower2)
        })
        .await
        .expect("followers must complete after leader drop");

        assert_eq!(res1.expect("follower1 join"), Ok(77));
        assert_eq!(res2.expect("follower2 join"), Ok(77));
        assert_eq!(follower_calls.load(Ordering::SeqCst), 1);
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn errors_propagate_to_current_waiters_without_poisoning_future_attempts() {
        let flight = Arc::new(SingleFlight::<&'static str, Result<usize, &'static str>>::new());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let t1 = {
            let flight = Arc::clone(&flight);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                flight
                    .run("k", || async move {
                        entered.notify_one();
                        release.notified().await;
                        Err("transient failure")
                    })
                    .await
            })
        };

        entered.notified().await;
        let t2 = {
            let flight = Arc::clone(&flight);
            tokio::spawn(async move { flight.run("k", || async { Ok(123) }).await })
        };

        tokio::task::yield_now().await;
        release.notify_waiters();

        assert_eq!(t1.await.expect("t1 join"), Err("transient failure"));
        assert_eq!(t2.await.expect("t2 join"), Err("transient failure"));

        // A subsequent attempt after the error must not be poisoned.
        let t3 = flight.run("k", || async { Ok(456) }).await;
        assert_eq!(t3, Ok(456));
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn detached_leader_survives_caller_cancellation() {
        let flight = Arc::new(SingleFlight::<&'static str, usize>::new());
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let leader = tokio::spawn({
            let flight = Arc::clone(&flight);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                flight
                    .run_detached("detached", move || async move {
                        started.notify_one();
                        release.notified().await;
                        999
                    })
                    .await
            }
        });

        started.notified().await;

        let follower = tokio::spawn({
            let flight = Arc::clone(&flight);
            async move { flight.run_detached("detached", || async { 111 }).await }
        });
        tokio::task::yield_now().await;

        // Abort the leader's awaiting task; the detached work continues for the follower.
        leader.abort();
        let _ = leader.await;

        release.notify_waiters();
        assert_eq!(follower.await.expect("follower join"), 999);
        assert_eq!(flight.in_flight_count().await, 0);
    }

    use std::sync::Arc;
}
