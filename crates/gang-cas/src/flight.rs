//! Request coalescing (`SingleFlight`) for deduplicating concurrent fetches and builds.
//!
//! Concurrent callers sharing a key execute the work function once and broadcast the cloned
//! outcome over a `tokio::sync::watch` channel. Completion removes the map entry before
//! publishing so future attempts are never poisoned by transient errors.
//!
//! If a leader future in [`SingleFlight::run`] is dropped before completion, its drop guard
//! clears the entry and notifies waiting followers so one promotes cleanly and re-drives the work
//! without deadlock. [`SingleFlight::run_detached`] spawns the work on `Tokio` so cancelling a
//! single caller does not abort other followers.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher as _, Hash, RandomState};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::{Mutex, Notify, watch};

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

#[derive(Debug, Default)]
struct DetachedTasks {
    active: AtomicUsize,
    drained: Notify,
}

/// Cache-owned cancellation shared by every detached flight.
#[derive(Debug, Clone, Default)]
pub struct FlightCancel(Arc<FlightShutdown>);

#[derive(Debug, Default)]
struct FlightShutdown {
    cancelled: AtomicBool,
    notify: Notify,
}

impl FlightCancel {
    /// Returns whether cache shutdown has been signalled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    /// Waits for cache shutdown, including cancellation before this future was polled.
    pub async fn cancelled(&self) {
        let notified = self.0.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_cancelled() {
            notified.await;
        }
    }
}

struct DetachedGuard(Arc<DetachedTasks>);

impl DetachedGuard {
    fn new(tasks: &Arc<DetachedTasks>) -> Self {
        tasks.active.fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(tasks))
    }
}

impl Drop for DetachedGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

/// Deduplicates concurrent asynchronous operations keyed by `K`.
pub struct SingleFlight<K, T> {
    shards: Vec<Arc<Shard<K, T>>>,
    hash_builder: RandomState,
    detached: Arc<DetachedTasks>,
    cancel: FlightCancel,
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
            detached: Arc::new(DetachedTasks::default()),
            cancel: FlightCancel::default(),
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
    /// aborted or panics, waiting followers promote cleanly and re-drive. [`Self::drain`] waits
    /// for the spawned work even after all callers have been cancelled.
    pub async fn run_detached<F, Fut>(&self, key: K, work: F) -> T
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.run_detached_cancellable(key, move |_| work()).await
    }

    /// Runs coalesced work with a cache shutdown token; cancelling a caller leaves it running.
    pub async fn run_detached_cancellable<F, Fut>(&self, key: K, work: F) -> T
    where
        F: FnOnce(FlightCancel) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let shard = self.shard_for(&key);
        loop {
            let role = acquire_role(shard, &key).await;
            match role {
                Role::Leader(flight) => {
                    let shard_clone = Arc::clone(shard);
                    let key_clone = key.clone();
                    let fut = work(self.cancel.clone());
                    let detached = DetachedGuard::new(&self.detached);
                    let handle = tokio::spawn(async move {
                        let _detached = detached;
                        drive_leader(shard_clone, key_clone, flight, fut).await
                    });
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

    /// Signals cache shutdown without cancelling individual callers' shared flights.
    pub fn cancel(&self) {
        self.cancel.0.cancelled.store(true, Ordering::Release);
        self.cancel.0.notify.notify_waiters();
    }

    /// Cancels cache-owned work and waits for every detached flight to finish.
    pub async fn shutdown(&self) {
        self.cancel();
        self.drain().await;
    }

    /// Waits for all detached work to finish, including work whose callers were cancelled.
    ///
    /// Stop admitting callers before draining; this does not prevent new flights from starting.
    pub async fn drain(&self) {
        loop {
            let notified = self.detached.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.detached.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::SingleFlight;

    #[tokio::test]
    async fn coalesces_concurrent_callers_for_same_key() {
        let flight = SingleFlight::new();
        let calls = AtomicUsize::new(0);
        let release = Notify::new();
        let mut leader = Box::pin(flight.run("blob-a", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            release.notified().await;
            Ok::<_, &'static str>(42)
        }));
        assert!(futures::poll!(leader.as_mut()).is_pending());

        let mut followers = Vec::new();
        for _ in 0..8 {
            let mut follower = Box::pin(flight.run("blob-a", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(99)
            }));
            assert!(futures::poll!(follower.as_mut()).is_pending());
            followers.push(follower);
        }

        release.notify_one();
        assert_eq!(leader.await, Ok(42));
        for follower in followers {
            assert_eq!(follower.await, Ok(42));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn leader_cancellation_promotes_waiting_follower_without_deadlock() {
        let flight = SingleFlight::new();
        let release = Notify::new();
        let calls = AtomicUsize::new(0);

        let mut leader = Box::pin(flight.run("key", || {
            std::future::pending::<Result<usize, &'static str>>()
        }));
        assert!(futures::poll!(leader.as_mut()).is_pending());

        let mut followers = Vec::new();
        for _ in 0..2 {
            let mut follower = Box::pin(flight.run("key", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok(77)
            }));
            assert!(futures::poll!(follower.as_mut()).is_pending());
            followers.push(follower);
        }

        drop(leader);
        for follower in &mut followers {
            assert!(futures::poll!(follower.as_mut()).is_pending());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_one();

        for follower in followers {
            assert_eq!(follower.await, Ok(77));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn errors_propagate_to_current_waiters_without_poisoning_future_attempts() {
        let flight = SingleFlight::new();
        let release = Notify::new();
        let mut leader = Box::pin(flight.run("key", || async {
            release.notified().await;
            Err::<usize, _>("transient failure")
        }));
        assert!(futures::poll!(leader.as_mut()).is_pending());

        let mut follower = Box::pin(flight.run("key", || async { Ok(123) }));
        assert!(futures::poll!(follower.as_mut()).is_pending());
        release.notify_one();

        assert_eq!(leader.await, Err("transient failure"));
        assert_eq!(follower.await, Err("transient failure"));
        assert_eq!(flight.run("key", || async { Ok(456) }).await, Ok(456));
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn detached_leader_survives_caller_cancellation() {
        tokio::time::timeout(Duration::from_secs(5), detached_cancellation_drains())
            .await
            .expect("detached flight did not drain");
    }

    async fn detached_cancellation_drains() {
        let flight = SingleFlight::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let mut leader = Box::pin(flight.run_detached("detached", {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move || async move {
                started.notify_one();
                release.notified().await;
                999
            }
        }));
        assert!(futures::poll!(leader.as_mut()).is_pending());
        let mut drain = Box::pin(flight.drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());
        started.notified().await;

        let mut follower = Box::pin(flight.run_detached("detached", || async { 111 }));
        assert!(futures::poll!(follower.as_mut()).is_pending());
        drop(leader);
        assert!(futures::poll!(drain.as_mut()).is_pending());

        release.notify_one();
        assert_eq!(follower.await, 999);
        drain.await;
        assert_eq!(flight.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn shutdown_releases_all_cancellable_detached_flights() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let flight = SingleFlight::new();
            let started = Arc::new(Notify::new());
            let mut caller = Box::pin(flight.run_detached_cancellable("cancel", {
                let started = Arc::clone(&started);
                move |cancel| async move {
                    started.notify_one();
                    tokio::join!(cancel.cancelled(), cancel.cancelled());
                    cancel.is_cancelled()
                }
            }));
            assert!(futures::poll!(caller.as_mut()).is_pending());
            started.notified().await;
            drop(caller);
            flight.shutdown().await;
            assert_eq!(flight.in_flight_count().await, 0);
            assert!(
                flight
                    .run_detached_cancellable("after", |cancel| async move {
                        cancel.cancelled().await;
                        cancel.is_cancelled()
                    })
                    .await
            );
            flight.shutdown().await;
        })
        .await
        .expect("cache shutdown lost cancellation or completion notification");
    }

    #[tokio::test]
    async fn drain_returns_immediately_when_idle() {
        let flight = SingleFlight::<&str, usize>::new();
        let mut drain = Box::pin(flight.drain());
        assert!(futures::poll!(drain.as_mut()).is_ready());
    }

    #[tokio::test]
    async fn panicking_detached_task_releases_all_drainers() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let flight = SingleFlight::<&str, usize>::new();
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let mut caller = Box::pin(flight.run_detached("panic", {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                move || async move {
                    started.notify_one();
                    release.notified().await;
                    panic!("detached flight panic");
                }
            }));
            assert!(futures::poll!(caller.as_mut()).is_pending());
            started.notified().await;
            drop(caller);

            let mut first = Box::pin(flight.drain());
            let mut second = Box::pin(flight.drain());
            assert!(futures::poll!(first.as_mut()).is_pending());
            assert!(futures::poll!(second.as_mut()).is_pending());
            release.notify_one();
            first.await;
            second.await;
            assert_eq!(flight.in_flight_count().await, 0);
            assert_eq!(flight.run_detached("panic", || async { 123 }).await, 123);
            flight.drain().await;
        })
        .await
        .expect("panicking detached flight did not drain");
    }
}
