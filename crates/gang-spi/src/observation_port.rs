//! Bounded, non-blocking build observation channel.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::{BuildKey, InvocationId, Mnemonic, TargetLabel, ToolDetails, WorkerId};
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};

/// Which remote-execution call was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    /// A lookup in the action cache.
    GetActionResult,
    /// An upload to the action cache.
    UpdateActionResult,
    /// An execution request.
    Execute,
}

/// How the call was answered from the response head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallOutcome {
    /// HTTP 200 with `grpc-status` absent or zero.
    Ok,
    /// The status was `NOT_FOUND`.
    NotFound,
    /// Another status was returned.
    Error,
}

/// The build an observation belongs to and its Bazel action metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    /// The build that made the request.
    pub build: BuildKey,
    /// Bazel's `correlated_invocations_id` (`--build_request_id`).
    pub correlated: Option<InvocationId>,
    /// Tool name and version.
    pub tool: Option<ToolDetails>,
    /// The action's target label.
    pub target: Option<TargetLabel>,
    /// The action's mnemonic.
    pub mnemonic: Option<Mnemonic>,
}

/// A client call to the action cache or the execution service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The request's origin.
    pub origin: Origin,
    /// The call observed.
    pub rpc: Rpc,
    /// The response outcome.
    pub outcome: CallOutcome,
}

/// One phase of an execution, as the worker reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase {
    /// When the phase started.
    pub start: Timestamp,
    /// When the phase ended.
    pub end: Timestamp,
}

impl Phase {
    /// Returns a phase when both endpoints were reported, clamping clock skew to zero duration.
    #[must_use]
    pub fn reported(start: Timestamp, end: Timestamp) -> Option<Self> {
        (start != Timestamp::UNIX_EPOCH && end != Timestamp::UNIX_EPOCH).then_some(Self {
            start,
            end: end.max(start),
        })
    }

    /// The duration between the phase's endpoints.
    #[must_use]
    pub fn duration(&self) -> Duration {
        self.end.saturating_duration_since(self.start)
    }
}

/// Execution phases, when the worker reported both endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTimes {
    /// From queuing until the worker started.
    pub queued: Option<Phase>,
    /// Input fetching.
    pub input_fetch: Option<Phase>,
    /// Action execution.
    pub executing: Option<Phase>,
    /// Output uploading.
    pub output_upload: Option<Phase>,
    /// From queuing until the worker completed the action.
    pub whole: Option<Phase>,
}

/// An action a worker completed, attributed to the operation's first submitter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed {
    /// The first submitter's origin.
    pub origin: Origin,
    /// The action digest, `<hash>/<size>`.
    pub digest: String,
    /// The worker, when its id names a Gang worker.
    pub worker: Option<WorkerId>,
    /// The action's exit code.
    pub exit_code: i32,
    /// Whether the result carries an execution error.
    pub error: bool,
    /// The worker's phase timestamps.
    pub times: ExecutionTimes,
}

/// One thing a build did that the controller saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// A client call.
    Call(Call),
    /// A completed worker action.
    Executed(Executed),
}

impl Observation {
    /// The originating build and action metadata.
    #[must_use]
    pub fn origin(&self) -> &Origin {
        match self {
            Self::Call(call) => &call.origin,
            Self::Executed(executed) => &executed.origin,
        }
    }
}
/// An observation or a notification that losses are ready to drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery(Option<Observation>);

impl Delivery {
    /// Returns the owned observation, or `None` if losses are ready to drain with `take_lost`.
    #[must_use]
    pub fn into_observation(self) -> Option<Observation> {
        self.0
    }
}

#[derive(Debug)]
struct Lost {
    each: Mutex<HashMap<BuildKey, u64>>,
    capacity: usize,
    unattributed: AtomicU64,
    dirty: AtomicBool,
    notify: Notify,
}
impl Lost {
    fn add(&self, key: &BuildKey, count: u64) {
        let mut each = self.each.lock();
        if let Some(existing) = each.get_mut(key) {
            *existing = existing.saturating_add(count);
        } else if each.len() < self.capacity {
            each.insert(key.clone(), count);
        } else {
            self.add_unattributed_locked(count);
        }
        self.dirty.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    fn add_unattributed(&self, count: u64) {
        let _each = self.each.lock();
        self.add_unattributed_locked(count);
        self.dirty.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    // The caller holds `each`, so a drain cannot clear dirty between the count and notification.
    fn add_unattributed_locked(&self, count: u64) {
        let _ = self
            .unattributed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(count))
            });
    }
}

/// The producing side of the bounded observation channel. Recording never waits for capacity.
#[derive(Debug, Clone)]
pub struct ObservationSink {
    sender: mpsc::Sender<Observation>,
    lost: Arc<Lost>,
}

/// The consuming side, owned by the server's recorder.
#[derive(Debug)]
pub struct Observations {
    receiver: mpsc::Receiver<Observation>,
    lost: Arc<Lost>,
}

impl ObservationSink {
    /// Creates a bounded queue and bounds the number of builds tracked between loss drains.
    #[must_use]
    pub fn channel(queue: NonZeroUsize, lost_builds: NonZeroUsize) -> (Self, Observations) {
        let (sender, receiver) = mpsc::channel(queue.get());
        let lost = Arc::new(Lost {
            each: Mutex::new(HashMap::new()),
            capacity: lost_builds.get(),
            unattributed: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            notify: Notify::new(),
        });
        (
            Self {
                sender,
                lost: Arc::clone(&lost),
            },
            Observations { receiver, lost },
        )
    }

    /// Sends without waiting, or counts the observation lost if the channel is full or closed.
    pub fn record(&self, observation: Observation) {
        if let Err(error) = self.sender.try_send(observation) {
            self.lost.add(&error.into_inner().origin().build, 1);
        }
    }

    /// Counts lost observations for a build, or as unattributed when the build limit is reached.
    /// Counts saturate at `u64::MAX` and are recorded even if the receiver is closed.
    pub fn lose(&self, key: &BuildKey, count: NonZeroU64) {
        self.lost.add(key, count.get());
    }

    /// Counts unattributed lost observations, saturating at `u64::MAX`.
    pub fn lose_unattributed(&self, count: NonZeroU64) {
        self.lost.add_unattributed(count.get());
    }
}

impl Observations {
    /// The next observation or loss notification; `None` once sinks and pending losses are gone.
    pub async fn next(&mut self) -> Option<Delivery> {
        tokio::select! {
            biased;
            observation = self.receiver.recv() => {
                match observation {
                    Some(observation) => Some(Delivery(Some(observation))),
                    None if self.lost.dirty.load(Ordering::Acquire) => Some(Delivery(None)),
                    None => None,
                }
            }
            () = async {
                loop {
                    if self.lost.dirty.load(Ordering::Acquire) {
                        break;
                    }
                    self.lost.notify.notified().await;
                }
            } => Some(Delivery(None)),
        }
    }

    /// Takes per-build losses through `each`, returning the unattributed loss count.
    pub fn take_lost(&self, mut each: impl FnMut(BuildKey, u64)) -> u64 {
        if !self.lost.dirty.load(Ordering::Acquire) {
            return 0;
        }
        let (counts, unattributed) = {
            let mut counts = self.lost.each.lock();
            self.lost.dirty.store(false, Ordering::Release);
            (
                std::mem::take(&mut *counts),
                self.lost.unattributed.swap(0, Ordering::Relaxed),
            )
        };
        for (key, count) in counts {
            each(key, count);
        }
        unattributed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::{Identity, InvocationId};

    fn call(invocation: &str) -> Observation {
        Observation::Call(Call {
            origin: Origin {
                build: BuildKey {
                    principal: Arc::new(
                        "example:alice".parse::<Identity>().expect("valid identity"),
                    ),
                    invocation: InvocationId::try_new(invocation).expect("valid invocation"),
                },
                correlated: None,
                tool: None,
                target: None,
                mnemonic: None,
            },
            rpc: Rpc::GetActionResult,
            outcome: CallOutcome::Ok,
        })
    }
    #[test]
    fn reported_phase_requires_endpoints_and_clamps_clock_skew() {
        let epoch = Timestamp::UNIX_EPOCH;
        let start = Timestamp::from_unix_millis(1_000);
        let end = Timestamp::from_unix_millis(1_750);
        assert_eq!(Phase::reported(epoch, end), None);
        assert_eq!(Phase::reported(start, epoch), None);
        let skewed = Phase::reported(end, start).expect("both endpoints were reported");
        assert_eq!(skewed.start, end);
        assert_eq!(skewed.end, end);
        assert_eq!(skewed.duration(), Duration::ZERO);
        let phase = Phase::reported(start, end).expect("ordered phase");
        assert_eq!(phase.duration(), Duration::from_millis(750));
        assert_eq!(
            Phase::reported(start, start).map(|phase| phase.duration()),
            Some(Duration::ZERO)
        );
    }
    #[tokio::test]
    async fn recording_a_loss_notifies_a_waiting_consumer() {
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let observation = call("a");
        {
            let next = observations.next();
            tokio::pin!(next);
            assert!(futures::poll!(&mut next).is_pending());
            sink.lose(
                &observation.origin().build,
                NonZeroU64::new(3).expect("nonzero"),
            );
            assert_eq!(
                futures::poll!(&mut next),
                std::task::Poll::Ready(Some(Delivery(None)))
            );
        }
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            0
        );
        assert_eq!(counts.get(&observation.origin().build), Some(&3));
    }

    #[tokio::test]
    async fn counted_unattributed_loss_notifies_a_waiting_consumer() {
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        {
            let next = observations.next();
            tokio::pin!(next);
            assert!(futures::poll!(&mut next).is_pending());
            sink.lose_unattributed(NonZeroU64::new(7).expect("nonzero"));
            assert_eq!(
                futures::poll!(&mut next),
                std::task::Poll::Ready(Some(Delivery(None)))
            );
        }
        assert_eq!(observations.take_lost(|_, _| panic!("unexpected key")), 7);
        assert_eq!(observations.take_lost(|_, _| panic!("loss repeated")), 0);
    }

    #[test]
    fn counted_losses_add_overflow_and_drain_once() {
        let (sink, observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let a = call("a");
        let b = call("b");
        sink.lose(&a.origin().build, NonZeroU64::new(3).expect("nonzero"));
        sink.lose(&a.origin().build, NonZeroU64::new(4).expect("nonzero"));
        sink.lose(&b.origin().build, NonZeroU64::new(5).expect("nonzero"));
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            5
        );
        assert_eq!(counts.len(), 1);
        assert_eq!(counts.get(&a.origin().build), Some(&7));
        assert_eq!(observations.take_lost(|_, _| panic!("loss repeated")), 0);
    }

    #[test]
    fn counted_losses_saturate_both_tallies() {
        let (sink, observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let a = call("a");
        let b = call("b");
        let max = NonZeroU64::new(u64::MAX).expect("nonzero");
        sink.lose(&a.origin().build, max);
        sink.lose(&a.origin().build, NonZeroU64::MIN);
        sink.lose_unattributed(max);
        sink.lose(&b.origin().build, NonZeroU64::MIN);
        sink.lose_unattributed(NonZeroU64::MIN);
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            u64::MAX
        );
        assert_eq!(counts.get(&a.origin().build), Some(&u64::MAX));
        assert_eq!(observations.take_lost(|_, _| panic!("loss repeated")), 0);
    }

    #[test]
    fn counted_losses_are_recorded_after_receiver_closes() {
        let (sink, observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        drop(observations);
        let a = call("a");
        sink.lose(&a.origin().build, NonZeroU64::new(3).expect("nonzero"));
        sink.lose_unattributed(NonZeroU64::new(5).expect("nonzero"));
        assert_eq!(sink.lost.each.lock().get(&a.origin().build), Some(&3));
        assert_eq!(sink.lost.unattributed.load(Ordering::Relaxed), 5);
        assert!(sink.lost.dirty.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn lost_wakes_without_a_further_observation() {
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let observation = call("a");
        sink.record(observation.clone());
        sink.record(observation.clone());
        assert_eq!(
            observations.next().await,
            Some(Delivery(Some(observation.clone())))
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), observations.next())
                .await
                .expect("loss wakes the recorder"),
            Some(Delivery(None))
        );
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            0
        );
        assert_eq!(counts.get(&observation.origin().build), Some(&1));
    }

    #[tokio::test]
    async fn full_queue_counts_per_build_once_and_receiver_drains() {
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::new(2).expect("nonzero"));
        let a = call("a");
        let b = call("b");
        sink.record(a.clone());
        sink.record(a.clone());
        sink.record(b.clone());
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            0
        );
        assert_eq!(counts.get(&a.origin().build), Some(&1));
        assert_eq!(counts.get(&b.origin().build), Some(&1));
        assert_eq!(observations.take_lost(|_, _| panic!("loss repeated")), 0);
        assert_eq!(observations.next().await, Some(Delivery(Some(a))));
        let another = sink.clone();
        drop(sink);
        drop(another);
        assert_eq!(observations.next().await, None);
    }

    #[tokio::test]
    async fn closing_channel_delivers_pending_losses_before_none() {
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let observation = call("a");
        sink.record(observation.clone());
        sink.record(observation.clone());
        drop(sink);
        assert_eq!(
            observations.next().await,
            Some(Delivery(Some(observation.clone())))
        );
        assert_eq!(observations.next().await, Some(Delivery(None)));
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            0
        );
        assert_eq!(counts.get(&observation.origin().build), Some(&1));
        assert_eq!(observations.next().await, None);
    }

    #[test]
    fn losses_beyond_build_capacity_are_unattributed() {
        let (sink, observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let a = call("a");
        sink.record(a.clone());
        sink.record(a.clone());
        sink.record(call("b"));
        let mut counts = HashMap::new();
        assert_eq!(
            observations.take_lost(|key, count| {
                counts.insert(key, count);
            }),
            1
        );
        assert_eq!(counts.len(), 1);
        assert_eq!(counts.get(&a.origin().build), Some(&1));
        assert_eq!(observations.take_lost(|_, _| panic!("loss repeated")), 0);
    }
}
