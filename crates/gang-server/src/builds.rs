//! Shared live builds and the observation recorder.

use std::convert::Infallible;
use std::sync::Arc;

use futures::FutureExt;
use gang_clock::Clock;
use gang_config::BuildsConfig;
use gang_insight::{Builds, Outcome};
use gang_spi::{CertificateSignerPort, Observation, ObservationSink, Observations};
use parking_lot::Mutex;

use crate::builds_forward::{Forwarding, Uplink};
use crate::leadership::Leading;

/// The builds held by the recorder and read by the API and publisher.
#[derive(Clone)]
pub(crate) struct BuildsCell(Arc<Mutex<Builds>>);

impl BuildsCell {
    /// Creates an empty store with the configured bounds.
    pub(crate) fn new(config: &BuildsConfig) -> Self {
        Self(Arc::new(Mutex::new(Builds::new(config))))
    }

    /// Creates a store and the bounded channel feeding its recorder.
    pub(crate) fn channel(config: &BuildsConfig) -> (Self, ObservationSink, Observations) {
        let (sink, observations) = ObservationSink::channel(config.queue, config.max_live);
        (Self::new(config), sink, observations)
    }

    /// Reads the builds while holding the store lock.
    pub(crate) fn read<T>(&self, read: impl FnOnce(&Builds) -> T) -> T {
        read(&self.0.lock())
    }

    /// Folds a fixture observation at an explicit time.
    #[cfg(test)]
    pub(crate) fn observe(&self, at: gang_clock::Timestamp, observation: &gang_spi::Observation) {
        self.0.lock().observe(at, observation);
    }

    /// Folds observations and losses until the recorder is cancelled.
    pub(crate) async fn record<C: Clock>(
        &self,
        mut observations: Observations,
        clock: &C,
    ) -> Infallible {
        while let Some(delivery) = observations.next().await {
            if let Some(observation) = delivery.into_observation() {
                self.0.lock().observe(clock.wall(), &observation);
            }
            self.drain_lost(&observations, clock);
        }
        self.drain_lost(&observations, clock);
        std::future::pending().await
    }

    /// Forwards follower observations until promotion, then folds locally.
    ///
    /// The process's cumulative traffic rides on every batch. The first batch leaves at once, so
    /// the leader learns the series and its baseline early; afterwards the totals are sampled at
    /// least every pause, so a change without calls reaches the leader within one pause.
    pub(crate) async fn record_or_forward<C: Clock, S: CertificateSignerPort, U: Uplink>(
        &self,
        mut observations: Observations,
        clock: &C,
        signer: &S,
        leading: &Leading,
        forwarding: Option<Forwarding<U>>,
    ) -> Infallible {
        let Some(mut forwarding) = forwarding else {
            return self.record(observations, clock).await;
        };
        // The pause pending items wait out starts when the outbox was last empty, so anything that
        // becomes pending, a traffic change included, leaves within one pause.
        let mut window = clock.monotonic();
        let mut reachable = true;
        let mut due = true;
        while !leading.get() {
            if !due {
                let wait = forwarding
                    .pause
                    .saturating_sub(clock.monotonic().saturating_duration_since(window));
                let received = tokio::select! {
                    biased;
                    () = leading.wait() => break,
                    received = clock.timeout(wait, observations.next()) => received,
                };
                match received {
                    Ok(Some(delivery)) => {
                        if let Some(observation) = delivery.into_observation() {
                            forwarding.outbox.push(observation);
                        }
                    }
                    Ok(None) => {
                        leading.wait().await;
                        break;
                    }
                    Err(_) => due = true,
                }
            }
            let unattributed =
                observations.take_lost(|key, count| forwarding.outbox.lose(key, count));
            forwarding.outbox.lose_unattributed(unattributed);
            while !forwarding.outbox.is_full() {
                let Some(Some(delivery)) = observations.next().now_or_never() else {
                    break;
                };
                if let Some(observation) = delivery.into_observation() {
                    forwarding.outbox.push(observation);
                }
                let unattributed =
                    observations.take_lost(|key, count| forwarding.outbox.lose(key, count));
                forwarding.outbox.lose_unattributed(unattributed);
            }
            forwarding
                .outbox
                .traffic(forwarding.start, observations.traffic());
            if leading.get() {
                continue;
            }
            if forwarding.outbox.is_empty() {
                window = clock.monotonic();
                due = false;
                continue;
            }
            if !due
                && !forwarding.outbox.is_full()
                && clock.monotonic().saturating_duration_since(window) < forwarding.pause
            {
                continue;
            }
            due = false;
            forward_pending(&mut forwarding, clock, signer, leading, &mut reachable).await;
            if forwarding.outbox.is_empty() {
                window = clock.monotonic();
            }
        }
        let (calls, lost, unattributed) = forwarding.outbox.into_local();
        let at = clock.wall();
        {
            let mut builds = self.0.lock();
            for call in calls {
                builds.observe(at, &Observation::Call(call));
            }
            for (key, count) in lost {
                builds.lose(at, &key, count);
            }
        }
        if unattributed != 0 {
            tracing::warn!(count = unattributed, "unattributed build observations lost");
        }
        self.record(observations, clock).await
    }

    /// Folds all loss counts observed so far.
    fn drain_lost<C: Clock>(&self, observations: &Observations, clock: &C) {
        let at = clock.wall();
        let mut builds = self.0.lock();
        let unattributed = observations.take_lost(|key, count| builds.lose(at, &key, count));
        if unattributed != 0 {
            tracing::warn!(count = unattributed, "unattributed build observations lost");
        }
    }
}

/// Sends pending follower observations until the outbox is drained or forwarding stops.
async fn forward_pending<C: Clock, S: CertificateSignerPort, U: Uplink>(
    forwarding: &mut Forwarding<U>,
    clock: &C,
    signer: &S,
    leading: &Leading,
    reachable: &mut bool,
) {
    while !forwarding.outbox.is_empty() && !leading.get() {
        let result = tokio::select! {
            biased;
            () = leading.wait() => break,
            result = forwarding.proofs.current(signer, clock.wall()) => result,
        };
        if leading.get() {
            break;
        }
        let proof = match result {
            Ok(proof) => proof,
            Err(error) => {
                if *reachable {
                    tracing::warn!(%error, "build observation forwarding unavailable");
                    *reachable = false;
                }
                if let Some(dispatch) = forwarding.outbox.next_dispatch() {
                    forwarding.outbox.resolve(dispatch, Outcome::NotApplied);
                }
                tokio::select! {
                    biased;
                    () = leading.wait() => {}
                    () = clock.sleep(forwarding.pause) => {}
                }
                break;
            }
        };
        let Some(dispatch) = forwarding.outbox.next_dispatch() else {
            break;
        };
        let carried = dispatch.carried_losses();
        let bytes = match serde_json::to_vec(dispatch.request()) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::error!(%error, "failed to encode build observations");
                forwarding.outbox.resolve(dispatch, Outcome::NotApplied);
                tokio::select! {
                    biased;
                    () = leading.wait() => {}
                    () = clock.sleep(forwarding.pause) => {}
                }
                break;
            }
        };
        let (outcome, promoted) = tokio::select! {
            biased;
            () = leading.wait() => (Outcome::Unknown, true),
            outcome = forwarding.uplink.send(proof, bytes.into()) => (outcome, false),
        };
        if outcome == Outcome::Unknown && carried != 0 {
            tracing::warn!(
                dropped = carried,
                "build observation loss counts dropped after an ambiguous send"
            );
        }
        forwarding.outbox.resolve(dispatch, outcome);
        if promoted {
            break;
        }
        if outcome == Outcome::Accepted {
            if !*reachable {
                tracing::info!("build observation forwarding restored");
                *reachable = true;
            }
            if leading.get() {
                break;
            }
        } else {
            if *reachable {
                tracing::warn!("build observation forwarding unavailable");
                *reachable = false;
            }
            if leading.get() {
                break;
            }
            tokio::select! {
                biased;
                () = leading.wait() => {}
                () = clock.sleep(forwarding.pause) => {}
            }
            break;
        }
    }
}
