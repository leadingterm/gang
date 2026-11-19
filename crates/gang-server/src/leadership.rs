//! Leadership: the leader lease taken, held and released on the injected clock, and the
//! validity of the fence that controls what the leader may still change.

use std::sync::{Arc, OnceLock};

use gang_clock::{Clock, Tick, Timestamp};
use gang_config::LeadershipConfig;
use gang_spi::{
    Acquisition, ControllerId, Fence, Keeper, LeaderLeasePort, Lease, PortError, Renewal,
};

/// The `stale_before` under which no lease counts as stale, so only a released lease is free.
const ONLY_RELEASED: Timestamp = Timestamp::from_unix_millis(0);

/// Whether this controller leads, and since when: set once when its term starts, after it takes
/// the lease and its cache baseline, and never cleared, since a leader that loses the lease hands
/// off and exits. Requests read it to serve a leader route locally or forward it to the leader;
/// ingest reads the start to tell a follower's traffic of this term from traffic it counted
/// before.
#[derive(Debug, Clone, Default)]
pub(crate) struct Leading(Arc<LeadingState>);

#[derive(Debug, Default)]
struct LeadingState {
    start: OnceLock<Timestamp>,
    changed: tokio::sync::Notify,
}

impl Leading {
    /// Whether this controller leads.
    pub(crate) fn get(&self) -> bool {
        self.0.start.get().is_some()
    }

    /// The wall time this controller began leading, once it leads.
    pub(crate) fn start(&self) -> Option<Timestamp> {
        self.0.start.get().copied()
    }

    /// Records that this controller leads since `at`. Only the first call sets the start.
    pub(crate) fn set(&self, at: Timestamp) {
        let _ = self.0.start.set(at);
        self.0.changed.notify_waiters();
    }

    /// Waits until this controller leads.
    pub(crate) async fn wait(&self) {
        let notified = self.0.changed.notified();
        tokio::pin!(notified);
        // Register before checking the flag so a concurrent set cannot be missed.
        notified.as_mut().enable();
        if !self.get() {
            notified.await;
        }
    }
}

/// One controller's hold on the leader lease.
pub(crate) struct Leadership<'a, L, C> {
    lease: &'a L,
    clock: &'a Arc<C>,
    controller: &'a ControllerId,
    config: LeadershipConfig,
    keeper: Keeper,
    fence: Fence,
}

impl<'a, L: LeaderLeasePort, C: Clock + 'static> Leadership<'a, L, C> {
    /// The leadership of `controller` over the lease in `lease`, timed by `config`.
    pub(crate) fn new(
        lease: &'a L,
        clock: &'a Arc<C>,
        controller: &'a ControllerId,
        config: LeadershipConfig,
    ) -> Self {
        let (fence, keeper) =
            Fence::lease(Arc::clone(clock) as Arc<dyn Clock>, config.call_timeout);
        Self {
            lease,
            clock,
            controller,
            config,
            keeper,
            fence,
        }
    }

    /// The fence this leadership keeps: it holds while the lease is valid.
    pub(crate) fn fence(&self) -> &Fence {
        &self.fence
    }

    /// Resolves with the lease once this controller takes it. It tries at once, then every
    /// `renew_interval`; a lease counts as free once it was released, or once it expired more than
    /// `clock_skew` ago by this controller's clock. A failed attempt is logged and retried.
    ///
    /// An expired lease is taken only after this controller has reached the database for a whole
    /// `ttl`: a leader that could not renew while the database was down renews within
    /// `renew_interval` of it answering again, so it keeps its lease instead of losing it to
    /// whichever controller reaches the database first. Until then only a released lease is free.
    pub(crate) async fn acquire(&self) -> Lease {
        let mut reachable_since = None;
        loop {
            let started = self.clock.monotonic();
            let wall = self.clock.wall();
            let stale_before = if reachable_since.is_some_and(|since| {
                started.saturating_duration_since(since) >= self.config.ttl.get()
            }) {
                wall - self.config.clock_skew
            } else {
                ONLY_RELEASED
            };
            let attempt = self
                .clock
                .timeout(
                    self.config.renew_interval.get(),
                    self.lease
                        .acquire(self.controller, stale_before, wall + self.config.ttl.get()),
                )
                .await;
            match attempt {
                Ok(Ok(Acquisition::Acquired(lease))) => {
                    self.confirm(started);
                    return lease;
                }
                Ok(Ok(Acquisition::Held(_))) => {
                    reachable_since.get_or_insert_with(|| self.clock.monotonic());
                }
                Ok(Err(error)) => {
                    reachable_since = None;
                    tracing::warn!(%error, "cannot acquire the leader lease");
                }
                Err(error) => {
                    reachable_since = None;
                    tracing::warn!(%error, "timed out acquiring the leader lease");
                }
            }
            self.clock
                .sleep_until(started + self.config.renew_interval.get())
                .await;
        }
    }

    /// Renews `lease` every `renew_interval` and returns once it is lost. Each renewal extends the
    /// validity. A renewal that fails, or does not answer within `renew_interval`, keeps the
    /// lease held: the leader keeps scheduling while its validity lapses, and renews again.
    pub(crate) async fn hold(&self, lease: &mut Lease) {
        loop {
            self.clock.sleep(self.config.renew_interval.get()).await;
            let started = self.clock.monotonic();
            let expires_at = self.clock.wall() + self.config.ttl.get();
            match self.renew(lease, expires_at).await {
                Ok(Renewal::Renewed(renewed)) => {
                    *lease = renewed;
                    self.confirm(started);
                }
                Ok(Renewal::Lost) => {
                    self.keeper.revoke();
                    return;
                }
                Err(error) => tracing::warn!(
                    %error,
                    "cannot renew the leader lease; fleet changes stop once it lapses"
                ),
            }
        }
    }

    /// Revokes authority and drains registered work before freeing `lease`, so another controller
    /// cannot take it while local mutations are still running. A release failure is logged: the
    /// lease then expires by itself.
    pub(crate) async fn release(&self, lease: &Lease) {
        self.keeper.revoke();
        self.keeper.quiesced().await;
        match self
            .clock
            .timeout(self.config.renew_interval.get(), self.lease.release(lease))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "cannot release the leader lease"),
            Err(error) => tracing::warn!(%error, "timed out releasing the leader lease"),
        }
    }

    /// One renewal, bounded by `renew_interval`.
    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError> {
        self.clock
            .timeout(
                self.config.renew_interval.get(),
                self.lease.renew(lease, expires_at),
            )
            .await
            .map_err(PortError::unavailable)?
    }

    /// Extends the validity for a lease confirmed by a call that started at `started`.
    fn confirm(&self, started: Tick) {
        let valid_for = self.config.ttl.get().saturating_sub(self.config.clock_skew);
        self.keeper.extend_to(started + valid_for);
    }
}
