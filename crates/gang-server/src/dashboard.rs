//! The operator dashboard of one server: the hub its viewers read, the publisher that feeds it
//! after every control pass, and the actions refresher that keeps the actions sample current.

use std::convert::Infallible;
use std::time::Duration;

use gang_clock::Clock;
use gang_config::DashboardConfig;
use gang_spi::{ListingLimits, ScalingEventSeq, SchedulerPort, StateRepositoryPort};
use gang_state::{Builder, BuilderConfig};

use crate::actions::{ActionsCache, ActionsRefresher};
use crate::api::ApiDashboard;
use crate::errors::ServerError;
use crate::events::wire_event;
use crate::hub::{HubConfig, StateHub};
use crate::principals::RevokedPrincipals;
use crate::publish::StatePublisher;

/// The dashboard of one server, built from its settings before the control loop starts.
pub(crate) struct Dashboard {
    hub: StateHub,
    cache: ActionsCache,
    config: DashboardConfig,
    provisioner: String,
    poll_interval: Duration,
}

impl Dashboard {
    /// Builds the dashboard with the initial state: the control loop has not run a pass, so the
    /// state reports it skipped, with an empty fleet and no actions. The state holds the newest
    /// `recent_events` scaling events of the ledger, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::DashboardEvents`] when the scaling events cannot be read.
    pub(crate) async fn start<R: StateRepositoryPort, C: Clock>(
        config: DashboardConfig,
        provisioner: String,
        poll_interval: Duration,
        principals_epoch: u64,
        clock: &C,
        repository: &R,
    ) -> Result<Self, ServerError> {
        let mut recorded = Vec::new();
        repository
            .scaling_events(
                ScalingEventSeq::default(),
                None,
                config.recent_events,
                &mut recorded,
            )
            .await
            .map_err(ServerError::DashboardEvents)?;
        let state = Builder::new(builder_config(&config, &provisioner, poll_interval))
            .initial(clock.wall(), principals_epoch);
        let hub = StateHub::new(
            HubConfig {
                max_viewers: config.max_viewers,
                retry_after: config.retry_after,
                lag_frames: config.lag_frames,
                recent_events: config.recent_events,
            },
            state,
        );
        hub.preload(recorded.into_iter().map(wire_event).collect());
        Ok(Self {
            hub,
            cache: ActionsCache::default(),
            config,
            provisioner,
            poll_interval,
        })
    }

    /// The hub and heartbeat the Gang API serves the dashboard with.
    #[must_use]
    pub(crate) fn api(&self) -> ApiDashboard {
        ApiDashboard {
            hub: self.hub.clone(),
            heartbeat: self.config.heartbeat,
        }
    }

    /// The publisher that turns each control pass into the next state frame of the hub.
    #[must_use]
    pub(crate) fn publisher(&self) -> StatePublisher {
        StatePublisher::new(
            self.hub.clone(),
            builder_config(&self.config, &self.provisioner, self.poll_interval),
            self.cache.clone(),
        )
    }

    /// The refresher that stores the scheduler's actions in the cache at the detail interval.
    #[must_use]
    pub(crate) fn refresher<'a, S: SchedulerPort, C: Clock>(
        &self,
        scheduler: &'a S,
        clock: &'a C,
    ) -> ActionsRefresher<'a, S, C> {
        ActionsRefresher::new(
            scheduler,
            clock,
            self.cache.clone(),
            ListingLimits {
                running: self.config.max_listed_running,
                queued: self.config.max_listed_queued,
            },
            self.config.detail_interval,
        )
    }

    /// Ends every viewer's stream.
    pub(crate) fn shutdown(&self) {
        self.hub.shutdown();
    }

    /// Publishes each change of the revoked principals to the viewers. Never completes: once the
    /// set can no longer change, it waits forever.
    pub(crate) async fn forward_principals(
        &self,
        revoked_principals: &RevokedPrincipals,
    ) -> Infallible {
        let mut revoked = revoked_principals.subscribe();
        loop {
            if revoked.changed().await.is_err() {
                return std::future::pending().await;
            }
            let epoch = revoked.borrow_and_update().epoch;
            self.hub.principals_changed(epoch);
        }
    }
}

fn builder_config(
    config: &DashboardConfig,
    provisioner: &str,
    poll_interval: Duration,
) -> BuilderConfig {
    BuilderConfig {
        events_limit: config.recent_events.get(),
        stale_after: config.actions_stale_after(),
        interval: poll_interval,
        provisioner: provisioner.to_owned(),
    }
}
