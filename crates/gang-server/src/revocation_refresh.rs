//! Refreshes revocations written by other controllers.

use std::convert::Infallible;

use gang_clock::Clock;
use gang_spi::{AuthenticationPort, ExpiringRecordPort, RevocationEpoch, StateRepositoryPort};
use gang_tls::RevocationSet;
use gang_types::NonZeroDuration;

use crate::core::Core;
use crate::deployment::Stack;

/// Polls the durable epoch and applies each newly observed snapshot.
pub(crate) async fn refresh_revocations<K: Stack>(
    core: &Core<K>,
    certificates: &RevocationSet,
    mut applied: RevocationEpoch,
    interval: NonZeroDuration,
) -> Infallible {
    loop {
        core.host.clock.sleep(interval.get()).await;
        let _guard = core.revocation_lock.lock().await;
        let next = match core.host.repository.revocation_epoch().await {
            Ok(next) => next,
            Err(error) => {
                tracing::warn!(%error, "cannot read revocation epoch");
                continue;
            }
        };
        if next == applied {
            continue;
        }
        // Do not overwrite a local revocation or restore that completes during these reads.
        let local_epoch = core.host.revoked_principals.epoch();
        let serials = match core.host.repository.revoked().await {
            Ok(serials) => serials,
            Err(error) => {
                tracing::warn!(%error, "cannot refresh revoked certificates");
                continue;
            }
        };
        let principals = match core.host.repository.revoked_principals().await {
            Ok(principals) => principals,
            Err(error) => {
                tracing::warn!(%error, "cannot refresh revoked principals");
                continue;
            }
        };
        for record in serials {
            core.workers.forget_revoked_certificate(&record.serial);
            certificates.revoke(record.serial);
        }
        if !core.host.revoked_principals.replace_if_epoch(
            local_epoch,
            principals.into_iter().map(|principal| principal.identity),
        ) {
            continue;
        }
        applied = next;
    }
}

/// Periodically requests cleanup independently of revocation refresh.
pub(crate) async fn purge_expired_records<K: Stack>(
    core: &Core<K>,
    interval: NonZeroDuration,
) -> Infallible {
    loop {
        core.host.clock.sleep(interval.get()).await;
        if let Err(error) = core.host.repository.purge(core.host.clock.wall()).await {
            tracing::warn!(%error, "cannot purge expired records");
        }
    }
}
