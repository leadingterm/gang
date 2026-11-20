//! Refreshes revocations written by other controllers.

use std::convert::Infallible;

use gang_clock::Clock;
use gang_spi::{
    AuthenticationPort, ExpiringRecordPort, PortError, RevocationEpoch, StateRepositoryPort,
};
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
        if let Ok(true) = apply_snapshot(core, certificates).await {
            applied = next;
        }
    }
}

/// Reloads revocations before admitting requests as leader.
pub(crate) async fn synchronize_revocations<K: Stack>(
    core: &Core<K>,
    certificates: &RevocationSet,
) -> Result<(), PortError> {
    let _guard = core.revocation_lock.lock().await;
    if !apply_snapshot(core, certificates).await? {
        return Err(PortError::unavailable(
            "revocations changed during synchronization",
        ));
    }
    Ok(())
}

/// Reads both snapshots before applying either; the caller holds the revocation lock.
async fn apply_snapshot<K: Stack>(
    core: &Core<K>,
    certificates: &RevocationSet,
) -> Result<bool, PortError> {
    // Do not overwrite a direct local change that completes during these reads.
    let local_epoch = core.host.revoked_principals.epoch();
    let serials = core.host.repository.revoked().await.inspect_err(|error| {
        tracing::warn!(%error, "cannot refresh revoked certificates");
    })?;
    let principals = core
        .host
        .repository
        .revoked_principals()
        .await
        .inspect_err(|error| {
            tracing::warn!(%error, "cannot refresh revoked principals");
        })?;
    for record in serials {
        core.workers.forget_revoked_certificate(&record.serial);
        certificates.revoke(record.serial);
    }
    Ok(core.host.revoked_principals.replace_if_epoch(
        local_epoch,
        principals.into_iter().map(|principal| principal.identity),
    ))
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
