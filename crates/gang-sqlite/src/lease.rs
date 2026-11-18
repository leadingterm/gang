//! The leader lease of a `SQLite` deployment. The plugin takes the database file's exclusive lock
//! when it is built and holds it for the life of the process, so exactly one controller can use
//! the database: that lock is the lease. Taking, renewing and releasing it never touch the
//! database, so a busy database can never lapse it and fence the only controller.

use gang_clock::Timestamp;
use gang_spi::{Acquisition, ControllerId, LeaderLeasePort, Lease, LeaseEpoch, PortError, Renewal};

use crate::Sqlite;

/// The epoch of the only taking the file lock admits.
const ONLY_TAKING: LeaseEpoch = LeaseEpoch::new(1);

impl LeaderLeasePort for Sqlite {
    async fn acquire(
        &self,
        holder: &ControllerId,
        _stale_before: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Acquisition, PortError> {
        Ok(Acquisition::Acquired(Lease {
            holder: holder.clone(),
            epoch: ONLY_TAKING,
            expires_at,
        }))
    }

    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError> {
        Ok(Renewal::Renewed(Lease {
            expires_at,
            ..lease.clone()
        }))
    }

    async fn release(&self, _lease: &Lease) -> Result<(), PortError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use gang_spi::{Plugin, PluginContext};
    use tempfile::TempDir;

    use super::*;
    use crate::SqliteConfig;

    /// The lease of the only controller a locked `SQLite` file admits: taking, renewing and
    /// releasing it never wait for the database, so a database busy past the lease's ttl (a long
    /// statement, a slow disk) can never lapse it and fence the controller.
    #[tokio::test]
    async fn the_lease_never_waits_for_a_busy_database() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };
        let db = Sqlite::build(SqliteConfig::new(dir.path().join("gang.db")), &context)?;
        db.run(|_| Ok(())).await?;
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let mut busy = pin!(db.run(move |_| {
            let _ = blocked.recv();
            Ok(())
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            busy.as_mut().poll(&mut cx).is_pending(),
            "the database is busy"
        );

        let holder = ControllerId::new("controller-a".to_owned());
        let at = |millis| Timestamp::from_unix_millis(millis);
        let acquired = pin!(db.acquire(&holder, at(0), at(10))).poll(&mut cx);
        let lease = Lease {
            holder: holder.clone(),
            epoch: LeaseEpoch::new(1),
            expires_at: at(10),
        };
        let renewed = pin!(db.renew(&lease, at(20))).poll(&mut cx);
        let released = pin!(db.release(&lease)).poll(&mut cx);
        release.send(())?;
        busy.await?;

        assert!(
            matches!(&acquired, Poll::Ready(Ok(Acquisition::Acquired(taken))) if *taken == lease),
            "{acquired:?}"
        );
        assert!(
            matches!(&renewed, Poll::Ready(Ok(Renewal::Renewed(kept))) if kept.expires_at == at(20) && kept.epoch == lease.epoch),
            "{renewed:?}"
        );
        assert!(matches!(released, Poll::Ready(Ok(()))), "{released:?}");
        Ok(())
    }
}
