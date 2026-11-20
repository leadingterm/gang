//! The leader lease of a `SQLite` deployment. The plugin takes the database file's exclusive lock
//! when it is built and holds it for the life of the process, so exactly one controller can use
//! the database: that lock is the lease. Registering a host always returns priority one, ignoring
//! its identity; registration, taking, renewal and release never touch the database, so a busy
//! database can never lapse the lease and fence the only controller.

use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, CandidateGeneration, CandidatePriority, ControllerHostIdentity, ControllerId,
    LeaderLeasePort, Lease, LeaseCandidate, LeaseEpoch, PortError, Renewal, Takeover,
};

use crate::Sqlite;

/// The epoch of the only taking the file lock admits.
const ONLY_TAKING: LeaseEpoch = LeaseEpoch::new(1);

impl LeaderLeasePort for Sqlite {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        _host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        Ok(LeaseCandidate {
            holder: holder.clone(),
            priority: CandidatePriority::registered(CandidateGeneration::new(1)),
        })
    }

    async fn acquire(
        &self,
        candidate: &LeaseCandidate,
        _now: Timestamp,
        _stale_before: Timestamp,
        expires_at: Timestamp,
        _takeover: Takeover,
    ) -> Result<Acquisition, PortError> {
        Ok(Acquisition::Acquired(Lease {
            holder: candidate.holder.clone(),
            epoch: ONLY_TAKING,
            expires_at,
        }))
    }

    async fn renew(
        &self,
        lease: &Lease,
        _now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Renewal, PortError> {
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
    use gang_types::InstanceId;
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
        let host = ControllerHostIdentity::Launched {
            at: at(42),
            instance: InstanceId::try_new("sqlite-host")?,
        };
        let candidate = pin!(db.register_candidate(&holder, &host)).poll(&mut cx);
        let candidate = match candidate {
            Poll::Ready(Ok(candidate)) => candidate,
            other => return Err(format!("registration blocked: {other:?}").into()),
        };
        assert_eq!(
            candidate.priority,
            CandidatePriority::registered(CandidateGeneration::new(1))
        );
        let acquired =
            pin!(db.acquire(&candidate, at(0), at(0), at(10), Takeover::Observe)).poll(&mut cx);
        let lease = Lease {
            holder: holder.clone(),
            epoch: LeaseEpoch::new(1),
            expires_at: at(10),
        };
        let renewed = pin!(db.renew(&lease, at(0), at(20))).poll(&mut cx);
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
