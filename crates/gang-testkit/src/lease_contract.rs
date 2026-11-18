//! Contract cases for the leader lease, shared by database implementations.

use gang_clock::Timestamp;
use gang_spi::{Acquisition, ControllerId, LeaderLeasePort, Lease, LeaseEpoch, Renewal};

use crate::TestResult;

fn ts(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn lease(holder: &ControllerId, epoch: u64, expires_at: u64) -> Lease {
    Lease {
        holder: holder.clone(),
        epoch: LeaseEpoch::new(epoch),
        expires_at: ts(expires_at),
    }
}

/// A free lease is acquired at epoch one.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if acquisition differs from the contract.
pub async fn lease_first_acquire<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    assert_eq!(
        database.acquire(&a, ts(1), ts(10)).await?,
        Acquisition::Acquired(lease(&a, 1, 10))
    );
    Ok(())
}

/// A live lease is held unchanged and a strictly stale lease is taken at the next epoch.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if acquisition differs from the contract.
pub async fn lease_live_then_stale<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    assert_eq!(
        database.acquire(&a, ts(1), ts(10)).await?,
        Acquisition::Acquired(lease(&a, 1, 10))
    );
    assert_eq!(
        database.acquire(&b, ts(9), ts(20)).await?,
        Acquisition::Held(lease(&a, 1, 10))
    );
    assert_eq!(
        database.acquire(&b, ts(11), ts(20)).await?,
        Acquisition::Acquired(lease(&b, 2, 20))
    );
    Ok(())
}

/// Expiry equal to the stale boundary remains held, even for the holder.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if the boundary is treated as stale.
pub async fn lease_equal_boundary<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    database.acquire(&a, ts(1), ts(10)).await?;
    for holder in [&a, &b] {
        assert_eq!(
            database.acquire(holder, ts(10), ts(30)).await?,
            Acquisition::Held(lease(&a, 1, 10))
        );
    }
    Ok(())
}

/// A holder can renew and the new expiry is stored.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if renewal differs from the contract.
pub async fn lease_holder_renews<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    database.acquire(&a, ts(1), ts(10)).await?;
    let renewed = lease(&a, 1, 20);
    assert_eq!(
        database.renew(&lease(&a, 1, 10), ts(20)).await?,
        Renewal::Renewed(renewed.clone())
    );
    assert_eq!(
        database.acquire(&b, ts(15), ts(30)).await?,
        Acquisition::Held(renewed)
    );
    Ok(())
}

/// An expired lease can still be renewed until another controller takes it.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if renewal before or after a taking differs from the contract.
pub async fn lease_renew_after_expiry_and_taking<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    database.acquire(&a, ts(1), ts(10)).await?;
    let old = lease(&a, 1, 10);
    assert_eq!(
        database.renew(&old, ts(20)).await?,
        Renewal::Renewed(lease(&a, 1, 20))
    );
    assert_eq!(
        database.acquire(&b, ts(21), ts(30)).await?,
        Acquisition::Acquired(lease(&b, 2, 30))
    );
    assert_eq!(database.renew(&old, ts(40)).await?, Renewal::Lost);
    assert_eq!(
        database.acquire(&a, ts(25), ts(40)).await?,
        Acquisition::Held(lease(&b, 2, 30))
    );
    Ok(())
}

/// Renewals with a wrong epoch or holder are lost without changing the lease.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if an invalid renewal changes the lease.
pub async fn lease_invalid_renewals<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    database.acquire(&a, ts(1), ts(10)).await?;
    database.acquire(&a, ts(11), ts(20)).await?;
    assert_eq!(
        database.renew(&lease(&a, 1, 10), ts(99)).await?,
        Renewal::Lost
    );
    assert_eq!(
        database.renew(&lease(&b, 2, 20), ts(99)).await?,
        Renewal::Lost
    );
    assert_eq!(
        database.acquire(&b, ts(20), ts(30)).await?,
        Acquisition::Held(lease(&a, 2, 20))
    );
    Ok(())
}

/// Release clears the holder but retains the epoch, and prevents renewal.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if release or reacquisition differs from the contract.
pub async fn lease_release<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    database.acquire(&a, ts(1), ts(10)).await?;
    let old = lease(&a, 1, 10);
    database.release(&old).await?;
    assert_eq!(database.renew(&old, ts(20)).await?, Renewal::Lost);
    assert_eq!(
        database.acquire(&b, ts(1), ts(30)).await?,
        Acquisition::Acquired(lease(&b, 2, 30))
    );
    Ok(())
}

/// Releases by a wrong holder or epoch leave the current lease unchanged.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if an invalid release changes the lease.
pub async fn lease_invalid_release<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    database.acquire(&a, ts(1), ts(10)).await?;
    database.acquire(&a, ts(11), ts(20)).await?;
    database.release(&lease(&a, 1, 10)).await?;
    database.release(&lease(&b, 2, 20)).await?;
    assert_eq!(
        database.acquire(&b, ts(20), ts(30)).await?,
        Acquisition::Held(lease(&a, 2, 20))
    );
    Ok(())
}

/// Concurrent contenders have exactly one winner and the loser sees that lease.
///
/// # Errors
/// Returns a database error or an error if the contenders do not produce one winner.
///
/// # Panics
/// Panics if the winning and held leases disagree.
pub async fn lease_concurrent_acquire<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let b = ControllerId::new("b");
    let (first, second) = tokio::join!(
        database.acquire(&a, ts(1), ts(10)),
        database.acquire(&b, ts(1), ts(20))
    );
    match (first?, second?) {
        (Acquisition::Acquired(winner), Acquisition::Held(held)) => {
            assert_eq!(winner, lease(&a, 1, 10));
            assert_eq!(held, winner);
        }
        (Acquisition::Held(held), Acquisition::Acquired(winner)) => {
            assert_eq!(winner, lease(&b, 1, 20));
            assert_eq!(held, winner);
        }
        outcomes => {
            return Err(
                format!("expected one acquisition and one held lease, got {outcomes:?}").into(),
            );
        }
    }
    Ok(())
}
