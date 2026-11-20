//! Contract cases for the leader lease, shared by database implementations.

use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, CandidateGeneration, CandidatePriority, ControllerHostIdentity, ControllerId,
    LeaderLeasePort, Lease, LeaseCandidate, LeaseEpoch, Renewal, Takeover,
};
use gang_types::InstanceId;

use crate::TestResult;

fn ts(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn host(name: &str) -> Result<ControllerHostIdentity, Box<dyn std::error::Error>> {
    Ok(ControllerHostIdentity::Persistent(InstanceId::try_new(
        name,
    )?))
}

fn candidate(holder: &ControllerId) -> LeaseCandidate {
    LeaseCandidate {
        holder: holder.clone(),
        priority: CandidatePriority::registered(CandidateGeneration::default()),
    }
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
        database
            .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
            .await?,
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
        database
            .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
            .await?,
        Acquisition::Acquired(lease(&a, 1, 10))
    );
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(9), ts(9), ts(20), Takeover::Observe)
            .await?,
        Acquisition::Held(lease(&a, 1, 10))
    );
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(11), ts(11), ts(20), Takeover::Observe)
            .await?,
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
    database
        .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
        .await?;
    for holder in [&a, &b] {
        assert_eq!(
            database
                .acquire(
                    &candidate(holder),
                    ts(10),
                    ts(10),
                    ts(30),
                    Takeover::Observe
                )
                .await?,
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
    database
        .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
        .await?;
    let renewed = lease(&a, 1, 20);
    assert_eq!(
        database.renew(&lease(&a, 1, 10), ts(0), ts(20)).await?,
        Renewal::Renewed(renewed.clone())
    );
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(15), ts(15), ts(30), Takeover::Observe)
            .await?,
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
    database
        .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
        .await?;
    let old = lease(&a, 1, 10);
    assert_eq!(
        database.renew(&old, ts(0), ts(20)).await?,
        Renewal::Renewed(lease(&a, 1, 20))
    );
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(21), ts(21), ts(30), Takeover::Observe)
            .await?,
        Acquisition::Acquired(lease(&b, 2, 30))
    );
    assert_eq!(database.renew(&old, ts(0), ts(40)).await?, Renewal::Lost);
    assert_eq!(
        database
            .acquire(&candidate(&a), ts(25), ts(25), ts(40), Takeover::Observe)
            .await?,
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
    database
        .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
        .await?;
    database
        .acquire(&candidate(&a), ts(11), ts(11), ts(20), Takeover::Observe)
        .await?;
    assert_eq!(
        database.renew(&lease(&a, 1, 10), ts(0), ts(99)).await?,
        Renewal::Lost
    );
    assert_eq!(
        database.renew(&lease(&b, 2, 20), ts(0), ts(99)).await?,
        Renewal::Lost
    );
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(20), ts(20), ts(30), Takeover::Observe)
            .await?,
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
    database
        .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
        .await?;
    let old = lease(&a, 1, 10);
    database.release(&old).await?;
    assert_eq!(database.renew(&old, ts(0), ts(20)).await?, Renewal::Lost);
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(1), ts(1), ts(30), Takeover::Observe)
            .await?,
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
    database
        .acquire(&candidate(&a), ts(1), ts(1), ts(10), Takeover::Observe)
        .await?;
    database
        .acquire(&candidate(&a), ts(11), ts(11), ts(20), Takeover::Observe)
        .await?;
    database.release(&lease(&a, 1, 10)).await?;
    database.release(&lease(&b, 2, 20)).await?;
    assert_eq!(
        database
            .acquire(&candidate(&b), ts(20), ts(20), ts(30), Takeover::Observe)
            .await?,
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
    let candidate_a = candidate(&a);
    let candidate_b = candidate(&b);
    let (first, second) = tokio::join!(
        database.acquire(&candidate_a, ts(1), ts(1), ts(10), Takeover::Observe),
        database.acquire(&candidate_b, ts(1), ts(1), ts(20), Takeover::Observe)
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

/// First registrations of distinct hosts are ordered; process restarts retain host priority.
///
/// # Errors
/// Returns a database or host-identity error.
///
/// # Panics
/// Panics when registration or restart ordering violates the contract.
pub async fn candidate_registration_order<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = ControllerId::new("a");
    let host_a = host("host-a")?;
    let first = database.register_candidate(&a, &host_a).await?;
    let holder_b = ControllerId::new("b");
    let holder_c = ControllerId::new("c");
    let host_b = host("host-b")?;
    let host_c = host("host-c")?;
    let (b, c) = tokio::join!(
        database.register_candidate(&holder_b, &host_b),
        database.register_candidate(&holder_c, &host_c)
    );
    let (b, c) = (b?, c?);
    assert_eq!(
        first.priority,
        CandidatePriority::registered(CandidateGeneration::new(1))
    );
    assert!(b.priority > first.priority);
    assert!(c.priority > first.priority);
    assert_ne!(b.priority, c.priority);
    let restarted = database
        .register_candidate(&ControllerId::new("a-restarted"), &host_a)
        .await?;
    assert_eq!(restarted.priority, first.priority);
    let shared_host = host("concurrent-host")?;
    let process_one = ControllerId::new("process-one");
    let process_two = ControllerId::new("process-two");
    let (one, two) = tokio::join!(
        database.register_candidate(&process_one, &shared_host),
        database.register_candidate(&process_two, &shared_host)
    );
    let (one, two) = (one?, two?);
    assert_eq!(one.priority, two.priority);
    assert!(one.priority > b.priority && one.priority > c.priority);
    let next = database
        .register_candidate(&ControllerId::new("next"), &host("next-host")?)
        .await?;
    assert!(next.priority > one.priority);
    restart_cannot_reverse_handoff(database, &host_a, &shared_host).await?;
    launched_registration_order(database).await
}

/// Requests yield only to newer, distinct candidates and preserve the renewed lease.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics when the handoff contract is violated.
pub async fn lease_handoff_requests<D: LeaderLeasePort>(database: &D) -> TestResult {
    let older = database
        .register_candidate(&ControllerId::new("older"), &host("older")?)
        .await?;
    let leader = database
        .register_candidate(&ControllerId::new("leader"), &host("leader")?)
        .await?;
    let newer = database
        .register_candidate(&ControllerId::new("newer"), &host("newer")?)
        .await?;
    let other = database
        .register_candidate(&ControllerId::new("other"), &host("other")?)
        .await?;
    let original = match database
        .acquire(&leader, ts(1), ts(0), ts(100), Takeover::Observe)
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("expected acquisition: {outcome:?}").into()),
    };
    assert!(matches!(
        database
            .acquire(&newer, ts(2), ts(0), ts(80), Takeover::Observe)
            .await?,
        Acquisition::Contending(_)
    ));
    assert!(matches!(
        database.renew(&original, ts(3), ts(110)).await?,
        Renewal::Renewed(_)
    ));
    for candidate in [&older, &leader] {
        assert!(matches!(
            database
                .acquire(candidate, ts(4), ts(0), ts(80), Takeover::Request)
                .await?,
            Acquisition::Held(_)
        ));
    }
    let equal = LeaseCandidate {
        holder: other.holder.clone(),
        priority: leader.priority.clone(),
    };
    let self_newer = LeaseCandidate {
        holder: leader.holder.clone(),
        priority: other.priority.clone(),
    };
    for candidate in [&equal, &self_newer] {
        assert!(matches!(
            database
                .acquire(candidate, ts(4), ts(0), ts(80), Takeover::Request)
                .await?,
            Acquisition::Held(_)
        ));
    }
    assert!(matches!(
        database.renew(&original, ts(5), ts(120)).await?,
        Renewal::Renewed(_)
    ));
    assert!(matches!(
        database
            .acquire(&newer, ts(6), ts(0), ts(90), Takeover::Request)
            .await?,
        Acquisition::Contending(_)
    ));
    assert_eq!(
        database.renew(&original, ts(7), ts(130)).await?,
        Renewal::YieldRequested(lease(&leader.holder, original.epoch.get(), 130))
    );
    handoff_successor(database, &newer, &other, &original).await
}

async fn handoff_successor<D: LeaderLeasePort>(
    database: &D,
    newer: &LeaseCandidate,
    other: &LeaseCandidate,
    original: &Lease,
) -> TestResult {
    assert!(matches!(
        database
            .acquire(other, ts(8), ts(0), ts(95), Takeover::Request)
            .await?,
        Acquisition::Contending(_)
    ));
    database.release(original).await?;
    assert_eq!(
        database
            .acquire(newer, ts(9), ts(0), ts(140), Takeover::Observe)
            .await?,
        Acquisition::Reserved
    );
    let wrong_id = LeaseCandidate {
        holder: newer.holder.clone(),
        priority: other.priority.clone(),
    };
    assert_eq!(
        database
            .acquire(&wrong_id, ts(9), ts(0), ts(140), Takeover::Observe)
            .await?,
        Acquisition::Reserved
    );
    let wrong_rank = LeaseCandidate {
        holder: other.holder.clone(),
        priority: newer.priority.clone(),
    };
    assert_eq!(
        database
            .acquire(&wrong_rank, ts(9), ts(0), ts(140), Takeover::Observe)
            .await?,
        Acquisition::Reserved
    );
    let next = match database
        .acquire(other, ts(9), ts(0), ts(140), Takeover::Observe)
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("expected reserved successor: {outcome:?}").into()),
    };
    assert_eq!(
        database.renew(original, ts(10), ts(150)).await?,
        Renewal::Lost
    );
    assert!(matches!(
        database.renew(&next, ts(10), ts(150)).await?,
        Renewal::Renewed(_)
    ));
    Ok(())
}

/// Reservations protect released and stale rows, expire, and admit a higher request.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics when reservation ordering is violated.
pub async fn lease_handoff_reservations<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = database
        .register_candidate(&ControllerId::new("a"), &host("a")?)
        .await?;
    let b = database
        .register_candidate(&ControllerId::new("b"), &host("b")?)
        .await?;
    let c = database
        .register_candidate(&ControllerId::new("c"), &host("c")?)
        .await?;
    let first = match database
        .acquire(&a, ts(1), ts(0), ts(10), Takeover::Observe)
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("expected acquisition: {outcome:?}").into()),
    };
    // Wall-expiry alone does not permit stealing, but must allow a request.
    assert!(matches!(
        database
            .acquire(&b, ts(11), ts(0), ts(30), Takeover::Request)
            .await?,
        Acquisition::Held(_)
    ));
    assert!(matches!(
        database.renew(&first, ts(12), ts(20)).await?,
        Renewal::YieldRequested(_)
    ));
    assert_eq!(
        database
            .acquire(&a, ts(21), ts(21), ts(50), Takeover::Observe)
            .await?,
        Acquisition::Reserved
    );
    let next = match database
        .acquire(&c, ts(21), ts(21), ts(50), Takeover::Request)
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("higher request must override: {outcome:?}").into()),
    };
    assert!(matches!(
        database.renew(&next, ts(22), ts(55)).await?,
        Renewal::Renewed(_)
    ));
    let d = database
        .register_candidate(&ControllerId::new("d"), &host("d")?)
        .await?;
    assert!(matches!(
        database
            .acquire(&d, ts(23), ts(0), ts(25), Takeover::Request)
            .await?,
        Acquisition::Contending(_)
    ));
    database.release(&next).await?;
    assert_eq!(
        database
            .acquire(&a, ts(24), ts(0), ts(60), Takeover::Observe)
            .await?,
        Acquisition::Reserved
    );
    assert!(matches!(
        database
            .acquire(&a, ts(25), ts(0), ts(60), Takeover::Observe)
            .await?,
        Acquisition::Acquired(_)
    ));
    Ok(())
}

/// Concurrent requests leave the highest eligible candidate reserved.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics when the lower-priority request wins.
pub async fn lease_concurrent_requests<D: LeaderLeasePort>(database: &D) -> TestResult {
    let a = database
        .register_candidate(&ControllerId::new("a"), &host("a")?)
        .await?;
    let b = database
        .register_candidate(&ControllerId::new("b"), &host("b")?)
        .await?;
    let c = database
        .register_candidate(&ControllerId::new("c"), &host("c")?)
        .await?;
    let leader = match database
        .acquire(&a, ts(1), ts(0), ts(100), Takeover::Observe)
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("expected acquisition: {outcome:?}").into()),
    };
    let (one, two) = tokio::join!(
        database.acquire(&b, ts(2), ts(0), ts(90), Takeover::Request),
        database.acquire(&c, ts(2), ts(0), ts(90), Takeover::Request)
    );
    assert!(matches!(one?, Acquisition::Contending(_)));
    assert!(matches!(two?, Acquisition::Contending(_)));
    database.release(&leader).await?;
    assert_eq!(
        database
            .acquire(&b, ts(3), ts(0), ts(100), Takeover::Observe)
            .await?,
        Acquisition::Reserved
    );
    assert!(matches!(
        database
            .acquire(&c, ts(3), ts(0), ts(100), Takeover::Observe)
            .await?,
        Acquisition::Acquired(_)
    ));
    Ok(())
}

async fn launched_registration_order<D: LeaderLeasePort>(database: &D) -> TestResult {
    let old_host = ControllerHostIdentity::Launched {
        at: ts(100),
        instance: InstanceId::try_new("instance-z")?,
    };
    let tie_host = ControllerHostIdentity::Launched {
        at: ts(100),
        instance: InstanceId::try_new("instance-zz")?,
    };
    let new_host = ControllerHostIdentity::Launched {
        at: ts(101),
        instance: InstanceId::try_new("instance-a")?,
    };
    // Deliberately register in reverse launch order.
    let new = database
        .register_candidate(&ControllerId::new("launched-new"), &new_host)
        .await?;
    let tie = database
        .register_candidate(&ControllerId::new("launched-tie"), &tie_host)
        .await?;
    let old = database
        .register_candidate(&ControllerId::new("launched-old"), &old_host)
        .await?;
    assert_eq!(
        new.priority,
        CandidatePriority::launched(ts(101), &InstanceId::try_new("instance-a")?)
    );
    assert!(old.priority < tie.priority);
    assert!(tie.priority < new.priority);
    let restart = database
        .register_candidate(&ControllerId::new("launched-old-restarted"), &old_host)
        .await?;
    assert_eq!(restart.priority, old.priority);
    restart_cannot_reverse_handoff(database, &old_host, &tie_host).await?;
    restart_cannot_reverse_handoff(database, &old_host, &new_host).await
}

async fn restart_cannot_reverse_handoff<D: LeaderLeasePort>(
    database: &D,
    old_host: &ControllerHostIdentity,
    new_host: &ControllerHostIdentity,
) -> TestResult {
    let old = database
        .register_candidate(&ControllerId::new("old-process"), old_host)
        .await?;
    let replacement = database
        .register_candidate(&ControllerId::new("replacement-process"), new_host)
        .await?;
    assert!(old.priority < replacement.priority);
    let Acquisition::Acquired(original) = database
        .acquire(&old, ts(1), ts(0), ts(100), Takeover::Observe)
        .await?
    else {
        return Err("old host did not acquire the free lease".into());
    };
    assert_eq!(
        database
            .acquire(&replacement, ts(2), ts(0), ts(90), Takeover::Request)
            .await?,
        Acquisition::Contending(original.clone())
    );
    assert_eq!(
        database.renew(&original, ts(3), ts(110)).await?,
        Renewal::YieldRequested(Lease {
            expires_at: ts(110),
            ..original.clone()
        })
    );
    database.release(&original).await?;
    let Acquisition::Acquired(mut leader) = database
        .acquire(&replacement, ts(4), ts(0), ts(120), Takeover::Observe)
        .await?
    else {
        return Err("replacement did not acquire its reserved lease".into());
    };
    let restarted = database
        .register_candidate(&ControllerId::new("old-process-restarted"), old_host)
        .await?;
    assert_eq!(restarted.priority, old.priority);
    let same_host = database
        .register_candidate(&ControllerId::new("replacement-peer"), new_host)
        .await?;
    assert_eq!(same_host.priority, replacement.priority);
    for now in [5, 6, 7] {
        for contender in [&restarted, &old, &same_host] {
            for takeover in [Takeover::Observe, Takeover::Request] {
                assert_eq!(
                    database
                        .acquire(contender, ts(now), ts(0), ts(90), takeover)
                        .await?,
                    Acquisition::Held(leader.clone())
                );
            }
        }
        let renewed = Lease {
            expires_at: ts(120 + now),
            ..leader.clone()
        };
        assert_eq!(
            database.renew(&leader, ts(now), renewed.expires_at).await?,
            Renewal::Renewed(renewed.clone())
        );
        leader = renewed;
    }
    database.release(&leader).await?;
    let Acquisition::Acquired(reacquired) = database
        .acquire(&restarted, ts(8), ts(0), ts(130), Takeover::Observe)
        .await?
    else {
        return Err("an old host could not acquire the free lease".into());
    };
    database.release(&reacquired).await?;
    Ok(())
}
