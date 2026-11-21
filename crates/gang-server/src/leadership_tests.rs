use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures::{pin_mut, poll};
use gang_clock::{Clock, TestClock, Timestamp};
use gang_config::LeadershipConfig;
use gang_spi::{
    Acquisition, ControllerHostIdentity, ControllerId, FleetSupply, Held, LeaderLeasePort, Lease,
    LeaseCandidate, PortError, ProvisionerPort, RunError, Takeover,
};
use gang_testkit::{FakeProvisioner, FakeRepository, Opaque, TestResult};
use gang_types::{Fleet, FleetName, InstanceId, Isa, Resources};

use crate::leadership::{Leadership, Leading, LeaseEnd};

const RENEW: Duration = Duration::from_secs(3);
const VALID_FOR: Duration = Duration::from_secs(8);

const TERM: Timestamp = Timestamp::from_unix_millis(5_000);

#[tokio::test]
async fn leading_wait_before_set_resolves() {
    let leading = Leading::default();
    let wait = leading.wait();
    pin_mut!(wait);
    assert!(!leading.get());
    assert_eq!(leading.start(), None);
    assert!(poll!(wait.as_mut()).is_pending());
    leading.set(TERM);
    assert!(leading.get());
    assert_eq!(leading.start(), Some(TERM));
    assert!(poll!(wait.as_mut()).is_ready());
}

#[tokio::test]
async fn leading_wait_after_set_resolves_immediately() {
    let leading = Leading::default();
    leading.set(TERM);
    let wait = leading.wait();
    pin_mut!(wait);
    assert!(poll!(wait.as_mut()).is_ready());
}

#[tokio::test]
async fn leading_set_notifies_all_registered_waiters() {
    let leading = Leading::default();
    let cloned = leading.clone();
    let first = leading.wait();
    let second = cloned.wait();
    pin_mut!(first, second);
    assert!(poll!(first.as_mut()).is_pending());
    assert!(poll!(second.as_mut()).is_pending());
    leading.set(TERM);
    assert!(poll!(first.as_mut()).is_ready());
    assert!(poll!(second.as_mut()).is_ready());
}

// A repeated set must not move the term start.
#[tokio::test]
async fn leading_repeated_set_keeps_the_first_start() {
    let leading = Leading::default();
    let cloned = leading.clone();
    let wait = cloned.wait();
    pin_mut!(wait);
    assert!(poll!(wait.as_mut()).is_pending());
    leading.set(TERM);
    leading.set(TERM + Duration::from_mins(1));
    assert!(cloned.get());
    assert_eq!(cloned.start(), Some(TERM));
    assert!(poll!(wait.as_mut()).is_ready());
    let after = cloned.wait();
    pin_mut!(after);
    assert!(poll!(after.as_mut()).is_ready());
}

fn clock() -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(100_000)))
}
fn timing() -> LeadershipConfig {
    LeadershipConfig {
        renew_interval: gang_types::NonZeroDuration::from_secs(3),
        call_timeout: gang_types::NonZeroDuration::from_secs(3),
        ..LeadershipConfig::default()
    }
}


fn candidate(id: &ControllerId) -> LeaseCandidate {
    LeaseCandidate {
        holder: id.clone(),
        priority: gang_spi::CandidatePriority::registered(gang_spi::CandidateGeneration::default()),
    }
}

fn host(id: &str) -> TestResult<ControllerHostIdentity> {
    Ok(ControllerHostIdentity::Persistent(InstanceId::try_new(id)?))
}

async fn acquire<L: LeaderLeasePort>(
    leader: &Leadership<'_, L, TestClock>,
    id: &ControllerId,
) -> Lease {
    leader
        .acquire(&candidate(id), &FakeProvisioner::default())
        .await
        .expect("active test host")
        .expect("active test host acquires")
}

struct StalledLease {
    calls: AtomicUsize,
    repository: FakeRepository,
}

impl LeaderLeasePort for StalledLease {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        self.repository.register_candidate(holder, host).await
    }

    async fn acquire(
        &self,
        _candidate: &LeaseCandidate,
        _now: Timestamp,
        _stale_before: Timestamp,
        _expires_at: Timestamp,
        _takeover: Takeover,
    ) -> Result<Acquisition, PortError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }

    async fn renew(
        &self,
        lease: &gang_spi::Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<gang_spi::Renewal, PortError> {
        self.repository.renew(lease, now, expires_at).await
    }

    async fn release(&self, lease: &gang_spi::Lease) -> Result<(), PortError> {
        self.repository.release(lease).await
    }
}

#[tokio::test]
async fn stalled_acquisition_retries_at_the_renewal_interval() {
    let lease = StalledLease {
        calls: AtomicUsize::new(0),
        repository: FakeRepository::default(),
    };
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&lease, &clock, &id, timing());
    let acquire = acquire(&leader, &id);
    pin_mut!(acquire);
    assert!(poll!(acquire.as_mut()).is_pending());
    assert_eq!(lease.calls.load(Ordering::SeqCst), 1);
    clock.advance(RENEW);
    assert!(poll!(acquire.as_mut()).is_pending());
    assert_eq!(lease.calls.load(Ordering::SeqCst), 2);
}

struct HungRelease(FakeRepository);

impl LeaderLeasePort for HungRelease {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        self.0.register_candidate(holder, host).await
    }

    async fn acquire(
        &self,
        candidate: &LeaseCandidate,
        now: Timestamp,
        stale_before: Timestamp,
        expires_at: Timestamp,
        takeover: Takeover,
    ) -> Result<Acquisition, PortError> {
        self.0
            .acquire(candidate, now, stale_before, expires_at, takeover)
            .await
    }

    async fn renew(
        &self,
        lease: &gang_spi::Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<gang_spi::Renewal, PortError> {
        self.0.renew(lease, now, expires_at).await
    }

    async fn release(&self, _lease: &gang_spi::Lease) -> Result<(), PortError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn hung_release_is_cancelled_at_renewal_interval() {
    let repository = HungRelease(FakeRepository::default());
    let clock = clock();
    let id = ControllerId::new("first");
    let leadership = Leadership::new(&repository, &clock, &id, timing());
    let lease = acquire(&leadership, &id).await;
    let release = leadership.release(&lease);
    pin_mut!(release);
    assert!(poll!(release.as_mut()).is_pending());
    clock.advance(RENEW);
    assert!(poll!(release.as_mut()).is_ready());
}

#[tokio::test]
async fn release_revokes_outstanding_call_before_rpc_completes() {
    let repository = HungRelease(FakeRepository::default());
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let lease = acquire(&leader, &id).await;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut call = Box::pin(leader.fence().run(async |held| {
        tx.send(held.deadline()).expect("receiver exists");
        std::future::pending::<Result<(), std::convert::Infallible>>().await
    }));
    assert!(poll!(call.as_mut()).is_pending());
    let deadline = rx.recv().expect("deadline exported");

    let mut release = Box::pin(leader.release(&lease));
    assert!(poll!(release.as_mut()).is_pending());

    // `call` remains alive: isolates keeper revocation from Held drop.
    assert!(deadline.check().is_err());
    assert!(!leader.fence().holds());
}

struct LostLease {
    repository: FakeRepository,
    releases: AtomicUsize,
}

impl LeaderLeasePort for LostLease {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        self.repository.register_candidate(holder, host).await
    }

    async fn acquire(
        &self,
        candidate: &LeaseCandidate,
        now: Timestamp,
        stale_before: Timestamp,
        expires_at: Timestamp,
        takeover: Takeover,
    ) -> Result<Acquisition, PortError> {
        self.repository
            .acquire(candidate, now, stale_before, expires_at, takeover)
            .await
    }

    async fn renew(
        &self,
        _lease: &gang_spi::Lease,
        _now: Timestamp,
        _expires_at: Timestamp,
    ) -> Result<gang_spi::Renewal, PortError> {
        Ok(gang_spi::Renewal::Lost)
    }

    async fn release(&self, lease: &gang_spi::Lease) -> Result<(), PortError> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.repository.release(lease).await
    }
}

#[tokio::test]
async fn release_waits_for_registered_work_before_releasing_repository_lease() {
    let repository = LostLease {
        repository: FakeRepository::default(),
        releases: AtomicUsize::new(0),
    };
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let lease = acquire(&leader, &id).await;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut call = Box::pin(leader.fence().run(async |held| {
        tx.send(held.deadline()).expect("receiver exists");
        std::future::pending::<Result<(), std::convert::Infallible>>().await
    }));
    assert!(poll!(call.as_mut()).is_pending());
    let deadline = rx.recv().expect("deadline exported");
    let work = deadline.begin_work().expect("lease is valid");

    let mut release = Box::pin(leader.release(&lease));
    assert!(poll!(release.as_mut()).is_pending());
    assert!(deadline.check().is_err());
    assert_eq!(repository.releases.load(Ordering::SeqCst), 0);
    clock.advance(RENEW * 2);
    assert!(poll!(release.as_mut()).is_pending());
    assert_eq!(repository.releases.load(Ordering::SeqCst), 0);

    drop(work);
    assert!(poll!(release.as_mut()).is_ready());
    assert_eq!(repository.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn renewal_loss_revokes_immediately_and_cannot_be_resurrected() {
    let repository = LostLease {
        repository: FakeRepository::default(),
        releases: AtomicUsize::new(0),
    };
    let clock = clock();
    let id = ControllerId::new("first");
    let config = LeadershipConfig {
        call_timeout: gang_types::NonZeroDuration::from_secs(4),
        ..timing()
    };
    let leader = Leadership::new(&repository, &clock, &id, config);
    let mut lease = acquire(&leader, &id).await;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut call = Box::pin(leader.fence().run(async |held| {
        tx.send(held.deadline()).expect("receiver exists");
        std::future::pending::<Result<(), std::convert::Infallible>>().await
    }));
    assert!(poll!(call.as_mut()).is_pending());
    let deadline = rx.recv().expect("deadline exported");
    assert!(deadline.check().is_ok());
    {
        let mut hold = Box::pin(leader.hold(&mut lease));
        assert!(poll!(hold.as_mut()).is_pending());
        clock.advance(RENEW);
        assert!(poll!(hold.as_mut()).is_ready());
    }
    assert!(deadline.check().is_err());
    assert!(!leader.fence().holds());

    // A later confirmation must not restore a keeper that has observed loss.
    repository
        .repository
        .release(&lease)
        .await
        .expect("release succeeds");
    let _reacquired = acquire(&leader, &id).await;
    assert!(!leader.fence().holds());
    assert!(deadline.check().is_err());
}

struct HungProvisioner(FakeProvisioner);

impl ProvisionerPort for HungProvisioner {
    type FleetOptions = Opaque;

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        self.0.supply(fleet, into).await
    }

    async fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.0.acknowledge_exit(held, fleet, instance).await
    }

    async fn set_desired_capacity(
        &self,
        _held: &Held,
        _fleet: Fleet<'_, Opaque>,
        _desired: u32,
    ) -> Result<(), PortError> {
        std::future::pending().await
    }

    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.0.terminate(held, fleet, instance).await
    }

    fn worker_lifetime(&self) -> gang_types::WorkerLifetime {
        self.0.worker_lifetime()
    }
}

struct SlowReadProvisioner {
    inner: FakeProvisioner,
    clock: Arc<TestClock>,
    writes: AtomicUsize,
}

impl ProvisionerPort for SlowReadProvisioner {
    type FleetOptions = Opaque;

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        self.inner.supply(fleet, into).await
    }

    async fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.acknowledge_exit(held, fleet, instance).await
    }

    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        desired: u32,
    ) -> Result<(), PortError> {
        self.clock
            .sleep(VALID_FOR.saturating_sub(Duration::from_millis(5)))
            .await;
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.set_desired_capacity(held, fleet, desired).await
    }

    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.terminate(held, fleet, instance).await
    }

    fn worker_lifetime(&self) -> gang_types::WorkerLifetime {
        self.inner.worker_lifetime()
    }
}

#[tokio::test]
async fn fence_cancels_multi_rpc_mutation_at_call_timeout() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let fence = leader.fence();
    let _lease = acquire(&leader, &id).await;
    let provisioner = SlowReadProvisioner {
        inner: FakeProvisioner::default(),
        clock: Arc::clone(&clock),
        writes: AtomicUsize::new(0),
    };
    let name = FleetName::try_new("workers")?;
    let options = Opaque::default();
    let fleet = Fleet {
        name: &name,
        isa: Isa::X86_64,
        instance: Resources::default(),
        options: &options,
    };
    let provisioner_ref = &provisioner;
    let call = fence.run(async |held| provisioner_ref.set_desired_capacity(held, fleet, 1).await);
    pin_mut!(call);
    assert!(poll!(call.as_mut()).is_pending());
    clock.advance(timing().call_timeout.get());
    assert!(matches!(
        poll!(call.as_mut()),
        Poll::Ready(Err(RunError::Overran(_)))
    ));
    clock.advance(VALID_FOR);
    assert_eq!(provisioner.writes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn hung_provisioner_is_cancelled_at_validity_deadline() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let fence = leader.fence();
    let _lease = acquire(&leader, &id).await;
    let provisioner = HungProvisioner(FakeProvisioner::default());
    let name = FleetName::try_new("workers")?;
    let options = Opaque::default();
    let fleet = Fleet {
        name: &name,
        isa: Isa::X86_64,
        instance: Resources::default(),
        options: &options,
    };
    let call = fence.run(async |held| provisioner.set_desired_capacity(held, fleet, 1).await);
    pin_mut!(call);
    assert!(poll!(call.as_mut()).is_pending());
    clock.advance(VALID_FOR);
    assert!(matches!(
        poll!(call.as_mut()),
        Poll::Ready(Err(RunError::Lapsed(_)))
    ));
    Ok(())
}

#[tokio::test]
async fn acquire_is_immediate_and_validity_excludes_the_deadline() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let validity = leader.fence();
    assert!(!validity.holds());
    let acquire = acquire(&leader, &id);
    pin_mut!(acquire);
    let Poll::Ready(lease) = poll!(acquire.as_mut()) else {
        panic!("free lease must be acquired immediately");
    };
    assert_eq!(lease.holder, id);
    assert_eq!(lease.epoch.get(), 1);
    assert!(validity.holds());
    clock.advance(
        VALID_FOR
            .checked_sub(Duration::from_nanos(1))
            .expect("validity exceeds one nanosecond"),
    );
    assert!(validity.holds());
    clock.advance(Duration::from_nanos(1));
    assert!(!validity.holds());
    Ok(())
}

#[tokio::test]
async fn acquire_waits_until_expiry_is_strictly_older_than_skew() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let first_id = ControllerId::new("first");
    let second_id = ControllerId::new("second");
    let first = Leadership::new(&repository, &clock, &first_id, timing());
    let second = Leadership::new(&repository, &clock, &second_id, timing());
    let lease = acquire(&first, &first_id).await;
    let acquire = acquire(&second, &second_id);
    pin_mut!(acquire);
    assert!(poll!(acquire.as_mut()).is_pending());
    for _ in 0..4 {
        clock.advance(RENEW);
        assert!(poll!(acquire.as_mut()).is_pending());
    }
    // At 12 seconds expiry is exactly two seconds old, so strict staleness fails.
    clock.advance(RENEW);
    let Poll::Ready(next) = poll!(acquire.as_mut()) else {
        panic!("expired lease must be acquired on the next attempt");
    };
    assert_eq!(next.holder, second_id);
    assert_eq!(next.epoch.get(), lease.epoch.get() + 1);
    Ok(())
}

#[tokio::test]
async fn fresh_follower_waits_a_full_ttl_before_taking_an_expired_lease() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let first_id = ControllerId::new("first");
    let second_id = ControllerId::new("second");
    let first = Leadership::new(&repository, &clock, &first_id, timing());
    let second = Leadership::new(&repository, &clock, &second_id, timing());
    let lease = acquire(&first, &first_id).await;
    clock.advance(Duration::from_secs(15));
    let acquire = acquire(&second, &second_id);
    pin_mut!(acquire);
    assert!(poll!(acquire.as_mut()).is_pending());
    for _ in 0..3 {
        clock.advance(RENEW);
        assert!(poll!(acquire.as_mut()).is_pending());
    }
    clock.advance(RENEW);
    let Poll::Ready(next) = poll!(acquire.as_mut()) else {
        panic!("expired lease must be taken after a full reachable ttl");
    };
    assert_eq!(next.holder, second_id);
    assert_eq!(next.epoch.get(), lease.epoch.get() + 1);
    Ok(())
}

#[tokio::test]
async fn hold_renews_on_every_interval_and_extends_validity() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let validity = leader.fence();
    let mut lease = acquire(&leader, &id).await;
    let initial_expiry = lease.expires_at;
    {
        let hold = leader.hold(&mut lease);
        pin_mut!(hold);
        assert!(poll!(hold.as_mut()).is_pending());
        for _ in 0..3 {
            clock.advance(RENEW);
            assert!(poll!(hold.as_mut()).is_pending());
            assert!(validity.holds());
        }
    }
    assert_eq!(lease.expires_at, initial_expiry + RENEW * 3);
    clock.advance(
        VALID_FOR
            .checked_sub(Duration::from_nanos(1))
            .expect("validity exceeds one nanosecond"),
    );
    assert!(validity.holds());
    clock.advance(Duration::from_nanos(1));
    assert!(!validity.holds());
    Ok(())
}

#[tokio::test]
async fn failed_renewals_keep_holding_but_validity_lapses() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let validity = leader.fence();
    let mut lease = acquire(&leader, &id).await;
    let hold = leader.hold(&mut lease);
    pin_mut!(hold);
    assert!(poll!(hold.as_mut()).is_pending());
    clock.advance(RENEW);
    assert!(poll!(hold.as_mut()).is_pending());
    repository.fail_lease(true);
    clock.advance(RENEW);
    assert!(poll!(hold.as_mut()).is_pending());
    clock.advance(Duration::from_secs(2) - Duration::from_nanos(1));
    assert!(validity.holds());
    clock.advance(Duration::from_nanos(1));
    // The successful renewal at +3 remains valid until +11, not +8.
    assert!(validity.holds());
    clock.advance(Duration::from_secs(1));
    assert!(poll!(hold.as_mut()).is_pending());
    clock.advance(Duration::from_secs(2) - Duration::from_nanos(1));
    assert!(validity.holds());
    clock.advance(Duration::from_nanos(1));
    assert!(!validity.holds());
    assert!(poll!(hold.as_mut()).is_pending());
    Ok(())
}

#[tokio::test]
async fn hold_returns_when_another_controller_takes_the_lease() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let first_id = ControllerId::new("first");
    let second_id = ControllerId::new("second");
    let first = Leadership::new(&repository, &clock, &first_id, timing());
    let second = Leadership::new(&repository, &clock, &second_id, timing());
    let mut lease = acquire(&first, &first_id).await;
    let epoch = lease.epoch.get();
    let hold = first.hold(&mut lease);
    pin_mut!(hold);
    assert!(poll!(hold.as_mut()).is_pending());
    repository.fail_lease(true);
    for _ in 0..5 {
        clock.advance(RENEW);
        assert!(poll!(hold.as_mut()).is_pending());
    }
    repository.fail_lease(false);
    let acquire = acquire(&second, &second_id);
    pin_mut!(acquire);
    assert!(poll!(acquire.as_mut()).is_pending());
    for _ in 0..3 {
        clock.advance(RENEW);
        assert!(poll!(acquire.as_mut()).is_pending());
    }
    clock.advance(RENEW);
    let Poll::Ready(next) = poll!(acquire.as_mut()) else {
        panic!("expired lease must be taken after a full reachable ttl");
    };
    assert_eq!(next.holder, second_id);
    assert_eq!(next.epoch.get(), epoch + 1);
    clock.advance(RENEW);
    assert!(poll!(hold.as_mut()).is_ready());
    Ok(())
}

#[tokio::test]
async fn outage_does_not_let_a_follower_steal_a_recovering_leaders_lease() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let first_id = ControllerId::new("first");
    let second_id = ControllerId::new("second");
    let first = Leadership::new(&repository, &clock, &first_id, timing());
    let second = Leadership::new(&repository, &clock, &second_id, timing());
    let mut lease = acquire(&first, &first_id).await;
    let epoch = lease.epoch;
    let hold = first.hold(&mut lease);
    let acquire = acquire(&second, &second_id);
    pin_mut!(hold, acquire);
    assert!(poll!(hold.as_mut()).is_pending());
    assert!(poll!(acquire.as_mut()).is_pending());

    repository.fail_lease(true);
    for _ in 0..10 {
        clock.advance(RENEW);
        assert!(poll!(acquire.as_mut()).is_pending());
        assert!(poll!(hold.as_mut()).is_pending());
    }
    repository.fail_lease(false);
    for _ in 0..5 {
        clock.advance(RENEW);
        // Give the follower the first attempt, even while the old lease is expired.
        assert!(poll!(acquire.as_mut()).is_pending());
        assert!(poll!(hold.as_mut()).is_pending());
        let now = clock.wall();
        let observed = repository
            .acquire(
                &candidate(&second_id),
                now,
                now - Duration::from_secs(2),
                now + Duration::from_secs(10),
                Takeover::Observe,
            )
            .await?;
        let Acquisition::Held(current) = observed else {
            panic!("the recovering leader must retain its lease");
        };
        assert_eq!(current.holder, first_id);
        assert_eq!(current.epoch, epoch);
        assert_eq!(current.expires_at, now + Duration::from_secs(10));
    }
    Ok(())
}

#[tokio::test]
async fn release_allows_immediate_acquisition_at_the_next_epoch() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let first_id = ControllerId::new("first");
    let second_id = ControllerId::new("second");
    let first = Leadership::new(&repository, &clock, &first_id, timing());
    let second = Leadership::new(&repository, &clock, &second_id, timing());
    let lease = acquire(&first, &first_id).await;
    first.release(&lease).await;
    let acquire = acquire(&second, &second_id);
    pin_mut!(acquire);
    let Poll::Ready(next) = poll!(acquire.as_mut()) else {
        panic!("released lease must be acquired immediately");
    };
    assert_eq!(next.holder, second_id);
    assert_eq!(next.epoch.get(), lease.epoch.get() + 1);
    Ok(())
}

#[tokio::test]
async fn fence_admits_changes_only_inside_validity() -> TestResult {
    let repository = FakeRepository::default();
    let clock = clock();
    let id = ControllerId::new("first");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let fence = leader.fence();
    let provisioner = FakeProvisioner::default();
    let name = FleetName::try_new("workers")?;
    let options = Opaque::default();
    let fleet = Fleet {
        name: &name,
        isa: Isa::X86_64,
        instance: Resources::default(),
        options: &options,
    };
    let instance = InstanceId::try_new("worker-1")?;
    let provisioner_ref = &provisioner;
    let instance_ref = &instance;
    assert!(matches!(
        fence
            .run(async |held| provisioner_ref.set_desired_capacity(held, fleet, 3).await)
            .await,
        Err(RunError::Lapsed(_))
    ));
    assert!(matches!(
        fence
            .run(async |held| { provisioner_ref.terminate(held, fleet, instance_ref).await })
            .await,
        Err(RunError::Lapsed(_))
    ));
    assert!(provisioner.desired_requests().is_empty());
    assert!(provisioner.terminations().is_empty());

    let _lease = acquire(&leader, &id).await;
    fence
        .run(async |held| provisioner_ref.set_desired_capacity(held, fleet, 3).await)
        .await?;
    fence
        .run(async |held| provisioner_ref.terminate(held, fleet, instance_ref).await)
        .await?;
    assert_eq!(provisioner.desired_requests(), vec![(name.clone(), 3)]);
    assert_eq!(provisioner.terminations(), vec![instance.clone()]);

    clock.advance(VALID_FOR.saturating_sub(Duration::from_secs(1)));
    assert!(matches!(
        fence
            .run(async |held| provisioner_ref.set_desired_capacity(held, fleet, 4).await)
            .await,
        Err(RunError::Lapsed(_))
    ));
    assert_eq!(provisioner.desired_requests(), vec![(name.clone(), 3)]);
    clock.advance(Duration::from_secs(1));
    assert!(matches!(
        fence
            .run(async |held| provisioner_ref.set_desired_capacity(held, fleet, 4).await)
            .await,
        Err(RunError::Lapsed(_))
    ));
    assert!(matches!(
        fence
            .run(async |held| { provisioner_ref.terminate(held, fleet, instance_ref).await })
            .await,
        Err(RunError::Lapsed(_))
    ));
    assert_eq!(provisioner.desired_requests(), vec![(name.clone(), 3)]);
    assert_eq!(provisioner.terminations(), vec![instance]);
    Ok(())
}

struct ObservedLease {
    repository: FakeRepository,
    attempts: parking_lot::Mutex<Vec<(Timestamp, Takeover)>>,
    reserved: std::sync::atomic::AtomicBool,
}

impl LeaderLeasePort for ObservedLease {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        self.repository.register_candidate(holder, host).await
    }

    async fn acquire(
        &self,
        candidate: &LeaseCandidate,
        now: Timestamp,
        stale_before: Timestamp,
        expires_at: Timestamp,
        takeover: Takeover,
    ) -> Result<Acquisition, PortError> {
        self.attempts.lock().push((now, takeover));
        if self.reserved.load(Ordering::SeqCst) {
            return Ok(Acquisition::Reserved);
        }
        self.repository
            .acquire(candidate, now, stale_before, expires_at, takeover)
            .await
    }

    async fn renew(
        &self,
        lease: &Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<gang_spi::Renewal, PortError> {
        self.repository.renew(lease, now, expires_at).await
    }

    async fn release(&self, lease: &Lease) -> Result<(), PortError> {
        self.repository.release(lease).await
    }
}

fn observed_lease() -> ObservedLease {
    ObservedLease {
        repository: FakeRepository::default(),
        attempts: parking_lot::Mutex::new(Vec::new()),
        reserved: std::sync::atomic::AtomicBool::new(false),
    }
}

#[tokio::test]
async fn newest_candidate_requests_immediately_and_acquires_next_epoch() -> TestResult {
    let repository = observed_lease();
    let clock = clock();
    let old_id = ControllerId::new("old");
    let new_id = ControllerId::new("new");
    let old = Leadership::new(&repository, &clock, &old_id, timing());
    let new = Leadership::new(&repository, &clock, &new_id, timing());
    let old_candidate = old.register_candidate(&host("old")?).await?;
    let new_candidate = new.register_candidate(&host("new")?).await?;
    let platform = FakeProvisioner::default();
    let mut lease = old.acquire(&old_candidate, &platform).await?.ok_or("old retired")?;
    let epoch = lease.epoch;
    {
        let hold = old.hold(&mut lease);
        pin_mut!(hold);
        assert!(poll!(hold.as_mut()).is_pending());
        let next = new.acquire(&new_candidate, &platform);
        pin_mut!(next);
        assert!(poll!(next.as_mut()).is_pending());
        assert_eq!(repository.attempts.lock().last().map(|(_, mode)| *mode), Some(Takeover::Request));
        clock.advance(RENEW);
        assert_eq!(poll!(hold.as_mut()), Poll::Ready(LeaseEnd::YieldRequested));
    }
    assert!(old.fence().holds());
    old.release(&lease).await;
    assert!(!old.fence().holds());
    let next = new.acquire(&new_candidate, &platform).await?.ok_or("new retired")?;
    assert_eq!(next.holder, new_id);
    assert_eq!(next.epoch.get(), epoch.get() + 1);
    Ok(())
}

#[tokio::test]
async fn retiring_candidate_never_acquires_even_after_lease_release() -> TestResult {
    let repository = observed_lease();
    let clock = clock();
    let old_id = ControllerId::new("old");
    let new_id = ControllerId::new("new");
    let old = Leadership::new(&repository, &clock, &old_id, timing());
    let new = Leadership::new(&repository, &clock, &new_id, timing());
    let lease = acquire(&old, &old_id).await;
    let candidate = new.register_candidate(&host("new")?).await?;
    let platform = FakeProvisioner::default();
    let attempt = new.acquire(&candidate, &platform);
    pin_mut!(attempt);
    assert!(poll!(attempt.as_mut()).is_pending());
    let calls = repository.attempts.lock().len();
    platform.retire();
    old.release(&lease).await;
    clock.advance(RENEW);
    assert!(matches!(poll!(attempt.as_mut()), Poll::Ready(Ok(None))));
    assert_eq!(repository.attempts.lock().len(), calls);
    assert!(!new.fence().holds());
    Ok(())
}

#[tokio::test]
async fn reserved_candidate_retries_without_acquiring() -> TestResult {
    let repository = observed_lease();
    let clock = clock();
    let id = ControllerId::new("candidate");
    let leader = Leadership::new(&repository, &clock, &id, timing());
    let candidate = leader.register_candidate(&host("candidate")?).await?;
    let platform = FakeProvisioner::default();
    repository.reserved.store(true, Ordering::SeqCst);
    let attempt = leader.acquire(&candidate, &platform);
    pin_mut!(attempt);
    assert!(poll!(attempt.as_mut()).is_pending());
    for _ in 0..4 {
        clock.advance(RENEW);
        assert!(poll!(attempt.as_mut()).is_pending());
    }
    assert!(!leader.fence().holds());
    repository.reserved.store(false, Ordering::SeqCst);
    clock.advance(RENEW);
    assert!(matches!(poll!(attempt.as_mut()), Poll::Ready(Ok(Some(_)))));
    Ok(())
}

struct RegistrationLease {
    calls: AtomicUsize,
    mode: u8,
}

impl LeaderLeasePort for RegistrationLease {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        _host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            0 if call == 0 => Err(PortError::unavailable("temporary")),
            1 => Err(PortError::backend("permanent")),
            2 => std::future::pending().await,
            _ => Ok(candidate(holder)),
        }
    }

    async fn acquire(
        &self,
        _: &LeaseCandidate,
        _: Timestamp,
        _: Timestamp,
        _: Timestamp,
        _: Takeover,
    ) -> Result<Acquisition, PortError> {
        std::future::pending().await
    }

    async fn renew(
        &self,
        _: &Lease,
        _: Timestamp,
        _: Timestamp,
    ) -> Result<gang_spi::Renewal, PortError> {
        std::future::pending().await
    }

    async fn release(&self, _: &Lease) -> Result<(), PortError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn registration_retries_transient_failure_and_propagates_permanent_failure() -> TestResult {
    let clock = clock();
    let id = ControllerId::new("candidate");
    let lease = RegistrationLease {
        calls: AtomicUsize::new(0),
        mode: 0,
    };
    let leader = Leadership::new(&lease, &clock, &id, timing());
    let identity = host("candidate")?;
    let registration = leader.register_candidate(&identity);
    pin_mut!(registration);
    assert!(poll!(registration.as_mut()).is_pending());
    assert_eq!(lease.calls.load(Ordering::SeqCst), 1);
    clock.advance(RENEW);
    assert!(matches!(poll!(registration.as_mut()), Poll::Ready(Ok(_))));
    assert_eq!(lease.calls.load(Ordering::SeqCst), 2);

    let permanent = RegistrationLease {
        calls: AtomicUsize::new(0),
        mode: 1,
    };
    let leader = Leadership::new(&permanent, &clock, &id, timing());
    assert!(matches!(
        leader.register_candidate(&identity).await,
        Err(PortError::Backend(_))
    ));
    assert_eq!(permanent.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn registration_timeout_retries_and_caller_can_cancel() -> TestResult {
    let clock = clock();
    let id = ControllerId::new("candidate");
    let lease = RegistrationLease {
        calls: AtomicUsize::new(0),
        mode: 2,
    };
    let leader = Leadership::new(&lease, &clock, &id, timing());
    let identity = host("candidate")?;
    {
        let registration = leader.register_candidate(&identity);
        pin_mut!(registration);
        assert!(poll!(registration.as_mut()).is_pending());
        clock.advance(RENEW);
        assert!(poll!(registration.as_mut()).is_pending());
        assert_eq!(lease.calls.load(Ordering::SeqCst), 2);
    }
    clock.advance(RENEW);
    assert_eq!(lease.calls.load(Ordering::SeqCst), 2);
    Ok(())
}
