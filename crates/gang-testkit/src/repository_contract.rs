//! The contract suite for [`StateRepositoryPort`].
//!
//! Every case takes a fresh repository and checks one behaviour the port promises. The
//! `repository_contract!` macro turns each case into a `#[tokio::test]`, so one suite runs
//! against every backend. The expected values follow the SQLite backend, which is the reference
//! behaviour where backends differ.

use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PortError, PrincipalRecord, RecordedScalingEvent,
    ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, StateRepositoryPort, Subject,
};
use gang_types::{CertSerial, FleetName, Identity, Principal, ProviderName, WorkerId};

use crate::TestResult;

/// The instant `millis` milliseconds after the Unix epoch.
fn ts(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

/// A certificate record for serial `serial`, issued at `issued_at` to `principal` under
/// `provider`, active until one second later.
fn record(
    serial: u8,
    provider: &str,
    principal: &str,
    issued_at: u64,
) -> TestResult<IssuedCertRecord> {
    Ok(IssuedCertRecord {
        serial: CertSerial::from_be_bytes(&[serial])?,
        identity: identity(provider, principal)?,
        issued_at: ts(issued_at),
        expires_at: ts(issued_at + 1_000),
        status: CertStatus::Active,
    })
}

/// The same certificate as `issued`, revoked at `at` milliseconds.
fn revoked_at(issued: &IssuedCertRecord, at: u64) -> IssuedCertRecord {
    IssuedCertRecord {
        status: CertStatus::Revoked { at: ts(at) },
        ..issued.clone()
    }
}

/// The serial whose single byte is `value`.
fn serial_of(value: u8) -> TestResult<CertSerial> {
    Ok(CertSerial::from_be_bytes(&[value])?)
}

/// The identity `principal` under `provider`.
fn identity(provider: &str, principal: &str) -> TestResult<Identity> {
    Ok(Identity::new(
        ProviderName::try_new(provider)?,
        Principal::try_new(principal)?,
    ))
}

/// A registry record for `identity` first and last seen at `seen` milliseconds, revoked at
/// `revoked` milliseconds when given.
fn principal_record(identity: &Identity, seen: u64, revoked: Option<u64>) -> PrincipalRecord {
    PrincipalRecord {
        identity: identity.clone(),
        first_seen: ts(seen),
        last_seen: ts(seen),
        revoked_at: revoked.map(ts),
    }
}

/// A worker-registered scaling event for the fleet `fleet-a`.
fn event(worker: &str) -> TestResult<ScalingAuditEvent> {
    Ok(ScalingAuditEvent {
        at: ts(1),
        fleet: FleetName::try_new("fleet-a")?,
        kind: ScalingEventKind::WorkerRegistered {
            worker: WorkerId::try_new(worker)?,
        },
        cause: None,
    })
}

/// A non-zero limit for a page read.
fn limit(value: usize) -> TestResult<NonZeroUsize> {
    NonZeroUsize::new(value).ok_or_else(|| "limit must not be zero".into())
}

/// A recorded certificate is returned by `certificate` under its serial, and an unknown serial
/// finds nothing.
///
/// # Errors
///
/// Fails when the repository fails or the returned record differs from the one recorded.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn recorded_certificate_is_found_by_serial<R: StateRepositoryPort>(
    repository: &R,
) -> TestResult {
    let issued = record(1, "github", "alice", 10)?;
    repository.record_issued(&issued).await?;
    assert_eq!(repository.certificate(&issued.serial).await?, Some(issued));
    assert_eq!(repository.certificate(&serial_of(2)?).await?, None);
    Ok(())
}

/// A second `record_issued` with a serial already stored is a backend error, and the ledger
/// keeps the first record.
///
/// # Errors
///
/// Fails when the repository fails or accepts the duplicate, or the first record changes.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn duplicate_record_issued_is_a_backend_error_and_keeps_one_record<
    R: StateRepositoryPort,
>(
    repository: &R,
) -> TestResult {
    let first = record(1, "github", "alice", 10)?;
    repository.record_issued(&first).await?;
    let duplicate = record(1, "github", "bob", 20)?;
    let outcome = repository.record_issued(&duplicate).await;
    assert!(
        matches!(outcome, Err(PortError::Backend(_))),
        "expected a backend error, got {outcome:?}"
    );
    assert_eq!(repository.certificate(&first.serial).await?, Some(first));
    Ok(())
}

/// A certificate revoked twice keeps its first revocation time, and revoking an unknown serial
/// is `NotFound`.
///
/// # Errors
///
/// Fails when the repository fails or a revocation time or error differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn revoke_keeps_the_first_revocation_time<R: StateRepositoryPort>(
    repository: &R,
) -> TestResult {
    let issued = record(1, "github", "alice", 10)?;
    repository.record_issued(&issued).await?;
    let first = repository.revoke(&issued.serial, ts(50)).await?;
    assert_eq!(first, revoked_at(&issued, 50));
    assert_eq!(
        repository.certificate(&issued.serial).await?,
        Some(first.clone())
    );
    let second = repository.revoke(&issued.serial, ts(90)).await?;
    assert_eq!(second, first);
    assert_eq!(repository.certificate(&issued.serial).await?, Some(second));
    let unknown = serial_of(9)?;
    assert!(matches!(
        repository.revoke(&unknown, ts(1)).await,
        Err(PortError::NotFound(Subject::Certificate(_)))
    ));
    Ok(())
}

/// `revoked` lists only revoked certificates, ordered by revocation time and then serial.
///
/// # Errors
///
/// Fails when the repository fails or the listing differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn revoked_is_ordered_by_revocation_time_then_serial<R: StateRepositoryPort>(
    repository: &R,
) -> TestResult {
    let first = record(1, "github", "alice", 10)?;
    let second = record(2, "github", "bob", 20)?;
    let third = record(3, "github", "carol", 30)?;
    for issued in [&first, &second, &third] {
        repository.record_issued(issued).await?;
    }
    repository.revoke(&first.serial, ts(2000)).await?;
    repository.revoke(&third.serial, ts(1000)).await?;
    repository.revoke(&second.serial, ts(1000)).await?;
    let expected = vec![
        revoked_at(&second, 1000),
        revoked_at(&third, 1000),
        revoked_at(&first, 2000),
    ];
    assert_eq!(repository.revoked().await?, expected);
    Ok(())
}

/// `certificates_of` lists the active certificates of one principal under one provider, ordered
/// by issue time and then serial. Revoked certificates and other principals or providers are
/// left out.
///
/// # Errors
///
/// Fails when the repository fails or the listing differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn certificates_of_lists_active_records_by_issue_time_then_serial<
    R: StateRepositoryPort,
>(
    repository: &R,
) -> TestResult {
    let late = record(1, "github", "alice", 30)?;
    let same_time_high = record(3, "github", "alice", 10)?;
    let same_time_low = record(2, "github", "alice", 10)?;
    let revoked = record(4, "github", "alice", 20)?;
    let bob = record(5, "github", "bob", 5)?;
    let other_provider = record(6, "gitlab", "alice", 5)?;
    for issued in [
        &late,
        &same_time_high,
        &same_time_low,
        &revoked,
        &bob,
        &other_provider,
    ] {
        repository.record_issued(issued).await?;
    }
    repository.revoke(&revoked.serial, ts(40)).await?;
    let alice = identity("github", "alice")?;
    let expected = vec![same_time_low, same_time_high, late];
    assert_eq!(repository.certificates_of(&alice).await?, expected);
    Ok(())
}

/// Each scaling event gets a sequence number greater than every event recorded before it.
///
/// # Errors
///
/// Fails when the repository fails or a sequence number does not increase.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn scaling_events_get_increasing_sequence_numbers<R: StateRepositoryPort>(
    repository: &R,
) -> TestResult {
    let mut previous = ScalingEventSeq::default();
    for worker in ["w1", "w2", "w3"] {
        let seq = repository.record_scaling_event(&event(worker)?).await?;
        assert!(
            seq > previous,
            "sequence {} does not follow {}",
            seq.get(),
            previous.get()
        );
        previous = seq;
    }
    Ok(())
}

/// `scaling_events` returns the newest events inside the window, oldest first, and reports
/// `more` when events were left out. An empty window leaves the buffer empty.
///
/// # Errors
///
/// Fails when the repository fails or a window, flag or buffer differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn scaling_window_returns_the_newest_events_and_reports_skipped<
    R: StateRepositoryPort,
>(
    repository: &R,
) -> TestResult {
    let mut seqs = Vec::new();
    for worker in ["w1", "w2", "w3", "w4", "w5"] {
        seqs.push(repository.record_scaling_event(&event(worker)?).await?);
    }
    let recorded = |index: usize, worker: &str| -> TestResult<RecordedScalingEvent> {
        Ok(RecordedScalingEvent {
            seq: seqs[index],
            event: event(worker)?,
        })
    };

    let mut into = Vec::new();
    let more = repository
        .scaling_events(ScalingEventSeq::default(), None, limit(2)?, &mut into)
        .await?;
    assert!(more, "events were left out of the newest page");
    assert_eq!(into, vec![recorded(3, "w4")?, recorded(4, "w5")?]);

    let more = repository
        .scaling_events(seqs[0], Some(seqs[4]), limit(2)?, &mut into)
        .await?;
    assert!(more, "w2 was left out of the bounded window");
    assert_eq!(into, vec![recorded(2, "w3")?, recorded(3, "w4")?]);

    let more = repository
        .scaling_events(seqs[0], Some(seqs[4]), limit(10)?, &mut into)
        .await?;
    assert!(!more, "nothing was left out of the bounded window");
    assert_eq!(
        into,
        vec![recorded(1, "w2")?, recorded(2, "w3")?, recorded(3, "w4")?]
    );

    let more = repository
        .scaling_events(seqs[4], None, limit(3)?, &mut into)
        .await?;
    assert!(!more, "an empty window reports nothing skipped");
    assert!(into.is_empty(), "an empty window leaves the buffer empty");
    Ok(())
}

/// The first observation sets `first_seen`; `last_seen` follows the latest observation but never
/// moves backwards.
///
/// # Errors
///
/// Fails when the repository fails or a registry record differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn observe_principal_keeps_first_seen_and_raises_last_seen<R: StateRepositoryPort>(
    repository: &R,
) -> TestResult {
    let alice = identity("github", "alice")?;
    repository.observe_principal(&alice, ts(1000)).await?;
    repository.observe_principal(&alice, ts(500)).await?;
    assert_eq!(
        repository.principal(&alice).await?,
        Some(principal_record(&alice, 1000, None))
    );
    repository.observe_principal(&alice, ts(3000)).await?;
    let expected = PrincipalRecord {
        last_seen: ts(3000),
        ..principal_record(&alice, 1000, None)
    };
    assert_eq!(repository.principal(&alice).await?, Some(expected));
    Ok(())
}

/// Revoking a principal twice keeps the first revocation time, and restoring clears it.
///
/// # Errors
///
/// Fails when the repository fails or a revocation time or registry record differs from the
/// contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn revoke_principal_twice_keeps_the_first_revocation_time_and_restore_clears_it<
    R: StateRepositoryPort,
>(
    repository: &R,
) -> TestResult {
    let alice = identity("github", "alice")?;
    let first = repository.revoke_principal(&alice, ts(1000)).await?;
    assert_eq!(first, principal_record(&alice, 1000, Some(1000)));
    assert_eq!(repository.principal(&alice).await?, Some(first.clone()));
    let second = repository.revoke_principal(&alice, ts(2000)).await?;
    assert_eq!(second, first);
    assert_eq!(repository.principal(&alice).await?, Some(second));
    let restored = repository.restore_principal(&alice).await?;
    assert_eq!(restored, principal_record(&alice, 1000, None));
    assert_eq!(repository.principal(&alice).await?, Some(restored));
    assert!(repository.revoked_principals().await?.is_empty());
    Ok(())
}

/// `principals` pages through the registry in provider and principal order, with the `more`
/// flag and the `only_revoked` filter.
///
/// # Errors
///
/// Fails when the repository fails or a page, flag or buffer differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn principals_page_with_after_and_the_more_flag_and_only_revoked<
    R: StateRepositoryPort,
>(
    repository: &R,
) -> TestResult {
    let a1 = identity("a", "p1")?;
    let a2 = identity("a", "p2")?;
    let a3 = identity("a", "p3")?;
    let b1 = identity("b", "p1")?;
    for who in [&a1, &a2, &a3, &b1] {
        repository.observe_principal(who, ts(1000)).await?;
    }
    repository.revoke_principal(&a2, ts(10)).await?;
    repository.revoke_principal(&b1, ts(20)).await?;

    let mut page = Vec::new();
    let more = repository
        .principals(None, limit(2)?, false, &mut page)
        .await?;
    assert!(more, "records follow the first page");
    assert_eq!(
        page,
        vec![
            principal_record(&a1, 1000, None),
            principal_record(&a2, 1000, Some(10)),
        ]
    );

    let more = repository
        .principals(Some(&a2), limit(2)?, false, &mut page)
        .await?;
    assert!(!more, "the second page is the end of the registry");
    assert_eq!(
        page,
        vec![
            principal_record(&a3, 1000, None),
            principal_record(&b1, 1000, Some(20)),
        ]
    );

    let more = repository
        .principals(Some(&b1), limit(2)?, false, &mut page)
        .await?;
    assert!(!more, "nothing follows the last record");
    assert!(page.is_empty(), "an empty listing leaves the buffer empty");

    let more = repository
        .principals(None, limit(1)?, true, &mut page)
        .await?;
    assert!(more, "a revoked record follows the first revoked one");
    assert_eq!(page, vec![principal_record(&a2, 1000, Some(10))]);

    let more = repository
        .principals(Some(&a2), limit(1)?, true, &mut page)
        .await?;
    assert!(!more, "the last revoked record ends the listing");
    assert_eq!(page, vec![principal_record(&b1, 1000, Some(20))]);
    Ok(())
}

/// `revoked_principals` orders by revocation time, then provider, then principal, and leaves out
/// principals that were never revoked.
///
/// # Errors
///
/// Fails when the repository fails or the listing differs from the contract.
///
/// # Panics
///
/// Panics when the repository breaks the contract this case checks.
pub async fn revoked_principals_are_ordered_by_revocation_time<R: StateRepositoryPort>(
    repository: &R,
) -> TestResult {
    let b1 = identity("b", "p1")?;
    let b2 = identity("b", "p2")?;
    let a1 = identity("a", "p1")?;
    let c1 = identity("c", "p1")?;
    for who in [&b1, &b2, &a1, &c1] {
        repository.observe_principal(who, ts(1000)).await?;
    }
    repository.revoke_principal(&b1, ts(2000)).await?;
    repository.revoke_principal(&b2, ts(1000)).await?;
    repository.revoke_principal(&a1, ts(1000)).await?;
    let expected = vec![
        principal_record(&a1, 1000, Some(1000)),
        principal_record(&b2, 1000, Some(1000)),
        principal_record(&b1, 1000, Some(2000)),
    ];
    assert_eq!(repository.revoked_principals().await?, expected);
    Ok(())
}

/// Runs every contract case against the repository that `$fresh` builds, one `#[tokio::test]`
/// per case. `$fresh` is an expression evaluating to a closure that returns a future of
/// `Result<R, E>`, where `R: StateRepositoryPort` and `E: Into<Box<dyn std::error::Error>>`.
/// Each test calls the closure once, for its own fresh repository.
#[macro_export]
macro_rules! repository_contract {
    (@case $fresh:expr, $case:ident) => {
        #[tokio::test]
        async fn $case() -> $crate::TestResult {
            let repository = ($fresh)()
                .await
                .map_err(Into::<Box<dyn std::error::Error>>::into)?;
            $crate::repository_contract::$case(&repository).await
        }
    };
    ($fresh:expr) => {
        $crate::repository_contract!(@case $fresh, recorded_certificate_is_found_by_serial);
        $crate::repository_contract!(
            @case $fresh,
            duplicate_record_issued_is_a_backend_error_and_keeps_one_record
        );
        $crate::repository_contract!(@case $fresh, revoke_keeps_the_first_revocation_time);
        $crate::repository_contract!(
            @case $fresh,
            revoked_is_ordered_by_revocation_time_then_serial
        );
        $crate::repository_contract!(
            @case $fresh,
            certificates_of_lists_active_records_by_issue_time_then_serial
        );
        $crate::repository_contract!(@case $fresh, scaling_events_get_increasing_sequence_numbers);
        $crate::repository_contract!(
            @case $fresh,
            scaling_window_returns_the_newest_events_and_reports_skipped
        );
        $crate::repository_contract!(
            @case $fresh,
            observe_principal_keeps_first_seen_and_raises_last_seen
        );
        $crate::repository_contract!(
            @case $fresh,
            revoke_principal_twice_keeps_the_first_revocation_time_and_restore_clears_it
        );
        $crate::repository_contract!(
            @case $fresh,
            principals_page_with_after_and_the_more_flag_and_only_revoked
        );
        $crate::repository_contract!(@case $fresh, revoked_principals_are_ordered_by_revocation_time);
    };
}
