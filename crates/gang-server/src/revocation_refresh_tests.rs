//! Revocations written by another controller reach the local admission and TLS sets.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::poll;
use gang_clock::Clock;
use gang_spi::{
    AuthenticationHost, AuthenticationPort, CertStatus, CertificateIssue, CredentialLifecyclePort,
    Evidence, ExpiringRecordPort, IssuedCertRecord, Peer, PortError, RecordId, RecordKey,
    RecordKind, RecordVersion, Rejection, RevocationEpoch, StateRepositoryPort,
};
use gang_testkit::TestResult;
use gang_tls::RevocationSet;
use gang_types::{CertSerial, Identity, NonZeroDuration, Principal, Proof, ProviderName, Url};

use crate::core::Core;
use crate::endpoint::WorkerEndpoint;
use crate::host::Host;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::revocation::Revocations;
use crate::revocation_refresh::{purge_expired_records, refresh_revocations};
use crate::security::Security;
use crate::test_support::{
    PlainDeployment, TlsDeployment, endpoint, plain_parts, signing_request, tls_parts, tls_settings,
};

const INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(1);

fn fixture() -> Arc<Core<PlainDeployment>> {
    let (parts, _, _) = plain_parts();
    let host = Host {
        proxy_protocol: None,
        repository: parts.repository,
        signer: Arc::new(parts.signer),
        security: Security::Plaintext,
        providers: parts.providers,
        clock: parts.clock,
        endpoint: endpoint("https://gang.example.test"),
        revoked_principals: RevokedPrincipals::new(),
        sightings: PrincipalSightings::new(
            Duration::from_mins(1),
            NonZeroUsize::new(10).expect("ten is nonzero"),
        ),
    };
    Core::new(host, parts.clients, parts.workers)
}

fn tls_fixture() -> Arc<Core<TlsDeployment>> {
    let (parts, _, _) = tls_parts();
    let host = Host {
        proxy_protocol: None,
        repository: parts.repository,
        signer: Arc::new(parts.signer),
        security: Security::Plaintext,
        providers: parts.providers,
        clock: parts.clock,
        endpoint: endpoint("https://gang.example.test"),
        revoked_principals: RevokedPrincipals::new(),
        sightings: PrincipalSightings::new(
            Duration::from_mins(1),
            NonZeroUsize::new(10).expect("ten is nonzero"),
        ),
    };
    Core::new(host, parts.clients, parts.workers)
}

fn alice() -> Identity {
    Identity::new(
        ProviderName::try_new("example-idp").expect("provider is valid"),
        Principal::try_new("alice").expect("principal is valid"),
    )
}

#[tokio::test]
async fn periodic_refresh_purges_expired_records() -> TestResult {
    let core = fixture();
    let now = core.host.clock.wall();
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([1; 32]),
    };
    ExpiringRecordPort::insert(
        &core.host.repository,
        &[(key, b"expired")],
        RecordVersion::new(1),
        now,
        now + INTERVAL.get(),
    )
    .await?;
    let mut refresh = Box::pin(purge_expired_records(&core, INTERVAL));
    assert!(poll!(refresh.as_mut()).is_pending());
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert_eq!(core.host.repository.stored_records(), 0);
    Ok(())
}

#[tokio::test]
async fn remote_revoke_and_restore_reach_this_controller() -> TestResult {
    let core = fixture();
    let certificates = RevocationSet::default();
    let serial = CertSerial::try_new("01")?;
    let now = core.host.clock.wall();
    core.host
        .repository
        .record_issued(&IssuedCertRecord {
            serial: serial.clone(),
            identity: alice(),
            issued_at: now,
            expires_at: now + Duration::from_mins(1),
            status: CertStatus::Active,
        })
        .await?;
    let mut refresh = Box::pin(refresh_revocations(
        &core,
        &certificates,
        RevocationEpoch::new(0),
        INTERVAL,
    ));
    assert!(poll!(refresh.as_mut()).is_pending());
    core.host.repository.revoke(&serial, now).await?;
    core.host.repository.revoke_principal(&alice(), now).await?;
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert!(certificates.is_revoked(&serial));
    assert!(matches!(
        core.host
            .verify(&alice().provider, &Proof::new("alice-proof"))
            .await,
        Err(PortError::Rejected {
            reason: Rejection::PrincipalRevoked,
            ..
        })
    ));

    core.host.repository.restore_principal(&alice()).await?;
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert_eq!(
        core.host
            .verify(&alice().provider, &Proof::new("alice-proof"))
            .await?,
        alice().principal
    );
    assert!(certificates.is_revoked(&serial));
    Ok(())
}

#[tokio::test]
async fn unchanged_epoch_does_not_reload_snapshots() -> TestResult {
    let core = fixture();
    let certificates = RevocationSet::default();
    let mut refresh = Box::pin(refresh_revocations(
        &core,
        &certificates,
        RevocationEpoch::new(0),
        INTERVAL,
    ));
    assert!(poll!(refresh.as_mut()).is_pending());
    // A reload of the empty durable snapshot would remove this local entry.
    core.host.revoked_principals.revoke(alice());
    core.host.repository.fail_revoked_reads(true);
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    core.host.repository.fail_revoked_reads(false);
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert!(core.host.revoked_principals.contains(&alice()));
    Ok(())
}

#[tokio::test]
async fn restore_in_flight_is_not_undone_by_a_refresh() -> TestResult {
    let core = fixture();
    let certificates = RevocationSet::default();
    let operations = Revocations::new(
        core.clone(),
        certificates.clone(),
        crate::health::Readiness::for_test(),
    );
    let identity = alice();
    operations.revoke_principal(&identity).await?;
    let gate = Arc::new(tokio::sync::Notify::new());
    core.host.repository.gate_restore(gate.clone());
    let mut restore = Box::pin(operations.restore_principal(&identity));
    assert!(poll!(restore.as_mut()).is_pending());
    let mut revoke = Box::pin(operations.revoke_principal(&identity));
    let completed = match poll!(revoke.as_mut()) {
        std::task::Poll::Ready(result) => Some(result),
        std::task::Poll::Pending => None,
    };
    let mut refresh = Box::pin(refresh_revocations(
        &core,
        &certificates,
        RevocationEpoch::new(0),
        INTERVAL,
    ));
    assert!(poll!(refresh.as_mut()).is_pending());
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    gate.notify_one();
    restore.await?;
    match completed {
        Some(result) => {
            result?;
        }
        None => {
            revoke.await?;
        }
    }
    assert!(core.host.revoked_principals.contains(&identity));
    Ok(())
}

#[tokio::test]
async fn a_successful_certificate_revoke_cuts_off_before_a_later_failure() -> TestResult {
    let core = tls_fixture();
    let certificates = RevocationSet::default();
    let first = CertSerial::try_new("01")?;
    let second = CertSerial::try_new("02")?;
    let now = core.host.clock.wall();
    for serial in [&first, &second] {
        core.host
            .repository
            .record_issued(&IssuedCertRecord {
                serial: serial.clone(),
                identity: alice(),
                issued_at: now,
                expires_at: now + Duration::from_mins(1),
                status: CertStatus::Active,
            })
            .await?;
    }
    core.host.repository.fail_revoke_serial(second.clone());
    let operations = Revocations::new(
        core,
        certificates.clone(),
        crate::health::Readiness::for_test(),
    );
    assert!(operations.revoke_principal(&alice()).await.is_err());
    assert!(certificates.is_revoked(&first));
    assert!(!certificates.is_revoked(&second));
    Ok(())
}

#[tokio::test]
async fn issuance_crossing_revocation_cannot_revive_after_restore() -> TestResult {
    let (parts, _, _) = tls_parts();
    let settings = tls_settings();
    let worker = WorkerEndpoint::try_new(Url::parse("grpcs://worker.example.test:8443")?, true)?;
    let signer = Arc::new(parts.signer);
    let security = Security::establish(
        settings.transport,
        &settings.endpoint,
        &worker,
        signer.clone(),
        parts.clock.clone(),
    )
    .await
    .unwrap_or_else(|error| panic!("the test security establishes: {error}"));
    let host = Host {
        proxy_protocol: None,
        repository: parts.repository,
        signer,
        security,
        providers: parts.providers,
        clock: parts.clock,
        endpoint: endpoint("https://gang.example.test"),
        revoked_principals: RevokedPrincipals::new(),
        sightings: PrincipalSightings::new(
            Duration::from_mins(1),
            NonZeroUsize::new(10).expect("ten is nonzero"),
        ),
    };
    let core = Core::<TlsDeployment>::new(host, parts.clients, parts.workers);
    let provider = ProviderName::try_new("example-workers")?;
    let identity = Identity::new(provider.clone(), Principal::try_new("worker-1")?);
    let proof = Proof::new("worker-proof");
    let csr = signing_request();
    let request = CertificateIssue {
        provider: &provider,
        proof: &proof,
        csr: &csr,
    };
    let gate = Arc::new(tokio::sync::Notify::new());
    core.host.repository.gate_record_issued(gate.clone());
    let mut issue = Box::pin(core.workers.issue_certificate(&core.host, &request));
    assert!(poll!(issue.as_mut()).is_pending());

    let operations = Revocations::new(
        core.clone(),
        RevocationSet::default(),
        crate::health::Readiness::for_test(),
    );
    operations.revoke_principal(&identity).await?;
    gate.notify_one();
    let issued = issue.await;
    operations.restore_principal(&identity).await?;

    match issued {
        Err(PortError::Rejected {
            reason: Rejection::PrincipalRevoked | Rejection::PolicyDenied,
            ..
        }) => {}
        Err(error) => panic!("unexpected issuance failure: {error}"),
        Ok(issued) => {
            let serial = issued.record.serial;
            let peer = Peer {
                certificate: Some(serial.clone()),
                ..Peer::default()
            };
            let headers = http::HeaderMap::new();
            let evidence = Evidence {
                peer: &peer,
                headers: &headers,
                session: None,
            };
            assert!(
                matches!(
                    core.workers.authenticate(&core.host, &evidence).await,
                    Ok(None) | Err(PortError::Rejected { .. })
                ),
                "a certificate issued across revocation must not authenticate after restore"
            );
            let record = core.host.repository.certificate(&serial).await?;
            assert!(
                matches!(
                    record.map(|record| record.status),
                    Some(CertStatus::Revoked { .. })
                ),
                "a certificate issued across revocation must remain revoked"
            );
        }
    }
    Ok(())
}

/// A controller whose worker plugin signs certificates, with a local revoked cache that only
/// `Revocations` or a refresh updates.
async fn issuing_core() -> Arc<Core<TlsDeployment>> {
    let (parts, _, _) = tls_parts();
    let settings = tls_settings();
    let worker = WorkerEndpoint::try_new(
        Url::parse("grpcs://worker.example.test:8443").expect("the worker URL parses"),
        true,
    )
    .expect("the worker endpoint is valid");
    let signer = Arc::new(parts.signer);
    let security = Security::establish(
        settings.transport,
        &settings.endpoint,
        &worker,
        signer.clone(),
        parts.clock.clone(),
    )
    .await
    .unwrap_or_else(|error| panic!("the test security establishes: {error}"));
    let host = Host {
        proxy_protocol: None,
        repository: parts.repository,
        signer,
        security,
        providers: parts.providers,
        clock: parts.clock,
        endpoint: endpoint("https://gang.example.test"),
        revoked_principals: RevokedPrincipals::new(),
        sightings: PrincipalSightings::new(
            Duration::from_mins(1),
            NonZeroUsize::new(10).expect("ten is nonzero"),
        ),
    };
    Core::<TlsDeployment>::new(host, parts.clients, parts.workers)
}

#[tokio::test]
async fn remote_revocation_before_refresh_refuses_issuance() -> TestResult {
    let core = issuing_core().await;
    let provider = ProviderName::try_new("example-workers")?;
    let identity = Identity::new(provider.clone(), Principal::try_new("worker-1")?);
    let proof = Proof::new("worker-proof");
    let csr = signing_request();
    let request = CertificateIssue {
        provider: &provider,
        proof: &proof,
        csr: &csr,
    };
    // Another controller committed the revocation; no refresh has reached this one yet.
    core.host
        .repository
        .revoke_principal(&identity, core.host.clock.wall())
        .await?;

    let issued = core.workers.issue_certificate(&core.host, &request).await;
    assert!(
        matches!(
            issued,
            Err(PortError::Rejected {
                reason: Rejection::PrincipalRevoked,
                ..
            })
        ),
        "a durably revoked principal must not be issued a certificate, got {issued:?}"
    );

    core.host.repository.restore_principal(&identity).await?;
    let records = core.host.repository.certificates_of(&identity).await?;
    assert!(
        records
            .iter()
            .all(|record| record.status != CertStatus::Active),
        "no certificate issued while revoked may be active after restore: {records:?}"
    );
    Ok(())
}

#[tokio::test]
async fn remote_revocation_evicts_worker_certificate_memo() -> TestResult {
    let core = tls_fixture();
    let certificates = RevocationSet::default();
    let serial = CertSerial::try_new("03")?;
    let now = core.host.clock.wall();
    let identity = Identity::new(
        ProviderName::try_new("example-workers")?,
        Principal::try_new("worker")?,
    );
    core.host
        .repository
        .record_issued(&IssuedCertRecord {
            serial: serial.clone(),
            identity,
            issued_at: now,
            expires_at: now + Duration::from_mins(1),
            status: CertStatus::Active,
        })
        .await?;
    let peer = Peer {
        certificate: Some(serial.clone()),
        ..Peer::default()
    };
    let headers = http::HeaderMap::new();
    let evidence = Evidence {
        peer: &peer,
        headers: &headers,
        session: None,
    };
    assert!(
        core.workers
            .authenticate(&core.host, &evidence)
            .await?
            .is_some()
    );
    core.host.repository.revoke(&serial, now).await?;
    let mut refresh = Box::pin(refresh_revocations(
        &core,
        &certificates,
        RevocationEpoch::new(0),
        INTERVAL,
    ));
    assert!(poll!(refresh.as_mut()).is_pending());
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert!(certificates.is_revoked(&serial));
    assert!(matches!(
        core.workers.authenticate(&core.host, &evidence).await,
        Err(PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn failed_snapshot_preserves_both_sets_and_retries() -> TestResult {
    let core = fixture();
    let certificates = RevocationSet::default();
    let serial = CertSerial::try_new("02")?;
    let now = core.host.clock.wall();
    core.host
        .repository
        .record_issued(&IssuedCertRecord {
            serial: serial.clone(),
            identity: alice(),
            issued_at: now,
            expires_at: now + Duration::from_mins(1),
            status: CertStatus::Active,
        })
        .await?;
    let mut refresh = Box::pin(refresh_revocations(
        &core,
        &certificates,
        RevocationEpoch::new(0),
        INTERVAL,
    ));
    assert!(poll!(refresh.as_mut()).is_pending());
    core.host.repository.revoke(&serial, now).await?;
    core.host.repository.revoke_principal(&alice(), now).await?;
    core.host.repository.fail_principals(true);
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert!(!certificates.is_revoked(&serial));
    assert!(!core.host.revoked_principals.contains(&alice()));
    core.host.repository.fail_principals(false);
    core.host.clock.advance(INTERVAL.get());
    assert!(poll!(refresh.as_mut()).is_pending());
    assert!(certificates.is_revoked(&serial));
    assert!(core.host.revoked_principals.contains(&alice()));
    Ok(())
}

#[tokio::test]
async fn handoff_waits_for_complete_revocation_and_refuses_queued_old_mutators() -> TestResult {
    let core = fixture();
    let readiness = crate::health::Readiness::for_test();
    let operations = Revocations::new(core.clone(), RevocationSet::default(), readiness.clone());
    let identity = alice();
    operations.revoke_principal(&identity).await?;
    let gate = Arc::new(tokio::sync::Notify::new());
    core.host.repository.gate_restore(gate.clone());
    let mut restore = Box::pin(operations.restore_principal(&identity));
    assert!(poll!(restore.as_mut()).is_pending());
    let epoch = core.host.repository.revocation_epoch().await?;
    let mut freeze = Box::pin(async {
        let _guard = core.revocation_lock.lock().await;
        readiness.stop();
    });
    assert!(
        poll!(freeze.as_mut()).is_pending(),
        "handoff waits for the old mutation acknowledgement"
    );
    let mut queued = Box::pin(operations.revoke_principal(&identity));
    assert!(poll!(queued.as_mut()).is_pending());
    gate.notify_one();
    assert!(matches!(
        poll!(restore.as_mut()),
        std::task::Poll::Ready(Ok(_))
    ));
    assert!(poll!(freeze.as_mut()).is_ready());
    assert!(matches!(
        poll!(queued.as_mut()),
        std::task::Poll::Ready(Err(PortError::Unavailable(_)))
    ));
    assert_eq!(
        core.host.repository.revocation_epoch().await?,
        epoch,
        "queued old writes cannot commit after freeze"
    );
    assert!(!core.host.revoked_principals.contains(&identity));
    Ok(())
}
