//! Certificates: creating one from a signing request, and revoking one by serial.

use std::sync::atomic::Ordering;

use gang_clock::Clock;
use gang_spi::CertStatus;
use gang_types::{CertSerial, CertificateRequestPem, Proof, ProviderName};
use gang_wire::routes::{CREATE_CERTIFICATE, GET_SERVER, REVOKE_CERTIFICATE};
use gang_wire::{Certificate, CreateCertificateRequest, ErrorReason, Server, name};
use serde_json::json;

use crate::api_test_support::{
    ADMIN_PROOF, ALICE_PROOF, FAR_FUTURE_MILLIS, PROVIDER, WORKER_PROOF, WORKER_PROVIDER, get,
    metadata, post, send, worker_certificate, worker_identity,
};
use crate::test_support::{
    api_request, at, mint_token, publicly_trusted_settings, serve, signing_request, tls_parts,
    tls_settings,
};

const TARGET_SERIAL: &str = "b1";

/// The resource name of the worker principal under the worker provider.
fn worker_principal_name() -> String {
    name::of::<name::Principals>(&worker_identity())
}

/// The body that asks the worker provider for a certificate for `pem_csr`.
fn create_body(pem_csr: CertificateRequestPem) -> serde_json::Value {
    let request = CreateCertificateRequest {
        provider: ProviderName::try_new(WORKER_PROVIDER).expect("fixture provider is valid"),
        proof: Proof::new(WORKER_PROOF),
        pem_csr,
    };
    serde_json::to_value(request).expect("create request serializes")
}

#[tokio::test]
async fn a_signing_request_is_answered_with_the_certificate_and_recorded_in_the_ledger() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let answer = send(
                &controller,
                api_request(
                    "POST",
                    CREATE_CERTIFICATE.template,
                    None,
                    Some(create_body(signing_request())),
                ),
            )
            .await;

            let certificate: Certificate = answer.ok();
            let server: Server<serde_json::Value> =
                get(&controller, GET_SERVER.template, None).await.ok();
            assert!(certificate.ca_certificate.is_some());
            assert_eq!(certificate.ca_certificate, server.ca_certificate);
            let issued = repository.issued();
            let [record] = issued.as_slice() else {
                panic!("exactly one certificate is recorded");
            };
            assert_eq!(
                certificate.name,
                name::of::<name::Certificates>(&record.serial)
            );
            assert_eq!(certificate.principal, worker_principal_name());
            assert_eq!(certificate.create_time, record.issued_at);
            assert_eq!(certificate.expire_time, record.expires_at);
            assert!(certificate.revoke_time.is_none());
            let pem = certificate
                .pem_certificate
                .expect("creating a certificate answers its PEM");
            assert!(pem.as_str().starts_with("-----BEGIN CERTIFICATE-----"));
        },
    )
    .await;
}

#[tokio::test]
async fn a_publicly_trusted_server_still_answers_the_ca_with_the_certificate() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        publicly_trusted_settings(),
        handles.scheduler,
        |controller| async move {
            let certificate: Certificate = send(
                &controller,
                api_request(
                    "POST",
                    CREATE_CERTIFICATE.template,
                    None,
                    Some(create_body(signing_request())),
                ),
            )
            .await
            .ok();
            let ca = certificate
                .ca_certificate
                .expect("creating a certificate answers the issuing CA");
            assert!(ca.as_str().starts_with("-----BEGIN CERTIFICATE-----"));
        },
    )
    .await;
}

#[tokio::test]
async fn a_malformed_signing_request_is_refused_as_a_malformed_request() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let body = json!({
                "provider": WORKER_PROVIDER,
                "proof": WORKER_PROOF,
                "pemCsr": "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n",
            });

            send(
                &controller,
                api_request("POST", CREATE_CERTIFICATE.template, None, Some(body)),
            )
            .await
            .refused(ErrorReason::MalformedRequest);
        },
    )
    .await;
}

#[tokio::test]
async fn creating_a_certificate_refuses_an_unknown_provider_and_a_rejected_proof() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let mut unknown = create_body(signing_request());
            unknown["provider"] = json!("nobody-idp");
            let error = send(
                &controller,
                api_request("POST", CREATE_CERTIFICATE.template, None, Some(unknown)),
            )
            .await
            .refused(ErrorReason::UnknownProvider);
            assert_eq!(metadata(&error, "provider"), "nobody-idp");

            let mut rejected = create_body(signing_request());
            rejected["proof"] = json!("forged-proof");
            send(
                &controller,
                api_request("POST", CREATE_CERTIFICATE.template, None, Some(rejected)),
            )
            .await
            .refused(ErrorReason::ProofRejected);
        },
    )
    .await;
}

#[tokio::test]
async fn signer_outage_is_answered_as_an_unavailable_dependency() {
    let (parts, _dials, handles) = tls_parts();
    let outage = handles.signer_outage.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            outage.store(true, Ordering::SeqCst);

            send(
                &controller,
                api_request(
                    "POST",
                    CREATE_CERTIFICATE.template,
                    None,
                    Some(create_body(signing_request())),
                ),
            )
            .await
            .refused(ErrorReason::DependencyUnavailable);
        },
    )
    .await;
}

#[tokio::test]
async fn issue_does_not_read_the_certificate_back_from_the_ledger() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    repository.forget_certificates(true);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let answer = send(
                &controller,
                api_request(
                    "POST",
                    CREATE_CERTIFICATE.template,
                    None,
                    Some(create_body(signing_request())),
                ),
            )
            .await;

            let certificate: Certificate = answer.ok();
            let issued = repository.issued();
            let [record] = issued.as_slice() else {
                panic!("the certificate is recorded as issued");
            };
            assert_eq!(
                certificate.name,
                name::of::<name::Certificates>(&record.serial)
            );
            assert_eq!(certificate.create_time, record.issued_at);
            assert_eq!(certificate.expire_time, record.expires_at);
            assert!(certificate.pem_certificate.is_some());
        },
    )
    .await;
}

#[tokio::test]
async fn admin_revokes_a_certificate_by_serial() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    let clock = handles.clock.clone();
    repository.insert(worker_certificate(TARGET_SERIAL));
    repository.forget_certificates(true);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let now = clock.wall();
            let path = REVOKE_CERTIFICATE
                .path(&CertSerial::try_new(TARGET_SERIAL).expect("fixture serial is valid"));

            let answer = post(&controller, &path, &admin).await;

            let certificate: Certificate = answer.ok();
            assert_eq!(certificate.name, format!("certificates/{TARGET_SERIAL}"));
            assert_eq!(certificate.principal, worker_principal_name());
            assert_eq!(certificate.create_time, at(0));
            assert_eq!(certificate.expire_time, at(FAR_FUTURE_MILLIS));
            assert_eq!(certificate.revoke_time, Some(now));
            assert!(certificate.pem_certificate.is_none());
            assert!(certificate.ca_certificate.is_none());
            let target = CertSerial::try_new(TARGET_SERIAL).expect("fixture serial is valid");
            let issued = repository.issued();
            let record = issued.iter().find(|record| record.serial == target);
            assert!(matches!(
                record.map(|record| &record.status),
                Some(CertStatus::Revoked { at }) if *at == now
            ));
        },
    )
    .await;
}

#[tokio::test]
async fn a_created_certificate_is_revoked_by_the_name_it_was_created_with() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let created: Certificate = send(
                &controller,
                api_request(
                    "POST",
                    CREATE_CERTIFICATE.template,
                    None,
                    Some(create_body(signing_request())),
                ),
            )
            .await
            .ok();
            let serial: CertSerial = name::parse::<name::Certificates>(&created.name)
                .expect("the name is a certificate");

            let answer = post(&controller, &REVOKE_CERTIFICATE.path(&serial), &admin).await;

            let revoked: Certificate = answer.ok();
            assert_eq!(revoked.name, created.name);
            assert_eq!(revoked.principal, created.principal);
            assert!(revoked.revoke_time.is_some());
        },
    )
    .await;
}

#[tokio::test]
async fn revoking_an_unknown_serial_is_not_found() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let serial = CertSerial::try_new("ff00").expect("fixture serial is valid");
            let answer = post(&controller, &REVOKE_CERTIFICATE.path(&serial), &admin).await;

            let error = answer.refused(ErrorReason::CertificateNotFound);
            assert_eq!(metadata(&error, "certificate"), "certificates/ff00");
        },
    )
    .await;
}

#[tokio::test]
async fn ledger_failure_during_a_certificate_revoke_is_an_unavailable_dependency() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    repository.insert(worker_certificate(TARGET_SERIAL));
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            repository.fail_revoke(true);

            let serial = CertSerial::try_new(TARGET_SERIAL).expect("fixture serial is valid");
            let answer = post(&controller, &REVOKE_CERTIFICATE.path(&serial), &admin).await;

            answer.refused(ErrorReason::DependencyUnavailable);
        },
    )
    .await;
}

#[tokio::test]
async fn revoking_a_certificate_needs_the_revoke_permission() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    repository.insert(worker_certificate(TARGET_SERIAL));
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let path = REVOKE_CERTIFICATE
                .path(&CertSerial::try_new(TARGET_SERIAL).expect("fixture serial is valid"));

            let error = post(&controller, &path, &alice)
                .await
                .refused(ErrorReason::MissingPermission);
            assert_eq!(metadata(&error, "permission"), "principals.revoke");

            let anonymous = send(&controller, api_request("POST", &path, None, None)).await;
            anonymous.refused(ErrorReason::CredentialsMissing);
            assert!(
                repository
                    .issued()
                    .iter()
                    .all(|record| record.status == CertStatus::Active),
                "no certificate was revoked"
            );
        },
    )
    .await;
}
