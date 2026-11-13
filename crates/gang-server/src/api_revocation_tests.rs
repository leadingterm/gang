//! Principals: revoking and restoring one, and what that does to its credentials.

use std::time::Duration;

use gang_clock::Clock;
use gang_spi::CertStatus;
use gang_types::{CertSerial, Principal, ProviderName};
use gang_wire::routes::{CREATE_CERTIFICATE, RESTORE_PRINCIPAL, REVOKE_PRINCIPAL};
use gang_wire::{ErrorReason, Principal as WirePrincipal, RevokePrincipalResponse, name};
use serde_json::{Value, json};

use crate::api_test_support::{
    ADMIN, ADMIN_PROOF, ALICE, ALICE_PROOF, AWKWARD_PRINCIPALS, BOB, BOB_PROOF, PROVIDER, metadata,
    post, principal_name, principal_path, record_of, restore_principal, revoke_principal, send,
    worker_certificate, worker_identity,
};
use crate::test_support::{
    admits, api_request, identity, mint_token, serve, tls_parts, tls_settings,
};

#[tokio::test]
async fn anonymous_caller_is_unauthenticated_on_revoke_and_restore() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            for route in [REVOKE_PRINCIPAL, RESTORE_PRINCIPAL] {
                let path = principal_path(&route, ALICE);
                send(&controller, api_request("POST", &path, None, None))
                    .await
                    .refused(ErrorReason::CredentialsMissing);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn caller_without_a_role_is_forbidden_on_revoke_and_restore() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(BOB_PROOF, BOB);
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let bob = mint_token(&controller, PROVIDER, BOB_PROOF).await;
            revoke_principal(&controller, &admin, ALICE)
                .await
                .ok::<Value>();

            for answer in [
                revoke_principal(&controller, &bob, ALICE).await,
                restore_principal(&controller, &bob, ALICE).await,
                restore_principal(&controller, &bob, "nobody").await,
            ] {
                let error = answer.refused(ErrorReason::MissingPermission);
                assert_eq!(metadata(&error, "permission"), "principals.revoke");
            }
            let record = record_of(&repository, &identity(ALICE));
            assert!(record.is_some_and(|record| record.revoked_at.is_some()));
        },
    )
    .await;
}

#[tokio::test]
async fn admin_revokes_a_principal_and_its_token_is_refused() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            assert!(admits(&controller, &alice).await);
            let now = clock.wall();

            let answer = revoke_principal(&controller, &admin, ALICE).await;

            let response: RevokePrincipalResponse = answer.ok();
            assert_eq!(response.principal.name, principal_name(ALICE));
            assert_eq!(
                response.principal.provider,
                ProviderName::try_new(PROVIDER).expect("provider name is valid")
            );
            assert_eq!(
                response.principal.principal,
                Principal::try_new(ALICE).expect("principal is valid")
            );
            assert_eq!(response.principal.revoke_time, Some(now));
            assert!(response.certificates.is_empty());
            let record = record_of(&repository, &identity(ALICE));
            assert_eq!(record.and_then(|record| record.revoked_at), Some(now));
            assert!(!admits(&controller, &alice).await);
        },
    )
    .await;
}

#[tokio::test]
async fn revoking_a_principal_revokes_its_active_certificates_and_lists_them() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    let clock = handles.clock.clone();
    repository.insert(worker_certificate("b1"));
    repository.forget_certificates(true);
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let now = clock.wall();
            let path = REVOKE_PRINCIPAL.path(&worker_identity());

            let answer = post(&controller, &path, &admin).await;

            let response: RevokePrincipalResponse = answer.ok();
            let [revoked] = response.certificates.as_slice() else {
                panic!("exactly one certificate is revoked");
            };
            assert_eq!(revoked.name, "certificates/b1");
            assert_eq!(revoked.principal, response.principal.name);
            assert_eq!(revoked.revoke_time, Some(now));
            let target = CertSerial::try_new("b1").expect("fixture serial is valid");
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
async fn admin_revoking_itself_refuses_its_next_request() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            assert!(admits(&controller, &admin).await);

            revoke_principal(&controller, &admin, ADMIN)
                .await
                .ok::<Value>();

            assert!(!admits(&controller, &admin).await);
            revoke_principal(&controller, &admin, ADMIN)
                .await
                .refused(ErrorReason::PrincipalRevoked);
        },
    )
    .await;
}

#[tokio::test]
async fn revoking_again_returns_the_original_revocation_time() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let first_time = clock.wall();
            revoke_principal(&controller, &admin, ALICE)
                .await
                .ok::<Value>();
            clock.advance(Duration::from_secs(5));

            let answer = revoke_principal(&controller, &admin, ALICE).await;

            let response: RevokePrincipalResponse = answer.ok();
            assert_eq!(response.principal.revoke_time, Some(first_time));
        },
    )
    .await;
}

#[tokio::test]
async fn idempotent_principal_operations_leave_the_live_epoch_unchanged() {
    use std::sync::Arc;

    use crate::api::{Api, ApiParts};
    use crate::authorizer::Authorizer;
    use crate::gate::Caller;
    use crate::rate_limit::RateLimiter;
    use crate::revocation::Revocations;
    use crate::test_support::{Harness, HarnessOptions, role_for};
    use gang_spi::{ApiService, Authenticated, StateRepositoryPort};
    use gang_tls::RevocationSet;
    use gang_types::PermissionSet;
    use http::StatusCode;

    let harness = Harness::token(HarnessOptions::default());
    let core = harness.core.clone();
    core.host
        .repository
        .observe_principal(&identity(BOB), harness.clock.wall())
        .await
        .expect("bob is recorded");
    let info = serde_json::from_value(json!({
        "name": "server",
        "remoteExecutor": "grpcs://gang.example.com:443",
        "remoteCache": "grpcs://gang.example.com:443",
        "defaultIsa": "x86-64",
        "authentication": {"none": {}},
    }))
    .expect("server info is valid");
    let api = Api::new(ApiParts {
        core: core.clone(),
        revocations: Revocations::new(core.clone(), RevocationSet::from_serials(Vec::new())),
        info,
        authorizer: Authorizer::new(vec![role_for("administrator", PermissionSet::all(), ADMIN)]),
        limits: tls_settings().limits,
        dashboard: None,
        limiter: Arc::new(RateLimiter::new(
            &tls_settings().rate_limit,
            core.host.clock.clone(),
        )),
        login_provider: None,
        device: None,
    });
    let caller = Caller::Authenticated(Authenticated {
        identity: Arc::new(identity(ADMIN)),
        valid_until: None,
    });

    for (route, principal, expected_epoch) in [
        (&REVOKE_PRINCIPAL, ALICE, 1),
        (&REVOKE_PRINCIPAL, ALICE, 1),
        (&RESTORE_PRINCIPAL, BOB, 1),
    ] {
        let mut request = api_request("POST", &principal_path(route, principal), None, None);
        request.extensions_mut().insert(caller.clone());
        let response = api.call(request).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(core.host.revoked_principals.epoch(), expected_epoch);
    }
}

#[tokio::test]
async fn restoring_a_principal_clears_the_revocation_in_the_ledger_and_the_live_set() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            revoke_principal(&controller, &admin, ALICE)
                .await
                .ok::<Value>();
            assert!(!admits(&controller, &alice).await);

            let answer = restore_principal(&controller, &admin, ALICE).await;

            let record = record_of(&repository, &identity(ALICE)).expect("alice is in the ledger");
            assert!(record.revoked_at.is_none());
            assert_eq!(
                answer.value(),
                json!({
                    "name": principal_name(ALICE),
                    "provider": PROVIDER,
                    "principal": ALICE,
                    "createTime": record.first_seen.to_rfc3339(),
                    "lastSeenTime": record.last_seen.to_rfc3339(),
                })
            );
            let restored: WirePrincipal = answer.ok();
            assert!(restored.revoke_time.is_none());
            assert!(admits(&controller, &alice).await);
        },
    )
    .await;
}

#[tokio::test]
async fn restoring_an_unknown_principal_is_not_found() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let answer = restore_principal(&controller, &admin, "nobody").await;

            let error = answer.refused(ErrorReason::PrincipalNotFound);
            assert_eq!(metadata(&error, "principal"), principal_name("nobody"));
        },
    )
    .await;
}

#[tokio::test]
async fn a_principal_id_that_is_not_an_identity_is_a_malformed_request() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for verb in ["revoke", "restore"] {
                let path = format!("/v1/principals/alice:{verb}");
                post(&controller, &path, &admin)
                    .await
                    .refused(ErrorReason::MalformedRequest);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn revoke_takes_an_absent_or_empty_body_and_refuses_any_field() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let path = principal_path(&REVOKE_PRINCIPAL, ALICE);

            post(&controller, &path, &admin).await.ok::<Value>();
            let empty = api_request("POST", &path, Some(&admin), Some(json!({})));
            send(&controller, empty).await.ok::<Value>();

            let field = json!({ "principal": { "provider": PROVIDER, "principal": ALICE } });
            let with_field = api_request("POST", &path, Some(&admin), Some(field));
            send(&controller, with_field)
                .await
                .refused(ErrorReason::MalformedRequest);
        },
    )
    .await;
}

#[tokio::test]
async fn ledger_failure_during_revoke_is_unavailable_and_the_live_set_is_unchanged() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            assert!(admits(&controller, &alice).await);
            repository.fail_principals(true);

            revoke_principal(&controller, &admin, ALICE)
                .await
                .refused(ErrorReason::DependencyUnavailable);

            repository.fail_principals(false);
            assert!(admits(&controller, &alice).await);
            let record = record_of(&repository, &identity(ALICE));
            assert!(record.is_none_or(|record| record.revoked_at.is_none()));
        },
    )
    .await;
}

#[tokio::test]
async fn revoked_principal_is_refused_certificate_creation_at_the_gate() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            revoke_principal(&controller, &admin, ALICE)
                .await
                .ok::<Value>();
            let request = api_request(
                "POST",
                CREATE_CERTIFICATE.template,
                Some(&alice),
                Some(json!({})),
            );

            send(&controller, request)
                .await
                .refused(ErrorReason::PrincipalRevoked);
        },
    )
    .await;
}

#[tokio::test]
async fn first_admission_records_the_principal_first_seen() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    let clock = handles.clock.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let now = clock.wall();

            assert!(admits(&controller, &alice).await);

            let record = record_of(&repository, &identity(ALICE));
            assert_eq!(record.map(|record| record.first_seen), Some(now));
        },
    )
    .await;
}

#[tokio::test]
async fn awkward_identities_are_revoked_and_restored_by_their_encoded_names() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for principal in AWKWARD_PRINCIPALS {
                let resource = name::of::<name::Principals>(&identity(principal));

                let revoked: RevokePrincipalResponse =
                    post(&controller, &format!("/v1/{resource}:revoke"), &admin)
                        .await
                        .ok();
                assert_eq!(revoked.principal.name, resource);
                assert_eq!(
                    revoked.principal.principal,
                    Principal::try_new(principal).expect("principal is valid")
                );
                assert!(revoked.principal.revoke_time.is_some());

                let restored: WirePrincipal =
                    post(&controller, &format!("/v1/{resource}:restore"), &admin)
                        .await
                        .ok();
                assert_eq!(restored.name, resource);
                assert!(restored.revoke_time.is_none());
            }
            let mut recorded: Vec<String> = repository
                .principals()
                .into_iter()
                .map(|record| record.identity.to_string())
                .collect();
            let mut expected: Vec<String> = AWKWARD_PRINCIPALS
                .iter()
                .map(|principal| identity(principal).to_string())
                .chain([identity(ADMIN).to_string()])
                .collect();
            recorded.sort();
            expected.sort();
            assert_eq!(recorded, expected, "each path named exactly one identity");
        },
    )
    .await;
}

#[tokio::test]
async fn a_principal_named_like_an_escape_is_not_decoded_twice() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let literal = name::of::<name::Principals>(&identity("a%41"));
            assert!(
                literal.ends_with("a%2541"),
                "the percent sign is encoded: {literal}"
            );

            post(&controller, &format!("/v1/{literal}:revoke"), &admin)
                .await
                .ok::<Value>();

            assert!(
                record_of(&repository, &identity("a%41"))
                    .is_some_and(|record| record.revoked_at.is_some())
            );
            assert!(record_of(&repository, &identity("aA")).is_none());
            post(
                &controller,
                &format!("/v1/{}:restore", principal_name("aA")),
                &admin,
            )
            .await
            .refused(ErrorReason::PrincipalNotFound);
        },
    )
    .await;
}

#[tokio::test]
async fn a_bad_escape_in_the_principal_id_is_a_malformed_request() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for id in [
                "example-idp%3Azz%zz",
                "example-idp%3Aab%4",
                "example-idp%3Aab%",
            ] {
                for verb in ["revoke", "restore"] {
                    let path = format!("/v1/principals/{id}:{verb}");
                    post(&controller, &path, &admin)
                        .await
                        .refused(ErrorReason::MalformedRequest);
                }
            }
        },
    )
    .await;
}
