//! Route matching, the gate's refusals rendered as error envelopes, mounting, and body handling.

use gang_types::{AccessToken, Proof, ProviderName};
use gang_wire::routes::{AUTHENTICATE_SESSION, CREATE_CERTIFICATE, GET_SERVER, LIST_EVENTS};
use gang_wire::{AuthenticateRequest, AuthenticateResponse, Authentication, ErrorReason, Server};
use serde_json::{Value, json};

use crate::api_test_support::{
    ADMIN_PROOF, ALICE, ALICE_PROOF, BODY_LIMIT, PROVIDER, get, metadata, restore_principal,
    revoke_principal, send, violated_fields,
};
use crate::test_support::{
    api_request, mint_token, plain_parts, plain_settings, raw_api_request, serve, tls_parts,
    tls_settings,
};

#[tokio::test]
async fn server_is_open_and_answers_the_token_authentication_and_the_ca() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let answer = get(&controller, GET_SERVER.template, None).await;

            let body = answer.value();
            assert_eq!(body["name"], "server");
            assert!(body["authentication"]["token"]["login"].is_object());
            assert!(body["caCertificate"].is_string());
            let server: Server<Value> = answer.ok();
            assert!(matches!(server.authentication, Authentication::Token(_)));
            assert!(server.ca_certificate.is_some());
        },
    )
    .await;
}

#[tokio::test]
async fn server_under_plaintext_answers_no_authentication() {
    let (parts, _dials, handles) = plain_parts();
    serve(
        parts,
        plain_settings(),
        handles.scheduler,
        |controller| async move {
            let answer = get(&controller, GET_SERVER.template, None).await;

            assert_eq!(answer.value()["authentication"], json!({ "none": {} }));
            let server: Server<Value> = answer.ok();
            assert!(matches!(server.authentication, Authentication::None(_)));
        },
    )
    .await;
}

#[tokio::test]
async fn unknown_path_is_unauthenticated_for_anonymous_callers_and_not_found_for_authenticated_ones()
 {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            get(&controller, "/v1/nope", None)
                .await
                .refused(ErrorReason::CredentialsMissing);

            let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let error = get(&controller, "/v1/nope", Some(&token))
                .await
                .refused(ErrorReason::RouteNotFound);
            assert_eq!(metadata(&error, "method"), "GET");
            assert_eq!(metadata(&error, "path"), "/v1/nope");
        },
    )
    .await;
}

#[tokio::test]
async fn an_authenticated_request_to_an_unknown_path_outside_the_open_paths_is_not_found() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let error = get(&controller, "/healthz", Some(&token))
                .await
                .refused(ErrorReason::RouteNotFound);
            assert_eq!(metadata(&error, "method"), "GET");
            assert_eq!(metadata(&error, "path"), "/healthz");
        },
    )
    .await;
}

#[tokio::test]
async fn wrong_method_on_a_known_path_is_a_missing_route() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let error = get(&controller, AUTHENTICATE_SESSION.template, Some(&token))
                .await
                .refused(ErrorReason::RouteNotFound);
            assert_eq!(metadata(&error, "method"), "GET");
            assert_eq!(metadata(&error, "path"), AUTHENTICATE_SESSION.template);

            let error = send(
                &controller,
                api_request("POST", LIST_EVENTS.template, Some(&token), Some(json!({}))),
            )
            .await
            .refused(ErrorReason::RouteNotFound);
            assert_eq!(metadata(&error, "method"), "POST");
            assert_eq!(metadata(&error, "path"), LIST_EVENTS.template);

            let error = send(
                &controller,
                api_request("DELETE", GET_SERVER.template, Some(&token), None),
            )
            .await
            .refused(ErrorReason::RouteNotFound);
            assert_eq!(metadata(&error, "method"), "DELETE");
        },
    )
    .await;
}

#[tokio::test]
async fn an_unknown_verb_or_a_trailing_slash_is_a_missing_route() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let token = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for path in [
                "/v1/principals/example-idp%3Aalice:disable",
                "/v1/events/",
                "/v1/events/1",
            ] {
                let method = if path.contains(':') { "POST" } else { "GET" };
                send(&controller, api_request(method, path, Some(&token), None))
                    .await
                    .refused(ErrorReason::RouteNotFound);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn a_bad_bearer_token_is_refused_as_invalid_credentials_even_on_open_routes() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let bogus = AccessToken::new("not.a.valid.token");

            for path in [GET_SERVER.template, LIST_EVENTS.template] {
                get(&controller, path, Some(&bogus))
                    .await
                    .refused(ErrorReason::CredentialsInvalid);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn a_revoked_caller_is_refused_with_the_revoked_principal_envelope_on_every_route() {
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

            for path in [GET_SERVER.template, LIST_EVENTS.template, "/v1/nope"] {
                get(&controller, path, Some(&alice))
                    .await
                    .refused(ErrorReason::PrincipalRevoked);
            }

            restore_principal(&controller, &admin, ALICE)
                .await
                .ok::<Value>();
            get(&controller, GET_SERVER.template, Some(&alice))
                .await
                .ok::<Value>();
        },
    )
    .await;
}

#[tokio::test]
async fn authenticate_answers_the_bearer_token_and_its_lifetime() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let request = AuthenticateRequest {
                provider: ProviderName::try_new(PROVIDER).expect("provider name is valid"),
                proof: Proof::new(ALICE_PROOF),
            };
            let answer = send(
                &controller,
                api_request(
                    "POST",
                    AUTHENTICATE_SESSION.template,
                    None,
                    Some(serde_json::to_value(&request).expect("request serializes")),
                ),
            )
            .await;

            let body = answer.value();
            let mut keys: Vec<&str> = body
                .as_object()
                .expect("the body is an object")
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(keys, ["lifetime", "token"]);
            assert_eq!(body["lifetime"], "600s");
            let response: AuthenticateResponse = answer.ok();
            assert!(!response.token.expose().is_empty());
        },
    )
    .await;
}

#[tokio::test]
async fn authenticate_refuses_an_unknown_provider_and_a_rejected_proof() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let unknown = json!({ "provider": "nobody-idp", "proof": ALICE_PROOF });
            let error = send(
                &controller,
                api_request("POST", AUTHENTICATE_SESSION.template, None, Some(unknown)),
            )
            .await
            .refused(ErrorReason::UnknownProvider);
            assert_eq!(metadata(&error, "provider"), "nobody-idp");

            let rejected = json!({ "provider": PROVIDER, "proof": "forged-proof" });
            send(
                &controller,
                api_request("POST", AUTHENTICATE_SESSION.template, None, Some(rejected)),
            )
            .await
            .refused(ErrorReason::ProofRejected);
        },
    )
    .await;
}

#[tokio::test]
async fn authenticate_is_mounted_under_tls_and_not_under_plaintext() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let error = send(
                &controller,
                raw_api_request("POST", AUTHENTICATE_SESSION.template, None, b"{".to_vec()),
            )
            .await
            .refused(ErrorReason::MalformedRequest);
            assert_eq!(violated_fields(&error), ["body"]);
        },
    )
    .await;

    let (parts, _dials, handles) = plain_parts();
    serve(
        parts,
        plain_settings(),
        handles.scheduler,
        |controller| async move {
            for route in [AUTHENTICATE_SESSION, CREATE_CERTIFICATE] {
                let error = send(
                    &controller,
                    api_request("POST", route.template, None, Some(json!({}))),
                )
                .await
                .refused(ErrorReason::RouteNotFound);
                assert_eq!(metadata(&error, "path"), route.template);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn body_over_the_limit_is_refused_as_too_large() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            send(
                &controller,
                raw_api_request(
                    "POST",
                    AUTHENTICATE_SESSION.template,
                    None,
                    vec![b' '; BODY_LIMIT + 1],
                ),
            )
            .await
            .refused(ErrorReason::RequestTooLarge);
        },
    )
    .await;
}

#[tokio::test]
async fn malformed_bodies_are_refused_as_malformed_requests() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let bodies: [&[u8]; 4] = [
                b"{not json",
                b"[]",
                br#"{"provider":"example-idp"}"#,
                br#"{"provider":7,"proof":"alice-proof"}"#,
            ];
            for body in bodies {
                let error = send(
                    &controller,
                    raw_api_request("POST", AUTHENTICATE_SESSION.template, None, body.to_vec()),
                )
                .await
                .refused(ErrorReason::MalformedRequest);
                assert_eq!(violated_fields(&error), ["body"]);
            }
            for (path, body) in [
                (
                    AUTHENTICATE_SESSION.template,
                    json!({"provider": PROVIDER, "proof": ALICE_PROOF, "extra": 1, "other": 2}),
                ),
                (
                    CREATE_CERTIFICATE.template,
                    json!({
                        "provider": "example-workers",
                        "proof": "worker-proof",
                        "pemCsr": "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n",
                        "extra": 1,
                        "other": 2,
                    }),
                ),
            ] {
                let error = send(&controller, api_request("POST", path, None, Some(body)))
                    .await
                    .refused(ErrorReason::MalformedRequest);
                assert_eq!(violated_fields(&error), ["extra", "other"]);
            }
            let error = send(
                &controller,
                raw_api_request(
                    "POST",
                    AUTHENTICATE_SESSION.template,
                    None,
                    br#"{"\u0065xtra":1,"provider":"example-idp","proof":"alice-proof"}"#
                        .to_vec(),
                ),
            )
            .await
            .refused(ErrorReason::MalformedRequest);
            assert_eq!(violated_fields(&error), ["extra"]);
        },
    )
    .await;
}

#[tokio::test]
async fn request_fields_are_lower_camel_case_and_snake_case_is_refused() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let body = json!({
                "provider": "example-workers",
                "proof": "worker-proof",
                "pem_csr": "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n",
            });
            let error = send(
                &controller,
                api_request("POST", CREATE_CERTIFICATE.template, None, Some(body)),
            )
            .await
            .refused(ErrorReason::MalformedRequest);
            assert_eq!(violated_fields(&error), ["pem_csr"]);
        },
    )
    .await;
}
