//! `GET /v1/session`: the caller's own session.

use gang_clock::Clock;
use gang_types::Permission;
use gang_wire::Session;
use gang_wire::routes::GET_SESSION;
use serde_json::json;

use crate::api_test_support::{
    ALICE, ALICE_PROOF, PROVIDER, get, principal_name, settings_with_alice,
};
use crate::test_support::{mint_token, serve, tls_parts, tls_settings};

#[tokio::test]
async fn an_anonymous_caller_has_a_session_without_a_principal() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let answer = get(&controller, GET_SESSION.template, None).await;

            assert_eq!(
                answer.value(),
                json!({ "name": "session", "permissions": [] })
            );
        },
    )
    .await;
}

#[tokio::test]
async fn a_token_holder_has_its_principal_permissions_and_expiry() {
    let (parts, _dials, handles) = tls_parts();
    let clock = handles.clock.clone();
    serve(
        parts,
        settings_with_alice("state-reader", Permission::StateRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let answer = get(&controller, GET_SESSION.template, Some(&alice)).await;

            let session: Session = answer.ok();
            assert_eq!(session.name, "session");
            assert_eq!(session.principal, Some(principal_name(ALICE)));
            assert_eq!(session.permissions, [gang_wire::Permission::StateRead]);
            assert!(
                session
                    .expire_time
                    .is_some_and(|expire_time| expire_time > clock.wall()),
                "a token expires after it was minted: {:?}",
                session.expire_time
            );
        },
    )
    .await;
}
