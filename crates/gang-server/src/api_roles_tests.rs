//! `GET /v1/roles`: the roles of `gang.toml`, in name order, in pages.

use gang_testkit::Controller;
use gang_types::{AccessToken, Permission};
use gang_wire::routes::LIST_ROLES;
use gang_wire::{ErrorReason, ListRolesResponse};
use serde_json::json;

use crate::api_test_support::{
    ADMIN, ALICE, ALICE_PROOF, PROVIDER, get, principal_name, settings_with_alice, violated_fields,
};
use crate::test_support::{ApiReply as Answer, mint_token, serve, tls_parts};

/// The roles page at `query`, read by `token`.
async fn list(controller: &Controller, token: &AccessToken, query: &str) -> Answer {
    let path = format!("{}?{query}", LIST_ROLES.template);
    get(controller, &path, Some(token)).await
}

/// The role names of `page`, in page order.
fn names_of(page: &ListRolesResponse) -> Vec<&str> {
    page.roles.iter().map(|role| role.name.as_str()).collect()
}

#[tokio::test]
async fn roles_list_each_role_with_expanded_permissions_and_principal_references() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let answer = get(&controller, LIST_ROLES.template, Some(&alice)).await;

            assert_eq!(
                answer.value(),
                json!({
                    "roles": [
                        {
                            "name": "roles/administrator",
                            "permissions": [
                                "state.read",
                                "principals.read",
                                "events.read",
                                "principals.revoke",
                            ],
                            "principals": [principal_name(ADMIN)],
                        },
                        {
                            "name": "roles/auditor",
                            "permissions": ["principals.read"],
                            "principals": [principal_name(ALICE)],
                        },
                    ],
                })
            );
        },
    )
    .await;
}

#[tokio::test]
async fn roles_page_in_name_order() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let first: ListRolesResponse = list(&controller, &alice, "pageSize=1").await.ok();
            assert_eq!(names_of(&first), ["roles/administrator"]);
            let token = first.next_page_token.expect("the auditor role is left");

            let query = format!("pageSize=1&pageToken={}", token.as_str());
            let last: ListRolesResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(names_of(&last), ["roles/auditor"]);
            assert!(last.next_page_token.is_none());

            let whole: ListRolesResponse = list(&controller, &alice, "pageSize=2").await.ok();
            assert_eq!(names_of(&whole), ["roles/administrator", "roles/auditor"]);
            assert!(whole.next_page_token.is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn roles_page_by_name_when_configured_out_of_order() {
    let (parts, _dials, handles) = tls_parts();
    let mut settings = settings_with_alice("auditor", Permission::PrincipalsRead);
    settings.roles.reverse();
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let first: ListRolesResponse = list(&controller, &alice, "pageSize=1").await.ok();
            assert_eq!(names_of(&first), ["roles/administrator"]);
            let token = first.next_page_token.expect("the auditor role is left");
            let query = format!("pageSize=1&pageToken={}", token.as_str());
            let last: ListRolesResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(names_of(&last), ["roles/auditor"]);
            assert!(last.next_page_token.is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn a_bad_page_size_or_page_token_is_refused() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            for query in ["pageSize=-1", "pageSize=many"] {
                let error = list(&controller, &alice, query)
                    .await
                    .refused(ErrorReason::MalformedRequest);
                assert_eq!(violated_fields(&error), ["pageSize"], "{query}");
            }
            list(&controller, &alice, "pageToken=garbage")
                .await
                .refused(ErrorReason::InvalidPageToken);
        },
    )
    .await;
}
