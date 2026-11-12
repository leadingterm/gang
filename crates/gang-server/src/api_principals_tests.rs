//! `GET /v1/principals`: the principal registry, in identity order, in pages.

use gang_clock::Clock;
use gang_spi::StateRepositoryPort;
use gang_testkit::{Controller, FakeRepository};
use gang_types::{AccessToken, Permission, Principal, ProviderName};
use gang_wire::routes::{LIST_PRINCIPALS, LIST_ROLES};
use gang_wire::{ErrorReason, ListPrincipalsResponse, Principal as WirePrincipal, name};
use serde_json::json;

use crate::api_test_support::{
    ALICE, ALICE_PROOF, AWKWARD_PRINCIPALS, BOB, CAROL, PROVIDER, get, metadata, principal_name,
    settings_with_alice, violated_fields,
};
use crate::test_support::{
    ApiReply as Answer, identity, mint_token, serve, tls_parts, tls_settings,
};

/// Records `principals` as seen at the repository's current time.
async fn observe(repository: &FakeRepository, now: gang_clock::Timestamp, principals: &[&str]) {
    for principal in principals {
        repository
            .observe_principal(&identity(principal), now)
            .await
            .expect("principal is recorded");
    }
}

/// The principals page at `query`, read by `token`.
async fn list(controller: &Controller, token: &AccessToken, query: &str) -> Answer {
    let path = format!("{}?{query}", LIST_PRINCIPALS.template);
    get(controller, &path, Some(token)).await
}

/// The resource names of the principals of `page`, in page order.
fn names_of(page: &ListPrincipalsResponse) -> Vec<&str> {
    page.principals
        .iter()
        .map(|principal| principal.name.as_str())
        .collect()
}

#[tokio::test]
async fn principals_are_listed_in_identity_order_with_their_resource_names() {
    let (parts, _dials, handles) = tls_parts();
    let now = handles.clock.wall();
    observe(&handles.repository, now, &[BOB, CAROL]).await;
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let answer = get(&controller, LIST_PRINCIPALS.template, Some(&alice)).await;

            let page: ListPrincipalsResponse = answer.ok();
            assert_eq!(
                names_of(&page),
                [
                    principal_name(ALICE),
                    principal_name(BOB),
                    principal_name(CAROL)
                ]
            );
            assert!(page.next_page_token.is_none());
            let bob: &WirePrincipal = &page.principals[1];
            assert_eq!(
                bob.provider,
                ProviderName::try_new(PROVIDER).expect("provider name is valid")
            );
            assert_eq!(
                bob.principal,
                Principal::try_new(BOB).expect("principal is valid")
            );
            assert_eq!(bob.create_time, now);
            assert_eq!(bob.last_seen_time, now);
            assert!(bob.revoke_time.is_none());
            assert_eq!(
                answer.value()["principals"][1],
                json!({
                    "name": principal_name(BOB),
                    "provider": PROVIDER,
                    "principal": BOB,
                    "createTime": now.to_rfc3339(),
                    "lastSeenTime": now.to_rfc3339(),
                })
            );
        },
    )
    .await;
}

#[tokio::test]
async fn the_revoked_filter_lists_only_revoked_principals_with_their_revoke_time() {
    let (parts, _dials, handles) = tls_parts();
    let now = handles.clock.wall();
    observe(&handles.repository, now, &[BOB, CAROL]).await;
    handles
        .repository
        .revoke_principal(&identity(BOB), now)
        .await
        .expect("principal is revoked");
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let page: ListPrincipalsResponse =
                list(&controller, &alice, "filter=revokeTime:*").await.ok();

            assert_eq!(names_of(&page), [principal_name(BOB)]);
            assert_eq!(page.principals[0].revoke_time, Some(now));
            assert!(page.next_page_token.is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn page_tokens_resume_in_identity_order_until_the_last_page() {
    let (parts, _dials, handles) = tls_parts();
    let now = handles.clock.wall();
    observe(&handles.repository, now, &[BOB, CAROL]).await;
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let first: ListPrincipalsResponse = list(&controller, &alice, "pageSize=1").await.ok();
            assert_eq!(names_of(&first), [principal_name(ALICE)]);
            let token = first.next_page_token.expect("bob and carol are left");

            let query = format!("pageSize=1&pageToken={}", token.as_str());
            let second: ListPrincipalsResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(names_of(&second), [principal_name(BOB)]);
            let token = second.next_page_token.expect("carol is left");

            let query = format!("pageSize=1&pageToken={}", token.as_str());
            let last: ListPrincipalsResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(names_of(&last), [principal_name(CAROL)]);
            assert!(last.next_page_token.is_none());

            let pair: ListPrincipalsResponse = list(&controller, &alice, "pageSize=2").await.ok();
            assert_eq!(
                names_of(&pair),
                [principal_name(ALICE), principal_name(BOB)]
            );
            assert!(pair.next_page_token.is_some());

            let whole: ListPrincipalsResponse = list(&controller, &alice, "pageSize=3").await.ok();
            assert_eq!(whole.principals.len(), 3);
            assert!(whole.next_page_token.is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn a_page_token_is_only_valid_for_the_filter_it_was_issued_for() {
    let (parts, _dials, handles) = tls_parts();
    let now = handles.clock.wall();
    observe(&handles.repository, now, &[BOB, CAROL]).await;
    for principal in [BOB, CAROL] {
        handles
            .repository
            .revoke_principal(&identity(principal), now)
            .await
            .expect("principal is revoked");
    }
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let unfiltered: ListPrincipalsResponse =
                list(&controller, &alice, "pageSize=1").await.ok();
            let unfiltered_token = unfiltered.next_page_token.expect("more principals follow");
            let filtered: ListPrincipalsResponse =
                list(&controller, &alice, "pageSize=1&filter=revokeTime:*")
                    .await
                    .ok();
            let filtered_token = filtered.next_page_token.expect("carol follows bob");

            let query = format!(
                "pageSize=1&filter=revokeTime:*&pageToken={}",
                filtered_token.as_str()
            );
            let next: ListPrincipalsResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(names_of(&next), [principal_name(CAROL)]);

            let query = format!(
                "pageSize=1&filter=revokeTime:*&pageToken={}",
                unfiltered_token.as_str()
            );
            list(&controller, &alice, &query)
                .await
                .refused(ErrorReason::InvalidPageToken);
            let query = format!("pageSize=1&pageToken={}", filtered_token.as_str());
            list(&controller, &alice, &query)
                .await
                .refused(ErrorReason::InvalidPageToken);
        },
    )
    .await;
}

#[tokio::test]
async fn a_bad_filter_page_size_or_page_token_is_refused() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            for query in [
                "filter=state:revoked",
                "filter=revokeTime:",
                "filter=revoked",
            ] {
                let error = list(&controller, &alice, query)
                    .await
                    .refused(ErrorReason::MalformedRequest);
                assert_eq!(violated_fields(&error), ["filter"], "{query}");
            }
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

#[tokio::test]
async fn principals_and_roles_refuse_callers_without_principals_read() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings_with_alice("events-reader", Permission::EventsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            for route in [LIST_PRINCIPALS, LIST_ROLES] {
                let error = get(&controller, route.template, Some(&alice))
                    .await
                    .refused(ErrorReason::MissingPermission);
                assert_eq!(metadata(&error, "permission"), "principals.read");
            }
        },
    )
    .await;
}

#[tokio::test]
async fn anonymous_caller_is_unauthenticated_on_principals_and_roles() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            for route in [LIST_PRINCIPALS, LIST_ROLES] {
                get(&controller, route.template, None)
                    .await
                    .refused(ErrorReason::CredentialsMissing);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn ledger_failure_during_a_principals_list_is_an_unavailable_dependency() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        move |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            repository.fail_principals(true);

            let answer = get(&controller, LIST_PRINCIPALS.template, Some(&alice)).await;

            answer.refused(ErrorReason::DependencyUnavailable);
        },
    )
    .await;
}

/// Follows `nextPageToken` from the first page of `query` to the last, asserting that every token
/// is URL-safe, and returns the resource names in the order the pages gave them.
async fn walk(controller: &Controller, token: &AccessToken, query: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut resume: Option<String> = None;
    loop {
        let full = match &resume {
            None => query.to_owned(),
            Some(page_token) => format!("{query}&pageToken={page_token}"),
        };
        let page: ListPrincipalsResponse = list(controller, token, &full).await.ok();
        names.extend(page.principals.into_iter().map(|principal| principal.name));
        let Some(next) = page.next_page_token else {
            return names;
        };
        assert!(
            next.as_str()
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-_".contains(character)),
            "page token {:?} is URL-safe",
            next.as_str()
        );
        resume = Some(next.as_str().to_owned());
    }
}

/// The resource names of `principals` under the test provider, in identity order.
fn names_in_identity_order(principals: &[&str]) -> Vec<String> {
    let mut identities: Vec<_> = principals
        .iter()
        .map(|principal| identity(principal))
        .collect();
    identities.sort_by_key(ToString::to_string);
    identities
        .iter()
        .map(name::of::<name::Principals>)
        .collect()
}

#[tokio::test]
async fn principals_with_awkward_identities_page_exactly_once_in_identity_order() {
    let (parts, _dials, handles) = tls_parts();
    let now = handles.clock.wall();
    observe(&handles.repository, now, &AWKWARD_PRINCIPALS).await;
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
            let mut everyone = AWKWARD_PRINCIPALS.to_vec();
            everyone.push(ALICE);

            for page_size in [1, 2, 4] {
                let names = walk(&controller, &alice, &format!("pageSize={page_size}")).await;
                assert_eq!(
                    names,
                    names_in_identity_order(&everyone),
                    "pageSize {page_size}"
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn the_revoked_filter_pages_awkward_identities_exactly_once_in_identity_order() {
    let (parts, _dials, handles) = tls_parts();
    let now = handles.clock.wall();
    observe(&handles.repository, now, &AWKWARD_PRINCIPALS).await;
    let revoked = [
        AWKWARD_PRINCIPALS[0],
        AWKWARD_PRINCIPALS[1],
        AWKWARD_PRINCIPALS[3],
    ];
    for principal in revoked {
        handles
            .repository
            .revoke_principal(&identity(principal), now)
            .await
            .expect("principal is revoked");
    }
    serve(
        parts,
        settings_with_alice("auditor", Permission::PrincipalsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let names = walk(&controller, &alice, "pageSize=1&filter=revokeTime:*").await;

            assert_eq!(names, names_in_identity_order(&revoked));
        },
    )
    .await;
}
