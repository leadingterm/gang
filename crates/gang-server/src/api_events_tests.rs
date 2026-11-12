//! `GET /v1/events`: the scaling ledger, newest first, in pages.

use gang_spi::{ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, StateRepositoryPort};
use gang_testkit::{Controller, FakeRepository, TestResult, worker_id};
use gang_types::{AccessToken, FleetName, InstanceId, Permission, ScalingCause, WorkerId};
use gang_wire::routes::LIST_EVENTS;
use gang_wire::{Cause, ErrorReason, Event, EventChange, ListEventsResponse, name};
use serde_json::json;

use crate::api_test_support::{
    ADMIN_PROOF, ALICE, ALICE_PROOF, PROVIDER, get, metadata, revoke_principal, send,
    settings_with_alice, violated_fields,
};
use crate::test_support::{
    ApiReply as Answer, api_request, at, mint_token, serve, tls_parts, tls_settings,
};

const FLEET: &str = "gang-x86";

fn fleet() -> FleetName {
    FleetName::try_new(FLEET).expect("fixture fleet name is valid")
}

fn instance() -> InstanceId {
    InstanceId::try_new("local-1").expect("fixture instance name is valid")
}

/// A ledger event of `kind` in the test fleet at `millis`, caused by `cause`.
fn ledger_event(
    millis: u64,
    kind: ScalingEventKind,
    cause: Option<ScalingCause>,
) -> ScalingAuditEvent {
    ScalingAuditEvent {
        at: at(millis),
        fleet: fleet(),
        kind,
        cause,
    }
}

/// A scale-out ledger event at `millis` with no cause.
fn scaled_out(millis: u64) -> ScalingAuditEvent {
    ledger_event(millis, ScalingEventKind::ScaledOut { from: 0, to: 1 }, None)
}

/// Records `events` in order; the first gets sequence number 1.
async fn record_scaling(repository: &FakeRepository, events: &[ScalingAuditEvent]) {
    for (seq, event) in (1..).zip(events) {
        let recorded = repository
            .record_scaling_event(event)
            .await
            .expect("the fixture scaling event is recorded");
        assert_eq!(recorded, ScalingEventSeq::new(seq));
    }
}

/// The wire event with sequence number `seq` that the ledger event at `millis` becomes.
fn wire_event(seq: u64, millis: u64, change: EventChange, cause: Option<Cause>) -> Event {
    Event {
        name: format!("events/{seq}"),
        create_time: at(millis),
        fleet: name::of::<name::Fleets>(&fleet()),
        cause,
        change,
    }
}

/// One event of each kind, one second apart, oldest first.
fn all_kinds(worker: &WorkerId) -> Vec<ScalingAuditEvent> {
    let kinds = [
        ScalingEventKind::ScaledOut { from: 0, to: 3 },
        ScalingEventKind::DrainStarted {
            worker: worker.clone(),
        },
        ScalingEventKind::DrainCancelled {
            worker: worker.clone(),
        },
        ScalingEventKind::Terminated {
            instance: instance(),
        },
    ];
    kinds
        .into_iter()
        .zip(1..)
        .map(|(kind, second)| ledger_event(second * 1_000, kind, None))
        .collect()
}

/// The wire form of [`all_kinds`], oldest first.
fn all_kinds_on_the_wire(worker: &WorkerId) -> Vec<Event> {
    let worker = name::of::<name::Workers>(worker);
    let changes = [
        EventChange::ScaledOut { from: 0, to: 3 },
        EventChange::DrainStarted {
            worker: worker.clone(),
        },
        EventChange::DrainCancelled { worker },
        EventChange::Terminated {
            instance: name::of::<name::Instances>(&instance()),
        },
    ];
    changes
        .into_iter()
        .zip(1..)
        .map(|(change, seq)| wire_event(seq, seq * 1_000, change, None))
        .collect()
}

/// The events page at `query`, read by `token`.
async fn list(controller: &Controller, token: &AccessToken, query: &str) -> Answer {
    let path = format!("{}?{query}", LIST_EVENTS.template);
    get(controller, &path, Some(token)).await
}

/// The sequence numbers of the events of `response`, in order.
fn seqs_of(response: &ListEventsResponse) -> Vec<u64> {
    response
        .events
        .iter()
        .map(|event| name::parse::<name::Events>(&event.name).expect("the name is an event name"))
        .collect()
}

#[tokio::test]
async fn an_empty_ledger_lists_no_events() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let answer = get(&controller, LIST_EVENTS.template, Some(&admin)).await;

            let response: ListEventsResponse = answer.ok();
            assert!(response.events.is_empty());
            assert!(response.next_page_token.is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn events_are_listed_newest_first_with_their_resource_names() {
    let (parts, _dials, handles) = tls_parts();
    record_scaling(
        &handles.repository,
        &[scaled_out(1_000), scaled_out(2_000), scaled_out(3_000)],
    )
    .await;
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let answer = get(&controller, LIST_EVENTS.template, Some(&admin)).await;

            let response: ListEventsResponse = answer.ok();
            assert_eq!(seqs_of(&response), [3, 2, 1]);
            assert!(
                response.next_page_token.is_none(),
                "one page holds every event"
            );
            assert_eq!(
                answer.value()["events"][2],
                json!({
                    "name": "events/1",
                    "createTime": "1970-01-01T00:00:01.000Z",
                    "fleet": "fleets/gang-x86",
                    "scaledOut": { "from": 0, "to": 1 },
                })
            );
        },
    )
    .await;
}

#[tokio::test]
async fn every_kind_of_event_is_mapped_to_its_oneof_member() -> TestResult {
    let worker = worker_id("local-1")?;
    let (parts, _dials, handles) = tls_parts();
    record_scaling(&handles.repository, &all_kinds(&worker)).await;
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let answer = get(&controller, LIST_EVENTS.template, Some(&admin)).await;

            let response: ListEventsResponse = answer.ok();
            let mut expected = all_kinds_on_the_wire(&worker);
            expected.reverse();
            assert_eq!(response.events, expected);
            let body = answer.value();
            assert_eq!(
                body["events"][2]["drainStarted"],
                json!({ "worker": name::of::<name::Workers>(&worker) })
            );
            assert_eq!(
                body["events"][0]["terminated"],
                json!({ "instance": "instances/local-1" })
            );
        },
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn events_carry_their_cause_in_upper_snake_case_and_omit_it_when_absent() -> TestResult {
    let (parts, _dials, handles) = tls_parts();
    record_scaling(
        &handles.repository,
        &[
            ledger_event(
                1_000,
                ScalingEventKind::ScaledOut { from: 0, to: 1 },
                Some(ScalingCause::IdleTimeout),
            ),
            ledger_event(
                2_000,
                ScalingEventKind::DrainStarted {
                    worker: worker_id("local-1")?,
                },
                None,
            ),
        ],
    )
    .await;
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            let answer = get(&controller, LIST_EVENTS.template, Some(&admin)).await;

            let body = answer.value();
            assert!(body["events"][0].get("cause").is_none());
            assert_eq!(body["events"][1]["cause"], json!("IDLE_TIMEOUT"));
            let response: ListEventsResponse = answer.ok();
            let causes: Vec<Option<Cause>> = response
                .events
                .into_iter()
                .map(|event| event.cause)
                .collect();
            assert_eq!(causes, [None, Some(Cause::IdleTimeout)]);
        },
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn page_tokens_walk_the_ledger_backwards_until_the_last_page() {
    let (parts, _dials, handles) = tls_parts();
    let events: Vec<ScalingAuditEvent> = (1..=5).map(|second| scaled_out(second * 1_000)).collect();
    record_scaling(&handles.repository, &events).await;
    serve(
        parts,
        settings_with_alice("events-reader", Permission::EventsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let first: ListEventsResponse = list(&controller, &alice, "pageSize=2").await.ok();
            assert_eq!(seqs_of(&first), [5, 4]);
            let token = first.next_page_token.expect("events 3 to 1 are left");

            let query = format!("pageSize=2&pageToken={}", token.as_str());
            let second: ListEventsResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(seqs_of(&second), [3, 2]);
            let token = second.next_page_token.expect("event 1 is left");

            let query = format!("pageSize=2&pageToken={}", token.as_str());
            let last: ListEventsResponse = list(&controller, &alice, &query).await.ok();
            assert_eq!(seqs_of(&last), [1]);
            assert!(
                last.next_page_token.is_none(),
                "nothing is older than event 1"
            );

            let exact: ListEventsResponse = list(&controller, &alice, "pageSize=5").await.ok();
            assert_eq!(seqs_of(&exact), [5, 4, 3, 2, 1]);
            assert!(
                exact.next_page_token.is_none(),
                "exactly pageSize events leave nothing for another page"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn the_page_size_defaults_to_one_hundred_and_is_clamped_to_one_thousand() {
    let (parts, _dials, handles) = tls_parts();
    let events: Vec<ScalingAuditEvent> = (1..=1_001).map(scaled_out).collect();
    record_scaling(&handles.repository, &events).await;
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for query in ["pageSize=0", "pageSize=100"] {
                let page: ListEventsResponse = list(&controller, &admin, query).await.ok();
                assert_eq!(page.events.len(), 100, "{query}");
                assert_eq!(seqs_of(&page).first(), Some(&1_001), "{query}");
            }
            let default: ListEventsResponse = get(&controller, LIST_EVENTS.template, Some(&admin))
                .await
                .ok();
            assert_eq!(default.events.len(), 100);
            assert!(default.next_page_token.is_some());

            let clamped: ListEventsResponse = list(&controller, &admin, "pageSize=5000").await.ok();
            let seqs = seqs_of(&clamped);
            assert_eq!(seqs.len(), 1_000);
            assert_eq!(seqs.first(), Some(&1_001));
            assert_eq!(seqs.last(), Some(&2));
            let token = clamped.next_page_token.expect("event 1 is left");

            let query = format!("pageSize=5000&pageToken={}", token.as_str());
            let rest: ListEventsResponse = list(&controller, &admin, &query).await.ok();
            assert_eq!(seqs_of(&rest), [1]);
            assert!(rest.next_page_token.is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn a_bad_page_size_or_page_token_is_refused() {
    let (parts, _dials, handles) = tls_parts();
    record_scaling(&handles.repository, &[scaled_out(1_000)]).await;
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;

            for query in ["pageSize=-1", "pageSize=many", "pageSize=1.5"] {
                let error = list(&controller, &admin, query)
                    .await
                    .refused(ErrorReason::MalformedRequest);
                assert_eq!(violated_fields(&error), ["pageSize"], "{query}");
            }
            for query in ["pageToken=garbage", "pageToken=AAAA"] {
                list(&controller, &admin, query)
                    .await
                    .refused(ErrorReason::InvalidPageToken);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn caller_without_a_role_cannot_list_events() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let error = get(&controller, LIST_EVENTS.template, Some(&alice))
                .await
                .refused(ErrorReason::MissingPermission);

            assert_eq!(metadata(&error, "permission"), "events.read");
        },
    )
    .await;
}

#[tokio::test]
async fn an_events_reader_lists_events_but_cannot_revoke() -> TestResult {
    let (parts, _dials, handles) = tls_parts();
    record_scaling(&handles.repository, &all_kinds(&worker_id("local-1")?)).await;
    serve(
        parts,
        settings_with_alice("events-reader", Permission::EventsRead),
        handles.scheduler,
        |controller| async move {
            let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;

            let response: ListEventsResponse = get(&controller, LIST_EVENTS.template, Some(&alice))
                .await
                .ok();
            assert_eq!(response.events.len(), 4);

            let error = revoke_principal(&controller, &alice, ALICE)
                .await
                .refused(ErrorReason::MissingPermission);
            assert_eq!(metadata(&error, "permission"), "principals.revoke");
        },
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn anonymous_caller_cannot_list_events() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        |controller| async move {
            send(
                &controller,
                api_request("GET", LIST_EVENTS.template, None, None),
            )
            .await
            .refused(ErrorReason::CredentialsMissing);
        },
    )
    .await;
}

#[tokio::test]
async fn ledger_failure_during_a_list_is_an_unavailable_dependency() {
    let (parts, _dials, handles) = tls_parts();
    let repository = handles.repository.clone();
    serve(
        parts,
        tls_settings(),
        handles.scheduler,
        move |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            repository.fail_scaling_reads(true);

            get(&controller, LIST_EVENTS.template, Some(&admin))
                .await
                .refused(ErrorReason::DependencyUnavailable);
        },
    )
    .await;
}
