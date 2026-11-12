//! The pure helpers of the server verdicts: SSE framing, page tokens, dashboard assets, the error
//! envelope and the reissue deadline.

use std::time::{Duration, SystemTime};

use gang_e2e::server_verdict::{
    REISSUE_SLACK, SseBlock, closed_of, hashed_asset, page, refusal, reissue_deadline,
    retry_after_seconds, take_blocks, tampered,
};
use gang_e2e::{Answer, CERTIFICATE_TTL, Leaf, REISSUE_AT_PERCENT};
use gang_wire::{Closed, ClosedReason, ErrorReason, ErrorResponse, Status};

/// An answer with the given status, headers (lower-case names) and body.
fn answer(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Answer {
    Answer {
        status,
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        body: body.to_vec(),
    }
}

/// The error envelope the server sends for `DELETE /v1/state`, which has no route.
fn route_not_found_body() -> Vec<u8> {
    let response = ErrorResponse {
        error: Status::new(
            ErrorReason::RouteNotFound,
            "no such route",
            [
                ("method", "DELETE".to_owned()),
                ("path", "/v1/state".to_owned()),
            ],
        ),
    };
    serde_json::to_vec(&response).unwrap_or_default()
}

#[test]
fn take_blocks_keeps_a_partial_block_for_the_next_chunk() {
    let mut buffer = b"event: snap".to_vec();
    let blocks = take_blocks(&mut buffer);
    assert!(blocks.is_empty(), "a partial block is not yet a block");
    assert_eq!(
        buffer, b"event: snap",
        "the partial block stays in the buffer"
    );

    buffer.extend_from_slice(b"shot\ndata: {}\n\n");
    let blocks = take_blocks(&mut buffer);
    assert_eq!(
        blocks,
        vec![SseBlock {
            event: Some("snapshot".to_owned()),
            data: "{}".to_owned(),
            ..SseBlock::default()
        }]
    );
    assert!(buffer.is_empty(), "a completed block leaves nothing behind");
}

#[test]
fn take_blocks_reads_retry_and_joins_data_lines() {
    let mut buffer = b"retry: 3000\nevent: closed\ndata: a\ndata: b\n\n: comment\n\n".to_vec();
    let blocks = take_blocks(&mut buffer);
    assert_eq!(blocks.len(), 2, "the comment-only block is still a block");
    assert_eq!(
        blocks[0],
        SseBlock {
            event: Some("closed".to_owned()),
            data: "a\nb".to_owned(),
            retry: Some("3000".to_owned()),
            id: None,
        }
    );
    assert_eq!(
        blocks[1],
        SseBlock::default(),
        "a comment-only block has every field empty"
    );
    assert!(buffer.is_empty(), "nothing stays in the buffer");
}

#[test]
fn take_blocks_preserves_partial_tail_after_multiple_blocks() {
    let mut buffer = b"retry: 17\n\nid: abc\n\ndata: incom".to_vec();
    let blocks = take_blocks(&mut buffer);
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].retry.as_deref(), Some("17"));
    assert_eq!(blocks[1].id.as_deref(), Some("abc"));
    assert_eq!(buffer, b"data: incom");

    buffer.extend_from_slice(b"plete\n\n");
    let blocks = take_blocks(&mut buffer);
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].data, "incomplete");
    assert!(buffer.is_empty());
}

#[test]
fn closed_of_reads_the_closed_frame() {
    let block = SseBlock {
        event: Some("closed".to_owned()),
        data: r#"{"reason":"VIEWER_CAP","retryDelay":"3s"}"#.to_owned(),
        ..SseBlock::default()
    };
    assert_eq!(
        closed_of(&block),
        Some(Closed {
            reason: ClosedReason::ViewerCap,
            retry_delay: Some(Duration::from_secs(3)),
        })
    );

    let snapshot = SseBlock {
        event: Some("snapshot".to_owned()),
        data: r#"{"reason":"VIEWER_CAP","retryDelay":"3s"}"#.to_owned(),
        ..SseBlock::default()
    };
    assert_eq!(
        closed_of(&snapshot),
        None,
        "only the closed event is a closed frame"
    );
}

#[test]
fn hashed_asset_takes_the_first_ui_script_or_stylesheet() {
    let html = concat!(
        r#"<link rel="icon" href="/favicon.ico">"#,
        r#"<link rel="stylesheet" href="/ui/assets/main-ab12.css">"#,
        r#"<script src="/ui/assets/main-cd34.js"></script>"#,
    );
    assert_eq!(
        hashed_asset(html),
        Some("/ui/assets/main-ab12.css".to_owned()),
        "the first /ui/ stylesheet or script in document order wins"
    );

    let bare = r#"<link rel="icon" href="/favicon.ico"><p>no assets</p>"#;
    assert_eq!(
        hashed_asset(bare),
        None,
        "a page with no /ui/ asset has none"
    );
}

#[test]
fn tampered_changes_only_the_version_character() {
    assert_eq!(tampered("AQBi"), "BQBi");
    assert_eq!(tampered("BQBi"), "AQBi");
    assert_eq!(
        tampered(""),
        "A",
        "an empty token becomes a token starting with A"
    );
}

#[test]
fn refusal_accepts_the_catalog_envelope_and_rejects_another_reason() {
    let refused = answer(404, &[], &route_not_found_body());
    let metadata = [("method", "DELETE"), ("path", "/v1/state")];
    assert!(
        refusal(&refused, ErrorReason::RouteNotFound, &metadata).is_ok(),
        "the catalog envelope with its metadata is accepted"
    );
    assert!(
        refusal(&refused, ErrorReason::RouteNotFound, &[("method", "GET")]).is_err(),
        "a metadata value that differs is refused"
    );
    assert!(
        refusal(&refused, ErrorReason::CredentialsMissing, &[]).is_err(),
        "a different reason is refused: its status differs"
    );

    let plain = answer(404, &[], br#"{"message":"x"}"#);
    assert!(
        refusal(&plain, ErrorReason::RouteNotFound, &[]).is_err(),
        "a body that is not the error envelope is refused"
    );
}

#[test]
fn retry_after_must_be_whole_seconds_of_at_least_one() {
    let whole = answer(429, &[("retry-after", "3600")], &[]);
    assert_eq!(retry_after_seconds(&whole), Ok(3600));

    let zero = answer(429, &[("retry-after", "0")], &[]);
    assert!(
        retry_after_seconds(&zero).is_err(),
        "zero seconds is not a wait"
    );

    let fractional = answer(429, &[("retry-after", "1.5")], &[]);
    assert!(
        retry_after_seconds(&fractional).is_err(),
        "fractional seconds are refused"
    );

    let absent = answer(429, &[], &[]);
    assert!(
        retry_after_seconds(&absent).is_err(),
        "a missing Retry-After is refused"
    );
}

#[test]
fn page_reads_items_and_the_token() {
    let body = br#"{"principals":[{"a":1},{"a":2}],"nextPageToken":"AQ"}"#;
    let with_token = page(&answer(200, &[], body), "principals").expect("a page reads");
    assert_eq!(
        with_token.items.len(),
        2,
        "both listed principals are items"
    );
    assert_eq!(with_token.next, Some("AQ".to_owned()));

    let no_token = br#"{"principals":[{"a":1}]}"#;
    let last = page(&answer(200, &[], no_token), "principals").expect("a page reads");
    assert_eq!(last.next, None, "a page without a token is the last page");

    let unauthorized = answer(401, &[], body);
    assert!(
        page(&unauthorized, "principals").is_err(),
        "a page that is not a 200 answer is refused"
    );

    let missing = br#"{"events":[]}"#;
    assert!(
        page(&answer(200, &[], missing), "principals").is_err(),
        "a page without the named array is refused"
    );
}

#[test]
fn the_reissue_deadline_follows_the_leaf() {
    let not_before = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let leaf = Leaf {
        serial: vec![1],
        not_before,
        not_after: not_before + CERTIFICATE_TTL,
    };
    let expected = not_before + CERTIFICATE_TTL * REISSUE_AT_PERCENT / 100 + REISSUE_SLACK;
    assert_eq!(reissue_deadline(&leaf), expected);
}
