//! Ingest decoding validation and identity regression tests.

use std::sync::Arc;

use gang_clock::Timestamp;
use gang_insight::{IngestError, decode};
use gang_wire::{
    IngestBuildsRequest, IngestCall, IngestLoss, IngestOrigin, IngestOutcome, IngestRpc,
    IngestTraffic,
};

fn origin() -> IngestOrigin {
    IngestOrigin {
        principal: "example:alice".into(),
        invocation_id: "invocation".into(),
        correlated_invocations_id: None,
        tool: None,
        target: None,
        mnemonic: None,
    }
}

fn request() -> IngestBuildsRequest {
    IngestBuildsRequest {
        origins: vec![origin()],
        calls: vec![IngestCall {
            origin: 0,
            rpc: IngestRpc::Execute,
            outcome: IngestOutcome::Ok,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        }],
        lost: Vec::new(),
        unattributed: 0,
        traffic: None,
    }
}

#[test]
fn unknown_wire_values_deserialize_but_are_rejected() {
    for (rpc, outcome) in [("FUTURE_RPC", "OK"), ("EXECUTE", "FUTURE_OUTCOME")] {
        let json = format!(
            r#"{{"origins":[{{"principal":"example:alice","invocationId":"i"}}],"calls":[{{"origin":0,"rpc":"{rpc}","outcome":"{outcome}"}}]}}"#
        );
        let request: IngestBuildsRequest =
            serde_json::from_str(&json).expect("unknown wire enums deserialize");
        assert!(matches!(decode(request, 1), Err(IngestError::UnknownValue)));
    }
}

#[test]
fn validates_all_origins_even_if_unused() {
    let mut request = request();
    let mut unused = origin();
    unused.principal = "not-an-identity".into();
    request.origins.push(unused);
    assert!(matches!(decode(request, 1), Err(IngestError::Identity(_))));
}

#[test]
fn rejects_empty_invalid_and_oversize_names() {
    for field in 0..5 {
        for bad in [String::new(), "bad\nname".into(), "x".repeat(1025)] {
            let mut request = request();
            let origin = &mut request.origins[0];
            match field {
                0 => origin.invocation_id = bad,
                1 => origin.correlated_invocations_id = Some(bad),
                2 => origin.tool = Some(bad),
                3 => origin.target = Some(bad),
                4 => origin.mnemonic = Some(bad),
                _ => unreachable!(),
            }
            assert!(matches!(decode(request, 1), Err(IngestError::Name(_))));
        }
    }
}

#[test]
fn rejects_bad_indices_and_losses() {
    let mut bad_call = request();
    bad_call.calls[0].origin = 1;
    assert!(matches!(
        decode(bad_call, 1),
        Err(IngestError::Origin { index: 1 })
    ));

    let mut bad_loss = request();
    bad_loss.lost.push(IngestLoss {
        origin: 2,
        count: 1,
    });
    assert!(matches!(
        decode(bad_loss, 1),
        Err(IngestError::Origin { index: 2 })
    ));

    let mut zero = request();
    zero.lost.push(IngestLoss {
        origin: 0,
        count: 0,
    });
    assert!(matches!(decode(zero, 1), Err(IngestError::ZeroLoss)));

    let mut excessive = request();
    excessive.lost.push(IngestLoss {
        origin: 0,
        count: 1,
    });
    assert!(matches!(
        decode(excessive, 0),
        Err(IngestError::TooManyLosses)
    ));
}

#[test]
fn principals_remain_distinct_and_each_origin_shares_its_identity() {
    let mut request = request();
    let mut second = origin();
    second.principal = "example:bob".into();
    request.origins.push(second);
    request.calls.push(IngestCall {
        origin: 1,
        rpc: IngestRpc::GetActionResult,
        outcome: IngestOutcome::NotFound,
        uploaded_bytes: 0,
        downloaded_bytes: 0,
    });
    request.lost.push(IngestLoss {
        origin: 0,
        count: 3,
    });
    request.unattributed = 7;
    let decoded = decode(request, 1).expect("valid request");
    assert_ne!(decoded.calls[0].origin.build, decoded.calls[1].origin.build);
    assert_eq!(
        decoded.calls[0].origin.build.invocation,
        decoded.calls[1].origin.build.invocation
    );
    assert!(Arc::ptr_eq(
        &decoded.calls[0].origin.build.principal,
        &decoded.lost[0].0.principal
    ));
    assert_eq!(decoded.lost[0].1.get(), 3);
    assert_eq!(decoded.unattributed, 7);
}

#[test]
fn legacy_defaults_and_cas_bytes_and_traffic_round_trip() {
    let legacy = r#"{"origins":[{"principal":"example:alice","invocationId":"i"}],"calls":[{"origin":0,"rpc":"CAS","outcome":"OK"}]}"#;
    let decoded = decode(serde_json::from_str(legacy).unwrap(), 1).unwrap();
    assert_eq!(decoded.calls[0].transferred.uploaded, 0);
    assert_eq!(decoded.calls[0].transferred.downloaded, 0);
    assert!(decoded.traffic.is_none());
    let mut request = request();
    request.calls[0].rpc = IngestRpc::Cas;
    request.calls[0].uploaded_bytes = 42;
    request.calls[0].downloaded_bytes = 73;
    let traffic = IngestTraffic {
        start_time: Timestamp::from_unix_millis(1000),
        lookups: 1,
        hits: 1,
        uploaded_bytes: 42,
        downloaded_bytes: 73,
    };
    request.traffic = Some(traffic.clone());
    let encoded = serde_json::to_vec(&request).unwrap();
    let decoded = decode(serde_json::from_slice(&encoded).unwrap(), 1).unwrap();
    assert_eq!(
        (
            decoded.calls[0].transferred.uploaded,
            decoded.calls[0].transferred.downloaded
        ),
        (42, 73)
    );
    assert_eq!(decoded.calls[0].rpc, gang_spi::Rpc::Cas);
    assert_eq!(decoded.traffic, Some(traffic));
    request.calls[0].origin = 99;
    assert!(decode(request, 1).is_err());
}

#[test]
fn rejects_traffic_with_more_hits_than_lookups() {
    let mut request = request();
    request.traffic = Some(IngestTraffic {
        start_time: Timestamp::UNIX_EPOCH,
        lookups: 1,
        hits: 2,
        uploaded_bytes: 0,
        downloaded_bytes: 0,
    });
    assert!(matches!(decode(request, 1), Err(IngestError::ExcessHits)));
}

#[test]
fn rejects_execute_with_transferred_bytes_in_either_direction() {
    for (uploaded, downloaded) in [(1, 0), (0, 1)] {
        let mut request = request();
        request.calls[0].uploaded_bytes = uploaded;
        request.calls[0].downloaded_bytes = downloaded;
        assert!(matches!(
            decode(request, 1),
            Err(IngestError::ExecuteTransferred)
        ));
    }
}

#[test]
fn rejects_unknown_nested_traffic_fields() {
    let json = r#"{"origins":[],"calls":[],"traffic":{"startTime":"1970-01-01T00:00:00.000Z","lookups":0,"hits":0,"uploadedBytes":0,"downloadedBytes":0,"unexpected":1}}"#;
    assert!(serde_json::from_str::<IngestBuildsRequest>(json).is_err());
}
