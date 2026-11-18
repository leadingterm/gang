//! Ingest decoding validation and identity regression tests.

use std::sync::Arc;

use gang_insight::{IngestError, decode};
use gang_wire::{
    IngestBuildsRequest, IngestCall, IngestLoss, IngestOrigin, IngestOutcome, IngestRpc,
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
        }],
        lost: Vec::new(),
        unattributed: 0,
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
