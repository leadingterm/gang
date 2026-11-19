//! Pure forwarding and ingest regression tests.

use std::num::NonZeroUsize;
use std::sync::Arc;

use gang_clock::Timestamp;
use gang_insight::{IngestError, Outbox, OutboxError, Outcome, decode};
use gang_spi::{
    Call, CallOutcome, Executed, ExecutionTimes, Observation, Origin, Rpc, Traffic, Transferred,
};
use gang_types::BuildKey;
use gang_wire::{
    IngestBuildsRequest, IngestCall, IngestLoss, IngestOrigin, IngestOutcome, IngestRpc,
    IngestTraffic,
};

fn origin(principal: &str, invocation: &str) -> Result<Origin, Box<dyn std::error::Error>> {
    Ok(Origin {
        build: BuildKey {
            principal: Arc::new(format!("example-idp:{principal}").parse()?),
            invocation: invocation.parse()?,
        },
        correlated: None,
        tool: None,
        target: None,
        mnemonic: None,
    })
}
fn call(origin: &Origin, rpc: Rpc, outcome: CallOutcome) -> Call {
    Call {
        origin: origin.clone(),
        rpc,
        outcome,
        transferred: Transferred::default(),
    }
}
fn outbox(keys: usize, bytes: usize) -> Result<Outbox, Box<dyn std::error::Error>> {
    Ok(Outbox::try_new(
        NonZeroUsize::new(keys).ok_or("fixture key limit must be nonzero")?,
        NonZeroUsize::new(bytes).ok_or("fixture byte cap must be nonzero")?,
    )?)
}
fn frame() -> Result<usize, Box<dyn std::error::Error>> {
    let request = IngestBuildsRequest {
        traffic: Some(IngestTraffic {
            start_time: Timestamp::END_OF_TIME,
            lookups: u64::MAX,
            hits: u64::MAX,
            uploaded_bytes: u64::MAX,
            downloaded_bytes: u64::MAX,
        }),
        ..IngestBuildsRequest::default()
    };
    Ok(serde_json::to_vec(&request)?.len() + 20)
}
fn wire_origin(origin: &Origin) -> IngestOrigin {
    IngestOrigin {
        principal: origin.build.principal.to_string(),
        invocation_id: origin.build.invocation.as_str().to_owned(),
        correlated_invocations_id: origin.correlated.as_ref().map(|v| v.as_str().to_owned()),
        tool: origin.tool.as_ref().map(|v| v.as_str().to_owned()),
        target: origin.target.as_ref().map(|v| v.as_str().to_owned()),
        mnemonic: origin.mnemonic.as_ref().map(|v| v.as_str().to_owned()),
    }
}
fn call_cost(origin: &Origin) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(serde_json::to_vec(&wire_origin(origin))?.len()
        + 1
        + serde_json::to_vec(&IngestCall {
            origin: u32::MAX,
            rpc: IngestRpc::Execute,
            outcome: IngestOutcome::Ok,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        })?
        .len()
        + 1)
}
fn assert_cap(request: &IngestBuildsRequest, cap: usize) -> Result<(), Box<dyn std::error::Error>> {
    assert!(serde_json::to_vec(request)?.len() <= cap);
    Ok(())
}

#[test]
fn frame_and_estimator_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let frame_size = frame()?;
    assert!(
        matches!(Outbox::try_new(NonZeroUsize::new(1).expect("one is nonzero"), NonZeroUsize::new(frame_size - 1).expect("frame size exceeds one")), Err(OutboxError::CapTooSmall { needed }) if needed == frame_size)
    );
    assert!(
        Outbox::try_new(
            NonZeroUsize::new(1).expect("one is nonzero"),
            NonZeroUsize::new(frame_size).expect("frame size is nonzero")
        )
        .is_ok()
    );
    let alice = origin("alice", "inv")?;
    let cap = frame_size + call_cost(&alice)?;
    let observation = call(&alice, Rpc::Execute, CallOutcome::Ok);
    let mut box_ = outbox(1, cap)?;
    box_.push(Observation::Call(observation.clone()));
    let dispatch = box_
        .next_dispatch()
        .expect("queued call produces a dispatch");
    assert_cap(dispatch.request(), cap)?;
    assert_eq!(
        decode(dispatch.request().clone(), 1)
            .expect("dispatched request decodes")
            .calls,
        vec![observation.clone()]
    );
    let mut box_ = outbox(1, cap - 1)?;
    box_.push(Observation::Call(observation));
    let dispatch = box_
        .next_dispatch()
        .expect("oversized call produces a loss dispatch");
    assert_cap(dispatch.request(), cap - 1)?;
    assert!(dispatch.request().calls.is_empty());
    assert_eq!(
        dispatch.request().lost.iter().map(|v| v.count).sum::<u64>()
            + dispatch.request().unattributed,
        1
    );
    Ok(())
}

#[test]
fn roundtrip_dedup_split_and_fifo() -> Result<(), Box<dyn std::error::Error>> {
    let alice = origin("alice", "inv")?;
    let mut metadata = alice.clone();
    metadata.target = Some(
        "//pkg:target"
            .parse()
            .expect("fixture target is a valid name"),
    );
    metadata.tool = Some("bazel 7".parse().expect("fixture tool is a valid name"));
    metadata.correlated = Some(
        "parent"
            .parse()
            .expect("fixture correlation is a valid name"),
    );
    metadata.mnemonic = Some("Compile".parse().expect("fixture mnemonic is a valid name"));
    let mut cas = call(&metadata, Rpc::Cas, CallOutcome::Error);
    cas.transferred = Transferred {
        uploaded: 123,
        downloaded: 456,
    };
    let calls = vec![
        call(&alice, Rpc::GetActionResult, CallOutcome::Ok),
        call(&alice, Rpc::UpdateActionResult, CallOutcome::NotFound),
        call(&metadata, Rpc::Execute, CallOutcome::Error),
        cas,
    ];
    let mut box_ = outbox(4, 8192)?;
    box_.lose(alice.build.clone(), 3);
    for observation in &calls {
        box_.push(Observation::Call(observation.clone()));
    }
    let dispatch = box_
        .next_dispatch()
        .expect("queued calls and loss produce a dispatch");
    assert_eq!(dispatch.request().origins.len(), 2);
    let decoded = decode(dispatch.request().clone(), 4).expect("dispatched request decodes");
    assert_eq!(decoded.calls, calls);
    assert_eq!(decoded.lost[0].0, alice.build);
    assert_eq!(decoded.lost[0].1.get(), 3);
    assert!(box_.is_empty());

    let cap = frame()? + call_cost(&alice)? + 16;
    let mut box_ = outbox(4, cap)?;
    let repeated = call(&alice, Rpc::Execute, CallOutcome::Ok);
    for _ in 0..5 {
        box_.push(Observation::Call(repeated.clone()));
    }
    let mut decoded = Vec::new();
    let mut batches = 0;
    while let Some(dispatch) = box_.next_dispatch() {
        assert_cap(dispatch.request(), cap)?;
        decoded.extend(
            decode(dispatch.request().clone(), 4)
                .expect("split dispatch decodes")
                .calls,
        );
        batches += 1;
    }
    assert!(batches > 1);
    assert_eq!(decoded, vec![repeated; 5]);
    Ok(())
}

#[test]
fn outcomes_keep_only_safe_loss_retries() -> Result<(), Box<dyn std::error::Error>> {
    let o = origin("alice", "inv")?;
    for outcome in [Outcome::Accepted, Outcome::NotApplied, Outcome::Unknown] {
        let mut b = outbox(4, 4096)?;
        b.lose(o.build.clone(), 7);
        b.lose_unattributed(9);
        b.push(Observation::Call(call(&o, Rpc::Execute, CallOutcome::Ok)));
        let d = b
            .next_dispatch()
            .expect("queued call and losses produce a dispatch");
        b.resolve(d, outcome);
        let (calls, losses, unattributed) = b.into_local();
        assert!(calls.is_empty());
        match outcome {
            Outcome::Accepted => {
                assert!(losses.is_empty());
                assert_eq!(unattributed, 0);
            }
            Outcome::NotApplied => {
                assert_eq!(losses, vec![(o.build.clone(), 8)]);
                assert_eq!(unattributed, 9);
            }
            Outcome::Unknown => {
                assert_eq!(losses, vec![(o.build.clone(), 1)]);
                assert_eq!(unattributed, 0);
            }
        }
    }
    Ok(())
}

#[test]
fn loss_cap_saturation_executed_and_priority() -> Result<(), Box<dyn std::error::Error>> {
    let a = origin("alice", "inv")?;
    let b = origin("bob", "inv")?;
    let mut box_ = outbox(1, 4096)?;
    box_.lose(a.build.clone(), u64::MAX);
    box_.lose(a.build.clone(), 1);
    box_.lose(b.build.clone(), 3);
    box_.lose_unattributed(u64::MAX);
    box_.push(Observation::Executed(Executed {
        origin: b.clone(),
        digest: "x".into(),
        worker: None,
        exit_code: 0,
        error: false,
        times: ExecutionTimes {
            queued: None,
            input_fetch: None,
            executing: None,
            output_upload: None,
            whole: None,
        },
    }));
    let d = box_
        .next_dispatch()
        .expect("queued losses produce a dispatch");
    assert_eq!(d.request().lost[0].count, u64::MAX);
    assert_eq!(d.request().unattributed, u64::MAX);
    assert!(d.request().calls.is_empty());

    let cap = frame()? + call_cost(&a)? + 30;
    let mut box_ = outbox(2, cap)?;
    box_.lose(a.build.clone(), 2);
    box_.push(Observation::Call(call(&b, Rpc::Execute, CallOutcome::Ok)));
    let first = box_
        .next_dispatch()
        .expect("queued loss produces the first dispatch");
    assert_eq!(first.request().lost.len(), 1);
    assert!(first.request().calls.is_empty());
    let second = box_
        .next_dispatch()
        .expect("queued call produces the second dispatch");
    assert_eq!(second.request().calls.len(), 1);
    Ok(())
}

#[test]
fn oversized_flood_and_full_bound_are_finite() -> Result<(), Box<dyn std::error::Error>> {
    let mut large = origin("alice", "inv")?;
    large.target = Some(
        format!("//{}:x", "a".repeat(1000))
            .parse()
            .expect("fixture target fits the name limit"),
    );
    let cap = frame()? + 8;
    let mut box_ = outbox(2, cap)?;
    for _ in 0..1000 {
        box_.push(Observation::Call(call(
            &large,
            Rpc::Execute,
            CallOutcome::Ok,
        )));
    }
    let dispatch = box_
        .next_dispatch()
        .expect("oversized calls produce a loss dispatch");
    assert_cap(dispatch.request(), cap)?;
    assert_eq!(dispatch.request().unattributed, 1000);
    assert!(box_.is_empty());

    let alice = origin("alice", "inv")?;
    let cost = call_cost(&alice)?;
    let cap = frame()? + cost * 3;
    let mut box_ = outbox(2, cap)?;
    let observation = call(&alice, Rpc::Execute, CallOutcome::Ok);
    let mut count = 0;
    while !box_.is_full() {
        box_.push(Observation::Call(observation.clone()));
        count += 1;
    }
    box_.push(Observation::Call(observation));
    count += 1;
    assert!(count * cost < 2 * (cap - frame()?));
    while let Some(dispatch) = box_.next_dispatch() {
        assert_cap(dispatch.request(), cap)?;
    }
    Ok(())
}

#[test]
fn escaped_maximum_metadata_stays_within_cap() -> Result<(), Box<dyn std::error::Error>> {
    let mut alice = origin("alice", &"\"".repeat(64))?;
    alice.correlated = Some(
        "\\".repeat(64)
            .parse()
            .expect("fixture correlation fits the name limit"),
    );
    alice.tool = Some(
        "\"".repeat(128)
            .parse()
            .expect("fixture tool fits the name limit"),
    );
    alice.target = Some(
        "\\".repeat(1024)
            .parse()
            .expect("fixture target fits the name limit"),
    );
    alice.mnemonic = Some(
        "\"".repeat(128)
            .parse()
            .expect("fixture mnemonic fits the name limit"),
    );
    let cost = call_cost(&alice)?;
    let cap = frame()? + cost;
    let observation = call(&alice, Rpc::Execute, CallOutcome::Ok);
    let mut box_ = outbox(4, cap)?;
    for _ in 0..10 {
        box_.push(Observation::Call(observation.clone()));
    }
    let mut received = Vec::new();
    while let Some(dispatch) = box_.next_dispatch() {
        assert_cap(dispatch.request(), cap)?;
        received.extend(
            decode(dispatch.request().clone(), 4)
                .expect("dispatched request decodes")
                .calls,
        );
    }
    assert_eq!(received, vec![observation; 10]);
    Ok(())
}

#[test]
fn decode_rejects_invalid_input_and_unknown_wire_enums() -> Result<(), Box<dyn std::error::Error>> {
    let o = origin("alice", "inv")?;
    let good = wire_origin(&o);
    let request = |origin: IngestOrigin| IngestBuildsRequest {
        origins: vec![origin],
        calls: vec![IngestCall {
            origin: 0,
            rpc: IngestRpc::Execute,
            outcome: IngestOutcome::Ok,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        }],
        lost: vec![],
        unattributed: 0,
        traffic: None,
    };
    let mut invalid = good.clone();
    invalid.principal = "bad".into();
    assert!(decode(request(invalid), 1).is_err());
    let mut invalid = good.clone();
    invalid.invocation_id = "x".repeat(65);
    assert!(decode(request(invalid), 1).is_err());
    let mut invalid = good.clone();
    invalid.tool = Some(String::new());
    assert!(decode(request(invalid), 1).is_err());
    let mut invalid = request(good.clone());
    invalid.calls[0].origin = 1;
    assert!(decode(invalid, 1).is_err());
    let mut invalid = request(good.clone());
    invalid.lost.push(IngestLoss {
        origin: 0,
        count: 0,
    });
    assert!(decode(invalid, 1).is_err());
    let mut invalid = request(good.clone());
    invalid.lost.push(IngestLoss {
        origin: 0,
        count: 1,
    });
    assert!(decode(invalid, 0).is_err());
    for (rpc, outcome) in [("FOO", "OK"), ("EXECUTE", "BAR")] {
        let mut json =
            serde_json::to_value(request(good.clone())).expect("fixture request serializes");
        json["calls"][0]["rpc"] = rpc.into();
        json["calls"][0]["outcome"] = outcome.into();
        let parsed: IngestBuildsRequest =
            serde_json::from_value(json).expect("unknown wire enums deserialize");
        assert!(matches!(decode(parsed, 1), Err(IngestError::UnknownValue)));
    }
    let mut req = request(good);
    req.origins.push(wire_origin(&origin("bob", "inv")?));
    req.calls.push(IngestCall {
        origin: 1,
        rpc: IngestRpc::Execute,
        outcome: IngestOutcome::Ok,
        uploaded_bytes: 0,
        downloaded_bytes: 0,
    });
    let result = decode(req, 1).expect("two valid fixture origins decode");
    assert_ne!(result.calls[0].origin.build, result.calls[1].origin.build);
    Ok(())
}

#[test]
fn traffic_acknowledges_only_the_snapshot_that_was_dispatched()
-> Result<(), Box<dyn std::error::Error>> {
    let mut b = outbox(2, 4096)?;
    let start = Timestamp::from_unix_millis(1234);
    let first = Traffic {
        lookups: 5,
        hits: 3,
        uploaded: 7,
        downloaded: 11,
    };
    let newer = Traffic {
        lookups: 8,
        ..first
    };
    b.traffic(start, first);
    assert!(!b.is_empty());
    let old = b.next_dispatch().unwrap();
    assert_eq!(old.request().traffic.as_ref().unwrap().lookups, 5);
    b.traffic(start, newer);
    b.resolve(old, Outcome::Accepted);
    assert!(!b.is_empty());
    let current = b.next_dispatch().unwrap();
    assert_eq!(current.request().traffic.as_ref().unwrap().lookups, 8);
    b.resolve(current, Outcome::Accepted);
    assert!(b.is_empty());
    b.traffic(start, newer);
    assert!(b.is_empty());
    b.lose_unattributed(1);
    let duplicate = b.next_dispatch().unwrap();
    assert_eq!(duplicate.request().traffic.as_ref().unwrap().lookups, 8);
    b.resolve(duplicate, Outcome::Accepted);
    assert!(b.is_empty());
    Ok(())
}

#[test]
fn ambiguous_and_not_applied_traffic_retransmit_latest() -> Result<(), Box<dyn std::error::Error>> {
    for outcome in [Outcome::Unknown, Outcome::NotApplied] {
        let mut b = outbox(2, 4096)?;
        let start = Timestamp::UNIX_EPOCH;
        let old = Traffic {
            uploaded: 1,
            ..Traffic::default()
        };
        let new = Traffic { uploaded: 2, ..old };
        b.traffic(start, old);
        let d = b.next_dispatch().unwrap();
        b.resolve(d, outcome);
        assert_eq!(
            b.next_dispatch()
                .unwrap()
                .request()
                .traffic
                .as_ref()
                .unwrap()
                .uploaded_bytes,
            1
        );
        b.traffic(start, new);
        let d = b.next_dispatch().unwrap();
        assert_eq!(d.request().traffic.as_ref().unwrap().uploaded_bytes, 2);
        b.resolve(d, Outcome::Accepted);
        assert!(b.is_empty());
    }
    Ok(())
}

#[test]
fn full_batch_with_maximum_traffic_and_call_bytes_fits() -> Result<(), Box<dyn std::error::Error>> {
    let o = origin("alice", "inv")?;
    let mut max = call(&o, Rpc::Cas, CallOutcome::Error);
    max.transferred = Transferred {
        uploaded: u64::MAX,
        downloaded: u64::MAX,
    };
    let cap = frame()? + call_cost(&o)? + 100;
    let mut b = outbox(2, cap)?;
    b.traffic(
        Timestamp::END_OF_TIME,
        Traffic {
            lookups: u64::MAX,
            hits: u64::MAX,
            uploaded: u64::MAX,
            downloaded: u64::MAX,
        },
    );
    for _ in 0..8 {
        b.push(Observation::Call(max.clone()));
    }
    let mut received = 0;
    while !b.is_empty() {
        let d = b.next_dispatch().unwrap();
        assert_cap(d.request(), cap)?;
        assert_eq!(
            d.request().traffic.as_ref().unwrap().downloaded_bytes,
            u64::MAX
        );
        received += d.request().calls.len();
        b.resolve(d, Outcome::Accepted);
    }
    assert_eq!(received, 8);
    Ok(())
}
