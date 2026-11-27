//! Golden-fixture tests proving byte-for-byte parity with the `NativeLink` v1 learned-history
//! wire format, `SHA-256` keys, and shaped resource/duration reservations.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::HistoryWindow;
use gang_scheduler::{
    ActionDurations, Budget, Histories, Observation, Policy, ResourceSketch, Shaped, SketchKey,
    history_id,
};
use gang_spi::{LearnedHistory, RecordId, RecordKey, RecordKind};
use gang_types::Percent;

const TARGET_RESOURCE_ID: [u8; 32] = [
    125, 150, 197, 151, 12, 15, 16, 114, 221, 24, 96, 76, 65, 250, 140, 138, 107, 160, 93, 176,
    157, 166, 104, 217, 162, 185, 82, 201, 225, 103, 237, 22,
];

const TARGET_RESOURCE_BODY: &[u8] = &[
    1, 0, 0, 0, 0, 0, 2, 163, 0, 78, 76, 72, 83, 3, 2, 0, 0, 0, 0, 163, 2, 0, 0, 0, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 47, 47, 97, 112, 112, 58, 98, 105, 110, 1, 10, 0, 0, 0, 67, 112,
    112, 67, 111, 109, 112, 105, 108, 101, 2, 0, 0, 0, 1, 5, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 48, 117, 0, 0, 0, 0, 0, 0, 244, 1, 0, 0, 0, 0, 0,
    0, 0, 8, 0, 0, 0, 0, 0, 0, 1, 5, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 115, 87, 0, 0, 0, 0, 0, 0, 220, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0,
    0, 64, 156, 0, 0, 0, 0, 0, 0, 128, 81, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 163, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
];

const COMMAND_RESOURCE_ID: [u8; 32] = [
    14, 222, 137, 161, 18, 161, 103, 147, 222, 51, 208, 202, 208, 160, 6, 238, 235, 72, 250, 185,
    195, 33, 168, 136, 108, 30, 250, 198, 123, 208, 114, 189,
];

const COMMAND_RESOURCE_BODY: &[u8] = &[
    1, 0, 0, 0, 0, 0, 2, 163, 0, 78, 76, 72, 83, 3, 2, 0, 0, 0, 0, 163, 2, 0, 0, 0, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 2, 11, 0, 0, 0, 100, 101, 97, 100, 98, 101, 101, 102, 47, 54, 52, 1, 0, 0, 0, 1,
    5, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 115, 87,
    0, 0, 0, 0, 0, 0, 220, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 163, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
    0, 0, 0, 0,
];

const TARGET_DURATION_ID: [u8; 32] = [
    231, 70, 45, 215, 198, 118, 68, 238, 138, 221, 118, 67, 163, 123, 241, 174, 163, 166, 5, 99,
    232, 90, 224, 3, 223, 92, 156, 195, 139, 31, 147, 217,
];

const TARGET_DURATION_BODY: &[u8] = br#"{"version":1,"sampler_generation":2,"window_ms":172800000,"key":{"Target":{"mnemonic":"CppCompile","target":"//app:bin"}},"entry":{"period":2,"current":{"sum_ms":1200,"count":1},"previous":{"sum_ms":0,"count":0},"last_seen_ms":172800000}}"#;

fn make_histories(percentile: u8) -> Histories {
    let window = HistoryWindow::from_units(2);
    let sketch = ResourceSketch::with_shards(
        Policy {
            headroom_percent: 20,
            cpu_reservation_percentile: Percent::from_percent(percentile),
            classes: Vec::new(),
            reserve_cpu: true,
            reserve_memory: true,
            reserve_disk: true,
        },
        Budget {
            target: 8,
            mnemonic: 8,
            command: 8,
            probation: 8,
        },
        window.get(),
        1,
    );
    let durations = ActionDurations::new(NonZeroUsize::MIN.saturating_add(7), window);
    Histories::new(sketch, durations)
}

fn golden_records() -> Vec<LearnedHistory> {
    let expires_at = Timestamp::from_unix_millis(345_600_000);
    vec![
        LearnedHistory {
            key: RecordKey {
                kind: RecordKind::ResourceHistory,
                id: RecordId::new(TARGET_RESOURCE_ID),
            },
            body: TARGET_RESOURCE_BODY.to_vec(),
            expires_at,
        },
        LearnedHistory {
            key: RecordKey {
                kind: RecordKind::ResourceHistory,
                id: RecordId::new(COMMAND_RESOURCE_ID),
            },
            body: COMMAND_RESOURCE_BODY.to_vec(),
            expires_at,
        },
        LearnedHistory {
            key: RecordKey {
                kind: RecordKind::DurationHistory,
                id: RecordId::new(TARGET_DURATION_ID),
            },
            body: TARGET_DURATION_BODY.to_vec(),
            expires_at,
        },
    ]
}

#[test]
fn golden_v1_records_decode_reencode_byte_for_byte() {
    assert_eq!(
        history_id(0, &["//app:bin", "CppCompile"]),
        RecordId::new(TARGET_RESOURCE_ID)
    );
    assert_eq!(
        history_id(3, &["deadbeef/64"]),
        RecordId::new(COMMAND_RESOURCE_ID)
    );
    assert_eq!(
        history_id(0, &["CppCompile", "//app:bin"]),
        RecordId::new(TARGET_DURATION_ID)
    );

    let golden = golden_records();
    let now = Timestamp::from_unix_millis(172_801_000);
    let now_s = now.as_unix_millis() / 1000;

    let mut median = make_histories(50);
    median.restore(now, &golden).expect("golden restore");

    let target_key = SketchKey::for_attempt(Some("//app:bin"), Some("CppCompile"), "");
    let cmd_key = SketchKey::for_attempt(None, None, "deadbeef/64");

    let target_entry = median
        .resource_mut()
        .lookup(&target_key, now_s)
        .expect("target restored");
    assert_eq!(
        median.resource().shape(&target_entry),
        Shaped {
            cpu_count: 1_200,
            memory_kb: 48_000,
            disk_kb: 2_458,
            class: None,
        }
    );

    let cmd_entry = median
        .resource_mut()
        .lookup(&cmd_key, now_s)
        .expect("command restored");
    assert_eq!(
        median.resource().shape(&cmd_entry),
        Shaped {
            cpu_count: 1_800,
            memory_kb: 26_865,
            disk_kb: 1_844,
            class: None,
        }
    );

    assert_eq!(
        median
            .durations_mut()
            .expected("//app:bin", "CppCompile", now),
        Some(Duration::from_millis(1_200))
    );

    let mut reencoded = Vec::new();
    median.snapshot(now, &mut reencoded);
    assert_eq!(reencoded.len(), golden.len());
    for expected in &golden {
        let actual = reencoded
            .iter()
            .find(|r| r.key == expected.key)
            .expect("golden key present after re-encode");
        assert_eq!(actual.expires_at, expected.expires_at);
        assert_eq!(actual.body, expected.body);
    }
}

#[test]
fn golden_v1_replay_from_observations_and_percentile_override() {
    let golden = golden_records();
    let now = Timestamp::from_unix_millis(172_801_000);
    let now_s = now.as_unix_millis() / 1000;
    let target_key = SketchKey::for_attempt(Some("//app:bin"), Some("CppCompile"), "");
    let cmd_key = SketchKey::for_attempt(None, None, "deadbeef/64");

    let mut fresh = make_histories(50);
    fresh.resource_mut().observe(
        target_key.clone(),
        &Observation {
            peak_memory_kb: 22_387,
            cpu_millicores: Some(1_500),
            peak_disk_kb: 1_536,
            killed_peak_memory_kb: Some(40_000),
            killed_peak_disk_kb: None,
            killed: true,
            now_s: 86_400,
        },
    );
    fresh.resource_mut().observe(
        target_key.clone(),
        &Observation {
            peak_memory_kb: 30_000,
            cpu_millicores: Some(500),
            peak_disk_kb: 2_048,
            killed_peak_memory_kb: None,
            killed_peak_disk_kb: None,
            killed: false,
            now_s: 172_800,
        },
    );
    let cmd_obs = Observation {
        peak_memory_kb: 22_387,
        cpu_millicores: Some(1_500),
        peak_disk_kb: 1_536,
        killed_peak_memory_kb: None,
        killed_peak_disk_kb: None,
        killed: false,
        now_s: 172_800,
    };
    assert!(!fresh.resource_mut().observe(cmd_key.clone(), &cmd_obs));
    assert!(fresh.resource_mut().observe(cmd_key, &cmd_obs));
    fresh.durations_mut().record(
        "//app:bin",
        "CppCompile",
        1_200,
        Timestamp::from_unix_millis(172_800_000),
    );

    let mut fresh_records = Vec::new();
    fresh.snapshot(now, &mut fresh_records);
    for expected in &golden {
        let actual = fresh_records
            .iter()
            .find(|r| r.key == expected.key)
            .expect("fresh record matches golden key");
        assert_eq!(actual.body, expected.body);
    }

    let mut p95 = make_histories(95);
    p95.restore(now, &golden).expect("p95 restore");
    let p95_entry = p95
        .resource_mut()
        .lookup(&target_key, now_s)
        .expect("p95 target restored");
    assert_eq!(p95.resource().shape(&p95_entry).cpu_count, 1_800);
}
