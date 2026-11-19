//! The leader side of build-observation forwarding: proof check and apply.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::LeadershipConfig;
use gang_insight::Ingested;
use gang_mtls::{ControllerKey, MAX_PROOF_BYTES, PROOF_HEADER, verify};
use gang_spi::{ControllerId, Observation, ObservationSink, Traffic};
use gang_types::NonZeroDuration;
use gang_wire::{ErrorReason, IngestTraffic, Status};
use http::HeaderMap;
use parking_lot::Mutex;

use crate::leadership::Leading;

/// The latest cumulative traffic one follower process reported.
struct Series {
    /// When the follower process started counting.
    start: Timestamp,
    /// The highest totals reported for this process.
    totals: Traffic,
    /// When the leader last accepted a report of this process.
    seen: Timestamp,
}

pub(crate) struct Ingest {
    sink: ObservationSink,
    leading: Leading,
    key: ControllerKey,
    issuer: String,
    skew: Duration,
    max_ttl: NonZeroDuration,
    pub(crate) max_lost: usize,
    max_series: NonZeroUsize,
    series: Mutex<HashMap<ControllerId, Series>>,
}

impl Ingest {
    /// An ingest that verifies proofs with the leadership's skew and lease lifetime. It keeps the
    /// traffic watermark of at most `max_series` follower controllers for the whole term.
    pub(crate) fn new(
        sink: ObservationSink,
        leading: Leading,
        key: ControllerKey,
        issuer: String,
        leadership: &LeadershipConfig,
        max_lost: usize,
        max_series: NonZeroUsize,
    ) -> Self {
        Self {
            sink,
            leading,
            key,
            issuer,
            skew: leadership.clock_skew,
            max_ttl: leadership.ttl,
            max_lost,
            max_series,
            series: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn leads(&self) -> bool {
        self.leading.get()
    }

    pub(crate) fn admit(
        &self,
        now: Timestamp,
        headers: &HeaderMap,
    ) -> Result<ControllerId, Status> {
        let mut values = headers.get_all(PROOF_HEADER).iter();
        let value = values
            .next()
            .ok_or(Status::from(ErrorReason::CredentialsMissing))?;
        if values.next().is_some()
            || value.len() > MAX_PROOF_BYTES
            || !value
                .as_bytes()
                .iter()
                .all(|byte| (0x21..=0x7e).contains(byte))
        {
            return Err(ErrorReason::CredentialsInvalid.into());
        }
        let text = value
            .to_str()
            .map_err(|_| Status::from(ErrorReason::CredentialsInvalid))?;
        verify(&self.key, text, &self.issuer, now, self.skew, self.max_ttl).map_err(|error| {
            tracing::debug!(%error, "invalid controller proof");
            ErrorReason::CredentialsInvalid.into()
        })
    }

    /// Applies a validated batch from the admitted `controller` at `now`: counts the increase of
    /// its traffic series, then records its calls and losses without counting them again.
    pub(crate) fn apply(&self, controller: &ControllerId, now: Timestamp, ingested: Ingested) {
        if let Some(reported) = &ingested.traffic {
            self.sink.count(self.increase(controller, now, reported));
        }
        for call in ingested.calls {
            self.sink.record(Observation::Call(call));
        }
        for (key, count) in ingested.lost {
            self.sink.lose(&key, count);
        }
        if let Some(count) = NonZeroU64::new(ingested.unattributed) {
            self.sink.lose_unattributed(count);
        }
    }

    /// Folds `reported` into the controller's series and returns the traffic not counted yet.
    ///
    /// A known process adds what its totals grew by, however long it was quiet. Its totals only
    /// grow, and every hit is a lookup, so a report where a counter went backwards or more hits
    /// than lookups were added is rejected whole: it adds nothing and leaves the series as it
    /// was. A report from an older process than the known one adds nothing and is dropped. A
    /// process not known yet adds its whole totals when it started after this leader began
    /// leading, and only sets the baseline otherwise. A controller not known yet, at
    /// `max_series`, first evicts the least recently updated one, whose next report then counts
    /// as a new process.
    fn increase(
        &self,
        controller: &ControllerId,
        now: Timestamp,
        reported: &IngestTraffic,
    ) -> Traffic {
        let totals = Traffic {
            lookups: reported.lookups,
            hits: reported.hits,
            uploaded: reported.uploaded_bytes,
            downloaded: reported.downloaded_bytes,
        };
        let process = Series {
            start: reported.start_time,
            totals,
            seen: now,
        };
        let mut series = self.series.lock();
        match series.get_mut(controller) {
            Some(known) if reported.start_time == known.start => {
                let Some(delta) = consistent_growth(known.totals, totals) else {
                    tracing::warn!(
                        %controller,
                        "inconsistent follower traffic report; ignoring its traffic"
                    );
                    return Traffic::default();
                };
                known.totals = totals;
                known.seen = now;
                delta
            }
            Some(known) if reported.start_time < known.start => Traffic::default(),
            Some(known) => {
                *known = process;
                self.first_report(known.start, totals)
            }
            None => {
                if series.len() >= self.max_series.get() {
                    evict_stalest(&mut series, self.max_series);
                }
                series.insert(controller.clone(), process);
                self.first_report(reported.start_time, totals)
            }
        }
    }

    /// What the first report of a process started at `start` adds: all of `totals` when it
    /// started during this term, nothing otherwise.
    fn first_report(&self, start: Timestamp, totals: Traffic) -> Traffic {
        if self.leading.start().is_some_and(|term| start >= term) {
            totals
        } else {
            Traffic::default()
        }
    }
}

/// Forgets the series updated least recently, the smallest controller id among equals.
fn evict_stalest(series: &mut HashMap<ControllerId, Series>, max_series: NonZeroUsize) {
    let stalest = series
        .iter()
        .min_by(|(a, left), (b, right)| {
            left.seen
                .cmp(&right.seen)
                .then_with(|| a.as_str().cmp(b.as_str()))
        })
        .map(|(controller, _)| controller.clone());
    if let Some(controller) = stalest {
        series.remove(&controller);
        tracing::warn!(
            %controller,
            max = max_series.get(),
            "too many follower traffic series; forgetting the least recently updated, whose \
             traffic may be counted again"
        );
    }
}

/// How far each counter grew from `old` to `new`, or `None` when a counter went backwards or
/// more hits than lookups were added.
fn consistent_growth(old: Traffic, new: Traffic) -> Option<Traffic> {
    let delta = Traffic {
        lookups: new.lookups.checked_sub(old.lookups)?,
        hits: new.hits.checked_sub(old.hits)?,
        uploaded: new.uploaded.checked_sub(old.uploaded)?,
        downloaded: new.downloaded.checked_sub(old.downloaded)?,
    };
    (delta.hits <= delta.lookups).then_some(delta)
}

#[cfg(test)]
mod tests {
    use gang_spi::CertificateSignerPort;
    use gang_testkit::TestSigner;

    use super::*;

    const MAX_SERIES: NonZeroUsize = NonZeroUsize::new(2).expect("2 is non-zero");

    fn report(start: Timestamp, lookups: u64) -> Ingested {
        Ingested {
            calls: Vec::new(),
            lost: Vec::new(),
            unattributed: 0,
            traffic: Some(IngestTraffic {
                start_time: start,
                lookups,
                ..IngestTraffic::default()
            }),
        }
    }

    // Catches an eviction that lets the map outgrow its cap, a restart of a known controller
    // evicting another one, and evicting the most recently updated series.
    #[tokio::test]
    async fn the_series_map_never_exceeds_its_cap() {
        let signer = TestSigner::authority().expect("signer");
        let public_key = signer.public_key().await.expect("public key");
        let key = ControllerKey::from_spki(&public_key).expect("controller key");
        let leading = Leading::default();
        leading.set(Timestamp::UNIX_EPOCH);
        let (sink, _observations) = ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let ingest = Ingest::new(
            sink,
            leading,
            key,
            "https://gang.example.test".to_owned(),
            &LeadershipConfig::default(),
            8,
            MAX_SERIES,
        );
        let start = Timestamp::from_unix_millis(1_000);
        let at = Timestamp::from_unix_millis;
        let (a, b, c) = (
            ControllerId::new("a"),
            ControllerId::new("b"),
            ControllerId::new("c"),
        );
        ingest.apply(&a, at(2_000), report(start, 1));
        ingest.apply(&b, at(3_000), report(start, 1));
        ingest.apply(&a, at(4_000), report(at(3_500), 1));
        assert_eq!(
            ingest.series.lock().len(),
            2,
            "a restart replaces its own series"
        );
        ingest.apply(&c, at(5_000), report(start, 1));
        let series = ingest.series.lock();
        assert_eq!(series.len(), MAX_SERIES.get());
        assert!(series.contains_key(&a) && series.contains_key(&c));
    }
}
