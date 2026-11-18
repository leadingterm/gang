//! The leader side of build-observation forwarding: proof check and apply.

use std::num::NonZeroU64;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_insight::Ingested;
use gang_mtls::{ControllerKey, MAX_PROOF_BYTES, PROOF_HEADER, verify};
use gang_spi::{ControllerId, Observation, ObservationSink};
use gang_types::NonZeroDuration;
use gang_wire::{ErrorReason, Status};
use http::HeaderMap;

use crate::leadership::Leading;

pub(crate) struct Ingest {
    sink: ObservationSink,
    leading: Leading,
    key: ControllerKey,
    issuer: String,
    skew: Duration,
    max_ttl: NonZeroDuration,
    pub(crate) max_lost: usize,
}

impl Ingest {
    pub(crate) fn new(
        sink: ObservationSink,
        leading: Leading,
        key: ControllerKey,
        issuer: String,
        skew: Duration,
        max_ttl: NonZeroDuration,
        max_lost: usize,
    ) -> Self {
        Self {
            sink,
            leading,
            key,
            issuer,
            skew,
            max_ttl,
            max_lost,
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

    pub(crate) fn apply(&self, ingested: Ingested) {
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
}
