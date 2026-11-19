//! Proof caching and bounded follower forwarding.

use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use bytes::Bytes;
use gang_clock::Timestamp;
use gang_insight::{Outbox, OutboxError, Outcome};
use gang_mtls::{ControllerProof, ProofError};
use gang_spi::{CertificateSignerPort, ControllerId, Traffic};
use gang_types::NonZeroDuration;

pub(crate) trait Uplink: Send + Sync {
    fn send(&self, proof: &ControllerProof, body: Bytes) -> impl Future<Output = Outcome> + Send;
}

/// Mints and caches a controller proof for half its lifetime.
pub(crate) struct ProofSource {
    controller: ControllerId,
    issuer: String,
    ttl: NonZeroDuration,
    cached: Option<(ControllerProof, Timestamp)>,
}

impl ProofSource {
    pub(crate) fn new(controller: ControllerId, issuer: String, ttl: NonZeroDuration) -> Self {
        Self {
            controller,
            issuer,
            ttl,
            cached: None,
        }
    }

    pub(crate) async fn current<S: CertificateSignerPort>(
        &mut self,
        signer: &S,
        now: Timestamp,
    ) -> Result<&ControllerProof, ProofError> {
        match &mut self.cached {
            Some((proof, minted)) => {
                if now.saturating_duration_since(*minted) >= self.ttl.get() / 2 {
                    *proof = gang_mtls::mint(signer, &self.issuer, &self.controller, now, self.ttl)
                        .await?;
                    *minted = now;
                }
                Ok(proof)
            }
            slot @ None => {
                let proof =
                    gang_mtls::mint(signer, &self.issuer, &self.controller, now, self.ttl).await?;
                Ok(&slot.insert((proof, now)).0)
            }
        }
    }
}

/// The follower recorder's transport, proof, buffer and retry cadence.
pub(crate) struct Forwarding<U: Uplink> {
    pub(crate) uplink: U,
    pub(crate) proofs: ProofSource,
    pub(crate) outbox: Outbox,
    pub(crate) pause: Duration,
    /// When this process began counting the traffic it forwards.
    pub(crate) start: Timestamp,
}

impl<U: Uplink> Forwarding<U> {
    /// Forwarding for a process that began counting traffic at `start`. The outbox starts with
    /// zero traffic, so the first dispatch tells the leader the series exists.
    pub(crate) fn try_new(
        uplink: U,
        proofs: ProofSource,
        pause: Duration,
        max_keys: NonZeroUsize,
        max_request_size: usize,
        start: Timestamp,
    ) -> Result<Self, OutboxError> {
        let max_bytes = NonZeroUsize::new(max_request_size).unwrap_or(NonZeroUsize::MIN);
        let mut outbox = Outbox::try_new(max_keys, max_bytes)?;
        outbox.traffic(start, Traffic::default());
        Ok(Self {
            uplink,
            proofs,
            outbox,
            pause,
            start,
        })
    }
}
