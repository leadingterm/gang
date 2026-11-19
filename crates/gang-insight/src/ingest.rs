//! Validation of forwarded build observations.

use std::num::NonZeroU64;
use std::sync::Arc;

use gang_spi::{Call, CallOutcome, Origin, Rpc, Transferred};
use gang_types::{BuildKey, Identity, InvocationId, Mnemonic, TargetLabel, ToolDetails};
use gang_wire::{IngestBuildsRequest, IngestOrigin, IngestOutcome, IngestRpc, IngestTraffic};

use crate::IngestError;

/// A fully validated batch of forwarded observations.
#[derive(Debug)]
pub struct Ingested {
    /// Calls to record.
    pub calls: Vec<Call>,
    /// Nonzero losses attributed to build keys.
    pub lost: Vec<(BuildKey, NonZeroU64)>,
    /// Losses that could not be attributed to a build.
    pub unattributed: u64,
    /// Sender's cumulative traffic snapshot.
    pub traffic: Option<IngestTraffic>,
}

fn origin(value: IngestOrigin) -> Result<Origin, IngestError> {
    Ok(Origin {
        build: BuildKey {
            principal: Arc::new(value.principal.parse::<Identity>()?),
            invocation: InvocationId::try_new(value.invocation_id)?,
        },
        correlated: value
            .correlated_invocations_id
            .map(InvocationId::try_new)
            .transpose()?,
        tool: value.tool.map(ToolDetails::try_new).transpose()?,
        target: value.target.map(TargetLabel::try_new).transpose()?,
        mnemonic: value.mnemonic.map(Mnemonic::try_new).transpose()?,
    })
}

fn lookup(origins: &[Origin], index: u32) -> Result<&Origin, IngestError> {
    usize::try_from(index)
        .ok()
        .and_then(|index| origins.get(index))
        .ok_or(IngestError::Origin { index })
}

/// Validates the entire request before returning observations to apply.
///
/// # Errors
/// Returns an error for invalid origins, references, counts, transferred bytes, or unknown wire values.
pub fn decode(request: IngestBuildsRequest, max_lost: usize) -> Result<Ingested, IngestError> {
    if request
        .traffic
        .as_ref()
        .is_some_and(|traffic| traffic.hits > traffic.lookups)
    {
        return Err(IngestError::ExcessHits);
    }
    let origins = request
        .origins
        .into_iter()
        .map(origin)
        .collect::<Result<Vec<_>, _>>()?;
    if request.lost.len() > max_lost {
        return Err(IngestError::TooManyLosses);
    }
    let calls = request
        .calls
        .into_iter()
        .map(|call| {
            let origin = lookup(&origins, call.origin)?.clone();
            let rpc = match call.rpc {
                IngestRpc::GetActionResult => Rpc::GetActionResult,
                IngestRpc::UpdateActionResult => Rpc::UpdateActionResult,
                IngestRpc::Execute => Rpc::Execute,
                IngestRpc::Cas => Rpc::Cas,
                IngestRpc::Unrecognized(_) => return Err(IngestError::UnknownValue),
            };
            if rpc == Rpc::Execute && (call.uploaded_bytes != 0 || call.downloaded_bytes != 0) {
                return Err(IngestError::ExecuteTransferred);
            }
            let outcome = match call.outcome {
                IngestOutcome::Ok => CallOutcome::Ok,
                IngestOutcome::NotFound => CallOutcome::NotFound,
                IngestOutcome::Error => CallOutcome::Error,
                IngestOutcome::Unrecognized(_) => return Err(IngestError::UnknownValue),
            };
            Ok(Call {
                origin,
                rpc,
                outcome,
                transferred: Transferred {
                    uploaded: call.uploaded_bytes,
                    downloaded: call.downloaded_bytes,
                },
            })
        })
        .collect::<Result<Vec<_>, IngestError>>()?;
    let lost = request
        .lost
        .into_iter()
        .map(|loss| {
            let build = lookup(&origins, loss.origin)?.build.clone();
            let count = NonZeroU64::new(loss.count).ok_or(IngestError::ZeroLoss)?;
            Ok((build, count))
        })
        .collect::<Result<Vec<_>, IngestError>>()?;
    Ok(Ingested {
        calls,
        lost,
        unattributed: request.unattributed,
        traffic: request.traffic,
    })
}
