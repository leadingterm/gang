//! Bounded, synchronous follower observation forwarding.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::num::NonZeroUsize;

use gang_spi::{Call, CallOutcome, Observation, Origin, Rpc};
use gang_types::BuildKey;
use gang_wire::{
    IngestBuildsRequest, IngestCall, IngestLoss, IngestOrigin, IngestOutcome, IngestRpc,
};

use crate::OutboxError;

struct PendingCall {
    call: Call,
    origin: IngestOrigin,
    wire: IngestCall,
    origin_cost: usize,
    item_cost: usize,
}

struct PendingLoss {
    count: u64,
    origin: IngestOrigin,
    origin_cost: usize,
    item_cost: usize,
}

/// Bounded observations waiting to be forwarded to the leader.
///
/// Callers should stop pushing when [`Self::is_full`] is true. Losses are
/// bounded separately by the key limit, with overflow counted as unattributed.
pub struct Outbox {
    calls: VecDeque<PendingCall>,
    lost: HashMap<BuildKey, PendingLoss>,
    unattributed: u64,
    max_keys: usize,
    budget: usize,
    used: usize,
}

/// A removed batch awaiting its delivery outcome.
pub struct Dispatch {
    request: IngestBuildsRequest,
    calls: Vec<Call>,
    lost: Vec<(BuildKey, u64)>,
    unattributed: u64,
}

impl Dispatch {
    /// The request to send; it is within the configured byte cap.
    #[must_use]
    pub fn request(&self) -> &IngestBuildsRequest {
        &self.request
    }

    /// The carried loss count that would be dropped for an unknown outcome.
    /// The sum saturates at `u64::MAX` and excludes losses derived from calls.
    #[must_use]
    pub fn carried_losses(&self) -> u64 {
        self.lost
            .iter()
            .fold(self.unattributed, |total, (_, count)| {
                total.saturating_add(*count)
            })
    }
}

/// What is known about whether the leader applied a batch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    /// The leader applied the request.
    Accepted,
    /// The leader definitely applied nothing; carried losses may be retried.
    NotApplied,
    /// The leader may have applied it; carried losses must not be retried.
    Unknown,
}

impl Outbox {
    /// Creates an outbox with limits on carried loss keys and request bytes.
    ///
    /// # Errors
    /// Returns [`OutboxError::CapTooSmall`] if the cap cannot hold the frame.
    pub fn try_new(max_keys: NonZeroUsize, max_bytes: NonZeroUsize) -> Result<Self, OutboxError> {
        let frame = json_len(&IngestBuildsRequest::default()) + 20;
        let budget = max_bytes
            .get()
            .checked_sub(frame)
            .ok_or(OutboxError::CapTooSmall { needed: frame })?;
        Ok(Self {
            calls: VecDeque::new(),
            lost: HashMap::new(),
            unattributed: 0,
            max_keys: max_keys.get(),
            budget,
            used: 0,
        })
    }

    /// Queues a call, or counts an execution or oversized call as lost.
    pub fn push(&mut self, observation: Observation) {
        let call = match observation {
            Observation::Call(call) => call,
            Observation::Executed(executed) => {
                self.lose(executed.origin.build, 1);
                return;
            }
        };
        let origin = wire_origin(&call.origin);
        let wire = IngestCall {
            origin: u32::MAX,
            rpc: wire_rpc(call.rpc),
            outcome: wire_outcome(call.outcome),
        };
        let origin_cost = json_len(&origin) + 1;
        let item_cost = json_len(&wire) + 1;
        let cost = origin_cost.saturating_add(item_cost);
        if cost > self.budget {
            self.lose(call.origin.build, 1);
            return;
        }
        self.used = self.used.saturating_add(cost);
        self.calls.push_back(PendingCall {
            call,
            origin,
            wire,
            origin_cost,
            item_cost,
        });
    }

    /// Counts lost observations, saturating and spilling excess keys to unattributed.
    pub fn lose(&mut self, key: BuildKey, count: u64) {
        if count == 0 {
            return;
        }
        if let Some(existing) = self.lost.get_mut(&key) {
            existing.count = existing.count.saturating_add(count);
            return;
        }
        if self.lost.len() >= self.max_keys {
            self.lose_unattributed(count);
            return;
        }
        let origin = key_origin(&key);
        let wire = IngestLoss {
            origin: u32::MAX,
            count: u64::MAX,
        };
        let origin_cost = json_len(&origin) + 1;
        let item_cost = json_len(&wire) + 1;
        let cost = origin_cost.saturating_add(item_cost);
        if cost > self.budget {
            self.lose_unattributed(count);
            return;
        }
        self.used = self.used.saturating_add(cost);
        self.lost.insert(
            key,
            PendingLoss {
                count,
                origin,
                origin_cost,
                item_cost,
            },
        );
    }

    /// Counts losses whose build cannot be carried, saturating at `u64::MAX`.
    pub fn lose_unattributed(&mut self, count: u64) {
        self.unattributed = self.unattributed.saturating_add(count);
    }

    /// Whether there is nothing to dispatch or hand over locally.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty() && self.lost.is_empty() && self.unattributed == 0
    }

    /// Whether the conservative pending-size estimate reaches the request budget.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.used >= self.budget
    }

    /// Removes the next bounded request, prioritizing carried losses over calls.
    pub fn next_dispatch(&mut self) -> Option<Dispatch> {
        if self.is_empty() {
            return None;
        }
        let mut dictionary = HashMap::new();
        let mut used = 0usize;
        let mut selected_losses = Vec::new();
        for (key, loss) in &self.lost {
            let existing = dictionary.get(&loss.origin).copied();
            let cost = loss.item_cost.saturating_add(if existing.is_none() {
                loss.origin_cost
            } else {
                0
            });
            if used.saturating_add(cost) > self.budget {
                continue;
            }
            let index = if let Some(index) = existing {
                index
            } else {
                let Ok(index) = u32::try_from(dictionary.len()) else {
                    continue;
                };
                dictionary.insert(&loss.origin, index);
                index
            };
            selected_losses.push((key.clone(), index, existing.is_none()));
            used += cost;
        }
        let mut selected_calls = Vec::new();
        for pending in &self.calls {
            let existing = dictionary.get(&pending.origin).copied();
            let cost = pending.item_cost.saturating_add(if existing.is_none() {
                pending.origin_cost
            } else {
                0
            });
            if used.saturating_add(cost) > self.budget {
                break;
            }
            let index = if let Some(index) = existing {
                index
            } else {
                let Ok(index) = u32::try_from(dictionary.len()) else {
                    break;
                };
                dictionary.insert(&pending.origin, index);
                index
            };
            selected_calls.push((index, existing.is_none()));
            used += cost;
        }
        drop(dictionary);

        let mut request = IngestBuildsRequest::default();
        let mut losses = Vec::with_capacity(selected_losses.len());
        for (key, index, new_origin) in selected_losses {
            let loss = self.lost.remove(&key)?;
            self.used -= loss.origin_cost + loss.item_cost;
            if new_origin {
                request.origins.push(loss.origin);
            }
            request.lost.push(IngestLoss {
                origin: index,
                count: loss.count,
            });
            losses.push((key, loss.count));
        }
        let mut calls = Vec::with_capacity(selected_calls.len());
        for (index, new_origin) in selected_calls {
            let mut pending = self.calls.pop_front()?;
            self.used -= pending.origin_cost + pending.item_cost;
            if new_origin {
                request.origins.push(pending.origin);
            }
            pending.wire.origin = index;
            request.calls.push(pending.wire);
            calls.push(pending.call);
        }
        let unattributed = std::mem::take(&mut self.unattributed);
        request.unattributed = unattributed;
        Some(Dispatch {
            request,
            calls,
            lost: losses,
            unattributed,
        })
    }

    /// Resolves a dispatch without retrying calls or possibly delivered losses.
    pub fn resolve(&mut self, dispatch: Dispatch, outcome: Outcome) {
        if outcome == Outcome::Accepted {
            return;
        }
        if outcome == Outcome::NotApplied {
            for (key, count) in dispatch.lost {
                self.lose(key, count);
            }
            self.lose_unattributed(dispatch.unattributed);
        }
        for call in dispatch.calls {
            self.lose(call.origin.build, 1);
        }
    }

    /// Returns pending calls and loss tallies for local folding after promotion.
    #[must_use]
    pub fn into_local(self) -> (Vec<Call>, Vec<(BuildKey, u64)>, u64) {
        (
            self.calls.into_iter().map(|pending| pending.call).collect(),
            self.lost
                .into_iter()
                .map(|(key, loss)| (key, loss.count))
                .collect(),
            self.unattributed,
        )
    }
}

fn key_origin(key: &BuildKey) -> IngestOrigin {
    IngestOrigin {
        principal: key.principal.to_string(),
        invocation_id: key.invocation.as_str().to_owned(),
        correlated_invocations_id: None,
        tool: None,
        target: None,
        mnemonic: None,
    }
}

fn wire_origin(origin: &Origin) -> IngestOrigin {
    let mut wire = key_origin(&origin.build);
    wire.correlated_invocations_id = origin.correlated.as_ref().map(|v| v.as_str().to_owned());
    wire.tool = origin.tool.as_ref().map(|v| v.as_str().to_owned());
    wire.target = origin.target.as_ref().map(|v| v.as_str().to_owned());
    wire.mnemonic = origin.mnemonic.as_ref().map(|v| v.as_str().to_owned());
    wire
}

fn wire_rpc(rpc: Rpc) -> IngestRpc {
    match rpc {
        Rpc::GetActionResult => IngestRpc::GetActionResult,
        Rpc::UpdateActionResult => IngestRpc::UpdateActionResult,
        Rpc::Execute => IngestRpc::Execute,
    }
}

fn wire_outcome(outcome: CallOutcome) -> IngestOutcome {
    match outcome {
        CallOutcome::Ok => IngestOutcome::Ok,
        CallOutcome::NotFound => IngestOutcome::NotFound,
        CallOutcome::Error => IngestOutcome::Error,
    }
}

struct CountingWriter(usize);

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_len(value: &impl serde::Serialize) -> usize {
    let mut writer = CountingWriter(0);
    let encoded = serde_json::to_writer(&mut writer, value);
    assert!(
        encoded.is_ok(),
        "typed ingest values and counting writer are infallible"
    );
    writer.0
}
