//! Applies a delta to the state it was diffed from. [`crate::diff`] is its inverse.

use gang_types::SaturatingInto;
use gang_wire::{Actions, ActionsDelta, Delta, Keyed, Named, State};

use crate::errors::ApplyError;

/// Patches `state` with `delta`. The delta must be diffed from a state with the same sequence
/// number; afterwards `state` equals the state the delta was diffed to.
///
/// # Errors
///
/// Returns [`ApplyError::BaseMismatch`] when `delta.base` is not `state.seq`. The state is left
/// unchanged in that case.
pub fn apply(state: &mut State, delta: &Delta) -> Result<(), ApplyError> {
    if delta.base != state.seq {
        return Err(ApplyError::BaseMismatch {
            state: state.seq,
            base: delta.base,
        });
    }
    if let Some(pass) = &delta.pass {
        state.pass.clone_from(pass);
    }
    if let Some(queue) = &delta.queue {
        state.queue.clone_from(queue);
    }
    if let Some(fleets) = &delta.fleets {
        apply_declared(&mut state.fleets, fleets);
    }
    if let Some(workers) = &delta.workers {
        apply_sorted(&mut state.workers, workers);
    }
    if let Some(instances) = &delta.instances {
        apply_sorted(&mut state.instances, instances);
    }
    if let Some(patch) = &delta.actions {
        apply_actions(state, patch);
    }
    state.events.extend(delta.events.iter().cloned());
    let limit: usize = state.events_limit.saturating_into();
    let excess = state.events.len().saturating_sub(limit);
    state.events.drain(..excess);
    if let Some(completed) = delta.completed {
        state.completed = completed;
    }
    if let Some(epoch) = delta.principals_epoch {
        state.principals_epoch = epoch;
    }
    state.seq = delta.seq;
    state.update_time = delta.update_time;
    Ok(())
}

/// Applies an actions patch. A patch to a state with no actions starts from `Actions::default()`,
/// which is the case of the first sample.
fn apply_actions(state: &mut State, patch: &ActionsDelta) {
    let actions = state.actions.get_or_insert_with(Actions::default);
    if let Some(sample_time) = patch.sample_time {
        actions.sample_time = sample_time;
    }
    if let Some(stale_after) = patch.stale_after {
        actions.stale_after = stale_after;
    }
    if let Some(running) = &patch.running {
        apply_sorted(&mut actions.running, running);
    }
    if let Some(queued) = &patch.queued {
        actions.queued.clone_from(queued);
    }
    if let Some(truncated) = patch.truncated {
        actions.truncated = truncated;
    }
}

/// Applies a keyed change to a list ordered by name. Remove names and upsert rows must also be
/// sorted by name, as they are when produced by `diff`.
fn apply_sorted<T: Named + Clone>(list: &mut Vec<T>, change: &Keyed<T>) {
    let mut remove = change.remove.iter().peekable();
    list.retain(|row| {
        while let Some(wanted) = remove.peek() {
            if wanted.as_str() >= row.name() {
                break;
            }
            remove.next();
        }
        remove
            .peek()
            .is_none_or(|wanted| wanted.as_str() != row.name())
    });

    let original_len = list.len();
    let mut index = 0;
    for row in &change.upsert {
        while index < original_len && list[index].name() < row.name() {
            index += 1;
        }
        if index < original_len && list[index].name() == row.name() {
            list[index].clone_from(row);
            index += 1;
        } else {
            list.push(row.clone());
        }
    }
    if list.len() != original_len {
        list.sort_by(|a, b| a.name().cmp(b.name()));
    }
}

/// Applies a keyed change to a list in declaration order: removes the vanished names, then
/// replaces the upserted rows found by position. New rows are appended.
fn apply_declared<T: Named + Clone>(list: &mut Vec<T>, change: &Keyed<T>) {
    for wanted in &change.remove {
        if let Some(index) = list.iter().position(|row| row.name() == wanted.as_str()) {
            list.remove(index);
        }
    }
    for row in &change.upsert {
        match list.iter().position(|known| known.name() == row.name()) {
            Some(index) => list[index].clone_from(row),
            None => list.push(row.clone()),
        }
    }
}
