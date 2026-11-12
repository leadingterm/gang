//! The delta between two consecutive states. [`crate::apply`] is its inverse.

use gang_wire::{Actions, ActionsDelta, Delta, Fleet, Keyed, Named, State};
use itertools::{EitherOrBoth, Itertools};

use crate::events::newer;

/// Returns the delta that moves `prev` to `cur`, or `None` when nothing but `seq` and
/// `update_time` differs. The delta's `base` is `prev.seq`; applying it to `prev` yields `cur`.
#[must_use]
pub fn diff(prev: &State, cur: &State) -> Option<Delta> {
    let delta = Delta {
        seq: cur.seq,
        base: prev.seq,
        update_time: cur.update_time,
        pass: changed(&prev.pass, &cur.pass).cloned(),
        queue: changed(&prev.queue, &cur.queue).cloned(),
        fleets: fleets(&prev.fleets, &cur.fleets),
        workers: keyed(&prev.workers, &cur.workers),
        instances: keyed(&prev.instances, &cur.instances),
        actions: actions(prev.actions.as_ref(), cur.actions.as_ref()),
        // The events of `cur` after the last one `prev` holds; every event of `cur` when none
        // of `prev`'s events is still in `cur`.
        events: newer(&prev.events, &cur.events)
            .unwrap_or(&cur.events)
            .to_vec(),
        completed: changed(&prev.completed, &cur.completed).copied(),
        principals_epoch: changed(&prev.principals_epoch, &cur.principals_epoch).copied(),
    };
    (!delta.is_empty()).then_some(delta)
}

/// Returns `cur` when it differs from `prev`.
fn changed<'a, T: PartialEq>(prev: &T, cur: &'a T) -> Option<&'a T> {
    (prev != cur).then_some(cur)
}

/// Diffs the fleets by name. Fleets are kept in declaration order rather than sorted by name,
/// so a sorted merge does not apply: each fleet is looked up by its name instead.
fn fleets(prev: &[Fleet], cur: &[Fleet]) -> Option<Keyed<Fleet>> {
    let upsert: Vec<Fleet> = cur
        .iter()
        .filter(|fleet| !prev.iter().any(|known| known == *fleet))
        .cloned()
        .collect();
    let remove: Vec<String> = prev
        .iter()
        .filter(|known| !cur.iter().any(|fleet| fleet.name() == known.name()))
        .map(|known| known.name().to_owned())
        .collect();
    (!upsert.is_empty() || !remove.is_empty()).then_some(Keyed { upsert, remove })
}

/// Diffs two lists that are each sorted by name with a sorted merge: rows that are new or
/// changed are upserted, names that vanished are removed.
fn keyed<T: Named + Clone + PartialEq>(prev: &[T], cur: &[T]) -> Option<Keyed<T>> {
    let mut upsert = Vec::new();
    let mut remove = Vec::new();
    for pair in prev
        .iter()
        .merge_join_by(cur, |stale, fresh| stale.name().cmp(fresh.name()))
    {
        match pair {
            EitherOrBoth::Left(stale) => remove.push(stale.name().to_owned()),
            EitherOrBoth::Right(fresh) => upsert.push(fresh.clone()),
            EitherOrBoth::Both(stale, fresh) => {
                if stale != fresh {
                    upsert.push(fresh.clone());
                }
            }
        }
    }
    (!upsert.is_empty() || !remove.is_empty()).then_some(Keyed { upsert, remove })
}

/// Diffs the sampled actions. A first sample carries every field; later samples carry only what
/// changed, and `stale_after` is never repeated.
fn actions(prev: Option<&Actions>, cur: Option<&Actions>) -> Option<ActionsDelta> {
    let cur = cur?;
    let Some(prev) = prev else {
        return Some(ActionsDelta {
            sample_time: Some(cur.sample_time),
            stale_after: Some(cur.stale_after),
            running: Some(Keyed {
                upsert: cur.running.clone(),
                remove: Vec::new(),
            }),
            queued: Some(cur.queued.clone()),
            truncated: Some(cur.truncated),
        });
    };
    let patch = ActionsDelta {
        sample_time: changed(&prev.sample_time, &cur.sample_time).copied(),
        stale_after: None,
        running: keyed(&prev.running, &cur.running),
        queued: changed(&prev.queued, &cur.queued).cloned(),
        truncated: changed(&prev.truncated, &cur.truncated).copied(),
    };
    (!patch.is_empty()).then_some(patch)
}
