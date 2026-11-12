// The keyed state store: a snapshot replaced by deltas. Pure functions over immutable frames, so a
// delta changes only the rows it names and every untouched row keeps its identity.
import { ms, seconds, type UnixMs } from "./wire-parse.ts";
import type { Actions, ActionsDelta, Delta, Keyed, State, Tick } from "./wire.ts";

/** How a delta or tick relates to the current state. */
export type Applied =
  | { readonly kind: "applied"; readonly state: State }
  /** The frame does not continue the current sequence: reconnect for a fresh snapshot. */
  | { readonly kind: "resync" };

type Order = "sorted" | "declared";

/**
 * Removes, then upserts by key. `sorted` lists keep ascending key order (workers, instances and
 * running actions are ordered by name); `declared` lists replace in place and append new keys.
 */
export const mergeKeyed = <T>(
  rows: T[],
  change: Keyed<T> | undefined,
  key: (row: T) => string,
  order: Order,
): T[] => {
  if (!change) return rows;
  const removed = new Set(change.remove ?? []);
  const upserts = new Map((change.upsert ?? []).map((row) => [key(row), row]));
  const kept: T[] = [];
  for (const row of rows) {
    const k = key(row);
    if (removed.has(k)) continue;
    const next = upserts.get(k);
    kept.push(next ?? row);
    upserts.delete(k);
  }
  if (upserts.size === 0) return kept;
  if (order === "declared") return [...kept, ...upserts.values()];
  return [...kept, ...upserts.values()].sort((a, b) => compare(key(a), key(b)));
};

const compare = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);

const byName = (row: { readonly name: string }): string => row.name;

const mergeActions = (actions: Actions | undefined, delta: ActionsDelta | undefined): Actions | undefined => {
  if (!delta) return actions;
  if (!actions) {
    if (delta.sampleTime === undefined || delta.staleAfter === undefined) return undefined;
    return {
      sampleTime: delta.sampleTime,
      staleAfter: delta.staleAfter,
      running: delta.running?.upsert ?? [],
      queued: delta.queued ?? [],
      truncated: delta.truncated ?? { running: 0, queued: 0 },
    };
  }
  return {
    sampleTime: delta.sampleTime ?? actions.sampleTime,
    staleAfter: actions.staleAfter,
    running: mergeKeyed(actions.running, delta.running, byName, "sorted"),
    queued: delta.queued ?? actions.queued,
    truncated: delta.truncated ?? actions.truncated,
  };
};

/** Applies a delta whose `base` is the current `seq`; any other base asks for a resync. */
export const applyDelta = (state: State, delta: Delta): Applied => {
  if (delta.base !== state.seq) return { kind: "resync" };
  const actions = mergeActions(state.actions, delta.actions);
  if (delta.actions && !actions) return { kind: "resync" };
  const { actions: _previous, ...rest } = state;
  return {
    kind: "applied",
    state: {
      ...rest,
      ...(actions === undefined ? {} : { actions }),
      seq: delta.seq,
      updateTime: delta.updateTime,
      pass: delta.pass ?? state.pass,
      queue: delta.queue ?? state.queue,
      fleets: mergeKeyed(state.fleets, delta.fleets, byName, "declared"),
      workers: mergeKeyed(state.workers, delta.workers, byName, "sorted"),
      instances: mergeKeyed(state.instances, delta.instances, byName, "sorted"),
      events: delta.events?.length ? [...state.events, ...delta.events].slice(-state.eventsLimit) : state.events,
      completed: delta.completed ?? state.completed,
      principalsEpoch: delta.principalsEpoch ?? state.principalsEpoch,
    },
  };
};

/**
 * Seq advances only with a delta and a tick carries the current seq, so a tick may run ahead of a
 * delta still queued for this viewer; only a tick behind the state (the sequence went back) breaks it.
 */
export const tickContinues = (state: State, tick: Tick): boolean => tick.seq >= state.seq;

/** The action sample is stale when the server clock is past its threshold (the frame's own). */
export const actionsStale = (actions: Actions | undefined, serverNow: UnixMs): boolean =>
  actions !== undefined && serverNow - ms(actions.sampleTime) > seconds(actions.staleAfter) * 1000;
