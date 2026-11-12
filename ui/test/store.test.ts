import assert from "node:assert/strict";
import { test } from "node:test";
import { actionsStale, applyDelta, mergeKeyed, tickContinues } from "../src/store.ts";
import { eventKind, idOf, ms, seconds } from "../src/wire-parse.ts";
import type { Delta, ScalingEvent, State } from "../src/wire.ts";
import { iso, load, snapshotOf } from "./sse.ts";

const live = load("live");
const snapshot = snapshotOf(live);
const t0 = ms(snapshot.updateTime);

const apply = (state: State, delta: Delta): State => {
  const applied = applyDelta(state, delta);
  assert.equal(applied.kind, "applied", `delta ${delta.seq} on ${state.seq}`);
  return applied.kind === "applied" ? applied.state : state;
};

const ascending = (keys: readonly string[]): boolean => keys.every((k, i) => i === 0 || (keys[i - 1] ?? "") < k);

const seqOf = (e: ScalingEvent): number => Number(idOf(e.name));
const lastSeq = Number(idOf(snapshot.events.at(-1)?.name ?? "events/0"));

/** Folds the recording, checking each frame against `check`. */
const fold = (check: (state: State) => void): State => {
  let state = snapshot;
  for (const { event } of live.slice(1)) {
    if (event.event === "delta") {
      state = apply(state, event.data);
      check(state);
    }
    if (event.event === "tick") assert.ok(tickContinues(state, event.data), `tick ${event.data.seq} on ${state.seq}`);
  }
  return state;
};

test("the recorded stream folds without a resync and ends at the last seq", () => {
  const deltas = live.flatMap(({ event }) => (event.event === "delta" ? [event.data] : []));
  assert.ok(deltas.length > 0);
  assert.equal(fold(() => {}).seq, deltas.at(-1)?.seq);
});

test("every folded frame keeps the wire invariants", () => {
  const fleetOrder = snapshot.fleets.map((f) => f.name);
  fold((state) => {
    const queued = state.fleets.reduce((n, f) => n + f.demand.queued, 0);
    assert.equal(queued + state.queue.unplaceable, state.queue.total);
    assert.ok(ascending(state.workers.map((w) => w.name)), "workers by name");
    assert.ok(ascending(state.instances.map((i) => i.name)), "instances by name");
    assert.ok(ascending(state.actions?.running.map((a) => a.name) ?? []), "running by name");
    assert.deepEqual(
      state.fleets.map((f) => f.name),
      fleetOrder,
    );
  });
});

test("the recorded worker events reach the store in ledger order", () => {
  const final = fold(() => {});
  const workers = final.events.filter((e) => "workerRegistered" in e || "workerDeregistered" in e);
  assert.deepEqual(
    workers.map((e) => [seqOf(e), eventKind(e)]),
    [
      [1022, "workerRegistered"],
      [1023, "workerDeregistered"],
    ],
  );
  assert.ok(final.events.every((e, i) => i === 0 || seqOf(final.events[i - 1] ?? e) < seqOf(e)), "events by seq");
});

test("a worker event in a delta is appended once, after the held events", () => {
  const seq = lastSeq + 1;
  const joined: ScalingEvent = {
    name: `events/${seq}`,
    createTime: snapshot.updateTime,
    fleet: "fleets/arm64-spot",
    workerRegistered: { worker: "workers/i-0f00000000000001-0194d5a2-7b3c-7f10-8a4e-1c2d3e4f5a6b" },
  };
  const next = apply(snapshot, after(snapshot, { events: [joined] }));
  assert.equal(next.events.length, snapshot.events.length + 1);
  assert.deepEqual(next.events.at(-1), joined);
});

/** The delta right after `state`, two seconds later, with `parts`. */
const after = (state: State, parts: Omit<Delta, "seq" | "base" | "updateTime"> = {}): Delta => ({
  seq: state.seq + 1,
  base: state.seq,
  updateTime: iso(ms(state.updateTime) + 2000),
  ...parts,
});

test("a delta on another base asks for a resync", () => {
  assert.deepEqual(applyDelta(snapshot, { ...after(snapshot), base: snapshot.seq + 1 }), { kind: "resync" });
});

test("an empty delta advances seq and the frame clock and keeps every row's identity", () => {
  const next = apply(snapshot, after(snapshot));
  assert.equal(next.seq, snapshot.seq + 1);
  assert.equal(ms(next.updateTime), t0 + 2000);
  assert.equal(next.fleets, snapshot.fleets);
  assert.equal(next.workers, snapshot.workers);
  assert.equal(next.instances, snapshot.instances);
  assert.equal(next.actions, snapshot.actions);
  assert.equal(next.events, snapshot.events);
});

test("sorted merges remove, replace in place and insert in key order", () => {
  const rows = [{ k: "a", v: 1 }, { k: "c", v: 1 }, { k: "e", v: 1 }];
  const merged = mergeKeyed(rows, { upsert: [{ k: "d", v: 2 }, { k: "c", v: 2 }], remove: ["a"] }, (r) => r.k, "sorted");
  assert.deepEqual(merged, [{ k: "c", v: 2 }, { k: "d", v: 2 }, { k: "e", v: 1 }]);
  assert.equal(mergeKeyed(rows, undefined, (r) => r.k, "sorted"), rows);
});

test("declared merges replace in place and append new keys", () => {
  const rows = [{ k: "z", v: 1 }, { k: "a", v: 1 }];
  const merged = mergeKeyed(rows, { upsert: [{ k: "m", v: 2 }, { k: "z", v: 2 }] }, (r) => r.k, "declared");
  assert.deepEqual(merged, [{ k: "z", v: 2 }, { k: "a", v: 1 }, { k: "m", v: 2 }]);
});

test("keyed deltas remove rows by their names", () => {
  const worker = snapshot.workers[0];
  assert.ok(worker);
  const next = apply(snapshot, after(snapshot, { workers: { remove: [worker.name] } }));
  assert.equal(next.workers.length, snapshot.workers.length - 1);
  assert.ok(!next.workers.some((w) => w.name === worker.name));
});

test("the first action sample can arrive in a delta, and needs its threshold", () => {
  const { actions: _actions, ...withoutActions } = snapshot;
  const empty: State = withoutActions;
  const sample = snapshot.actions;
  assert.ok(sample);
  const created = apply(
    empty,
    after(empty, {
      actions: { sampleTime: sample.sampleTime, staleAfter: sample.staleAfter, running: { upsert: sample.running }, queued: sample.queued },
    }),
  );
  assert.equal(created.actions?.running.length, sample.running.length);
  assert.equal(created.actions?.staleAfter, sample.staleAfter);
  assert.deepEqual(applyDelta(empty, after(empty, { actions: { sampleTime: sample.sampleTime } })), { kind: "resync" });
});

test("queued actions are replaced whole and the threshold stays", () => {
  const queued = snapshot.actions?.queued.slice(0, 2) ?? [];
  const next = apply(snapshot, after(snapshot, { actions: { sampleTime: snapshot.updateTime, queued } }));
  assert.deepEqual(next.actions?.queued, queued);
  assert.equal(next.actions?.staleAfter, snapshot.actions?.staleAfter);
});

test("events are appended and trimmed to the frame's eventsLimit", () => {
  const e: ScalingEvent = {
    name: `events/${lastSeq + 1}`,
    createTime: snapshot.updateTime,
    fleet: "fleets/gpu",
    cause: "DEMAND",
    scaledOut: { from: 0, to: 1 },
  };
  const next = apply(snapshot, after(snapshot, { events: [e] }));
  assert.deepEqual(next.events, [...snapshot.events, e]);
  const limited: State = { ...snapshot, eventsLimit: 3 };
  assert.deepEqual(apply(limited, after(limited, { events: [e] })).events, [...snapshot.events.slice(-2), e]);
});

test("the action sample is stale past the frame's own threshold", () => {
  const lagging = snapshotOf(load("lagging"));
  assert.ok(actionsStale(lagging.actions, ms(lagging.updateTime)));
  assert.ok(!actionsStale(snapshot.actions, t0));
  const actions = snapshot.actions;
  assert.ok(actions);
  const sampled = ms(actions.sampleTime);
  const threshold = seconds(actions.staleAfter) * 1000;
  assert.ok(!actionsStale(actions, sampled + threshold));
  assert.ok(actionsStale(actions, sampled + threshold + 1));
  assert.ok(!actionsStale(undefined, Number.MAX_SAFE_INTEGER));
});

test("a tick at the current seq or ahead of it continues the stream, one behind does not", () => {
  const tick = (seq: number) => ({ seq, time: snapshot.updateTime });
  assert.equal(tickContinues(snapshot, tick(snapshot.seq)), true);
  assert.equal(tickContinues(snapshot, tick(snapshot.seq + 1)), true);
  assert.equal(tickContinues(snapshot, tick(snapshot.seq - 1)), false);
});
