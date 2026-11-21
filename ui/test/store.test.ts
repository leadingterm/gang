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

test("scheduler availability deltas replace both true and false and preserve omitted values", () => {
  const failed = apply(snapshot, after(snapshot, { schedulerKnown: false }));
  assert.equal(failed.schedulerKnown, false);
  const recovered = apply(failed, after(failed, { schedulerKnown: true }));
  assert.equal(recovered.schedulerKnown, true);
  assert.equal(apply(recovered, after(recovered)).schedulerKnown, true);
});

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
    fleet: "fleets/build-worker-aarch64-graviton4-spot-xl",
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

test("a delta applies its recent increment", () => {
  const sample = (step: number, completed: number) => ({
    start: iso(step * 10000), completed, running: 1, queued: 2, passes: 1, skipped: 0, lookups: 0, hits: 0, uploadedBytes: 0, downloadedBytes: 0, evictedBytes: 0,
  });
  const recent = { window: "30s", step: "10s", samples: [sample(0, 1), sample(1, 2), sample(2, 3)] };
  const state = { ...snapshot, recent };
  const increment = { ...recent, samples: [sample(2, 30), sample(3, 4)] };
  assert.deepEqual(apply(state, after(state, { recent: increment })).recent, {
    ...recent, samples: [sample(1, 2), sample(2, 30), sample(3, 4)],
  });
});

test("cache deltas replace the whole part and omission preserves it", () => {
  const state = { ...snapshot, cache: { stores: [{ store: "CAS" as const, usedBytes: 25, budgetBytes: 100 }] } };
  const unchanged = apply(state, after(state));
  assert.equal(unchanged.cache, state.cache);
  const replaced = apply(unchanged, after(unchanged, { cache: { bucket: { bytes: 12, objects: 3, measureTime: iso(0) } } }));
  assert.deepEqual(replaced.cache, { bucket: { bytes: 12, objects: 3, measureTime: iso(0) } });
  assert.deepEqual(apply(replaced, after(replaced, { cache: {} })).cache, {});
});

test("connection deltas replace the whole part and omission preserves it", () => {
  const state = { ...snapshot, connections: { active: 4, limit: 10, refused: 2 } };
  const unchanged = apply(state, after(state));
  assert.equal(unchanged.connections, state.connections);
  const connections = { active: 10, limit: 10, refused: 3 };
  const replaced = apply(unchanged, after(unchanged, { connections }));
  assert.equal(replaced.connections, connections);
  assert.equal(apply(replaced, after(replaced)).connections, connections);
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

test("build deltas upsert and remove by name", () => {
  const first = snapshot.builds?.[0];
  assert.ok(first);
  const added = { ...first, name: "builds/zzzz" };
  const next = apply(snapshot, after(snapshot, { builds: { remove: [first.name], upsert: [added] } }));
  assert.ok(!next.builds?.some((b) => b.name === first.name));
  assert.deepEqual(next.builds?.find((b) => b.name === added.name), added);
});

test("a build state-only delta changes its row identity but keeps untouched rows", () => {
  const first = snapshot.builds?.[0];
  const second = snapshot.builds?.[1];
  assert.ok(first);
  assert.ok(second);
  const changed = { ...first, state: "IDLE" as const };
  const next = apply(snapshot, after(snapshot, { builds: { upsert: [changed] } }));
  assert.notEqual(next.builds?.[0], first);
  assert.equal(next.builds?.[0], changed);
  assert.equal(next.builds?.[1], second);
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

test("status replacement and expiry do not alter audit events or unknown payloads", () => {
  const unknown = { name: "events/9999", createTime: snapshot.updateTime, fleet: "fleets/a", futureKind: { value: 1 } } as unknown as ScalingEvent;
  const status: ScalingEvent = { name: "awsEvents/a", createTime: snapshot.updateTime, fleet: "fleets/a", aws: { event: {
    id: "a", at: snapshot.updateTime, kind: { type: "capacityUnavailable", reason: "capacity" },
  } } };
  const withAudit = apply(snapshot, after(snapshot, { events: [unknown] }));
  const withStatus = apply(withAudit, after(withAudit, { statusEvents: [status] }));
  assert.equal(withStatus.events, withAudit.events);
  assert.deepEqual(withStatus.events.at(-1), unknown);
  assert.deepEqual(withStatus.statusEvents, [status]);
  const unchanged = apply(withStatus, after(withStatus));
  assert.equal(unchanged.statusEvents, withStatus.statusEvents);
  const expired = apply(unchanged, after(unchanged, { statusEvents: [] }));
  assert.deepEqual(expired.statusEvents, []);
  assert.equal(expired.events, withAudit.events);
});
