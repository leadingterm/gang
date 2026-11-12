import assert from "node:assert/strict";
import { test } from "node:test";
import { donePerMinute, emptyHistory, observe, PASS_BARS, passBars, streamLost, type History } from "../src/history.ts";
import { ms, seconds } from "../src/wire-parse.ts";
import type { Phase, State } from "../src/wire.ts";
import { iso, load, snapshotOf } from "./sse.ts";

const snapshot = snapshotOf(load("live"));
const t0 = ms(snapshot.updateTime);
const worker = snapshot.workers[0];
assert.ok(worker);

const withWorker = (phase: Phase, since: number): State => ({
  ...snapshot,
  workers: [{ ...worker, phase, phaseTime: iso(since) }],
});

test("a phase change closes the open segment where the new phase began", () => {
  const first = observe(emptyHistory(t0), withWorker("BUSY", t0 - 1000), t0);
  assert.deepEqual(first.workers.get(worker.name)?.segments, [{ phase: "BUSY", from: t0 - 1000, to: null }]);
  const same = observe(first, withWorker("BUSY", t0 - 1000), t0 + 2000);
  assert.equal(same.workers.get(worker.name), first.workers.get(worker.name));
  const changed = observe(same, withWorker("IDLE", t0 + 3000), t0 + 4000);
  assert.deepEqual(changed.workers.get(worker.name)?.segments, [
    { phase: "BUSY", from: t0 - 1000, to: t0 + 3000 },
    { phase: "IDLE", from: t0 + 3000, to: null },
  ]);
});

test("a skipped pass opens an outage from the last good pass, an ok pass closes it", () => {
  const skipped: State = {
    ...snapshot,
    pass: {
      state: "SKIPPED",
      skipReason: "PROVISIONER_UNAVAILABLE",
      interval: "2s",
      applied: 0,
      failed: 0,
      lastOkTime: iso(t0 - 6000),
      skipped: 3,
    },
  };
  const open = observe(emptyHistory(t0), skipped, t0);
  assert.deepEqual(open.outages, [{ from: t0 - 6000, to: null }]);
  const closed = observe(open, snapshot, t0 + 8000);
  assert.deepEqual(closed.outages, [{ from: t0 - 6000, to: t0 + 8000 }]);
});

test("a skipped pass that never succeeded opens its outage at the frame", () => {
  const skipped: State = { ...snapshot, pass: { state: "SKIPPED", interval: "2s", applied: 0, failed: 0, skipped: 1 } };
  assert.deepEqual(observe(emptyHistory(t0), skipped, t0).outages, [{ from: t0, to: null }]);
});

test("done per minute needs a minute of samples and restarts with the counter", () => {
  const at = (now: number, completed: number) => (h: History) => observe(h, { ...snapshot, completed }, now);
  const h1 = at(t0, 1000)(emptyHistory(t0));
  assert.equal(donePerMinute(h1.samples), null);
  const h2 = at(t0 + 60000, 1043)(h1);
  assert.equal(donePerMinute(h2.samples), 43);
  const h3 = at(t0 + 62000, 5)(h2);
  assert.equal(donePerMinute(h3.samples), null);
});

test("a lost stream opens a gap that the next frame closes", () => {
  const seen = observe(emptyHistory(t0), snapshot, t0);
  const lost = streamLost(seen, t0);
  assert.deepEqual(lost.gaps, [{ from: t0, to: null }]);
  assert.equal(streamLost(lost, t0 + 5000), lost);
  const back = observe(lost, snapshot, t0 + 20000);
  assert.deepEqual(back.gaps, [{ from: t0, to: t0 + 20000 }]);
});

test("pass bars: unobserved before the first frame and across stream gaps, skipped over outages", () => {
  const pass = snapshot.pass;
  const interval = seconds(pass.interval) * 1000;
  const start = t0 - 40 * interval;
  const seen = observe(emptyHistory(start), snapshot, start);
  const back = observe(streamLost(seen, t0 - 30 * interval), snapshot, t0 - 20 * interval);
  const bars = passBars(pass, back, t0);
  assert.equal(bars.length, PASS_BARS);
  const index = (at: number): number => PASS_BARS - 1 - Math.round((t0 - at) / interval);
  assert.equal(bars[index(start - interval)], "unobserved");
  assert.equal(bars[index(t0 - 35 * interval)], "ok");
  assert.equal(bars[index(t0 - 25 * interval)], "unobserved");
  assert.equal(bars[index(t0 - 10 * interval)], "ok");
  const skipped: State = {
    ...snapshot,
    pass: {
      state: "SKIPPED",
      skipReason: "SCHEDULER_UNAVAILABLE",
      interval: pass.interval,
      applied: 0,
      failed: 0,
      lastOkTime: iso(t0 - 5 * interval),
      skipped: 5,
    },
  };
  assert.equal(passBars(pass, observe(back, skipped, t0), t0).at(-1), "skipped");
});

test("pass bars are half-open: outages that only touch a bar do not skip it", () => {
  const interval = seconds(snapshot.pass.interval) * 1000;
  const now = t0;
  // Bar `ago` covers [now - (ago + 1) * interval, now - ago * interval).
  const skippedAgo = (outages: History["outages"]): number[] =>
    passBars(snapshot.pass, { ...emptyHistory(now - 200 * interval), outages }, now)
      .flatMap((bar, i) => (bar === "skipped" ? [PASS_BARS - 1 - i] : []))
      .sort((a, b) => a - b);
  // An outage ending exactly where bar 3 starts only touches it.
  assert.deepEqual(skippedAgo([{ from: now - 6 * interval, to: now - 4 * interval }]), [4, 5]);
  // An outage starting exactly where bar 3 ends only touches it.
  assert.deepEqual(skippedAgo([{ from: now - 3 * interval, to: now - 2 * interval }]), [2]);
  // One millisecond of overlap at either edge does skip the bar.
  assert.deepEqual(skippedAgo([{ from: now - 6 * interval, to: now - 4 * interval + 1 }]), [3, 4, 5]);
  assert.deepEqual(skippedAgo([{ from: now - 3 * interval - 1, to: now - 2 * interval }]), [2, 3]);
});
