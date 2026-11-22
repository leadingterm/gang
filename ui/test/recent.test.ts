import assert from "node:assert/strict";
import { test } from "node:test";
import { applyRecent, bytesSeries, cacheWindow, donePerMinute, doneSeries, evictedSeries, inServiceSeries, passBars, queuedSeries, runningSeries, schedulerCurrent, schedulerKnown, slots, transferred, unrecordedSlots } from "../src/recent.ts";
import type { Recent, Sample } from "../src/wire.ts";
import { iso, load, snapshotOf } from "./sse.ts";

const sample = (step: number, completed = 0): Sample => ({
  start: iso(step * 10000), completed, running: 0, queued: 0, passes: 1, skipped: 0, lookups: 0, hits: 0, uploadedBytes: 0, downloadedBytes: 0, evictedBytes: 0,
});
const recent = (samples: Sample[], window = "60s"): Recent => ({ window, step: "10s", samples });

test("increments replace from their first start and trim to the window", () => {
  const held = recent([sample(0), sample(1, 1), sample(2, 2)], "30s");
  const increment = recent([sample(2, 20), sample(3, 3), sample(4, 4)], "30s");
  assert.deepEqual(applyRecent(held, increment), increment);
  const replacement = recent([sample(1, 10), sample(2, 20)], "30s");
  assert.deepEqual(applyRecent(held, replacement), recent([sample(0), sample(1, 10), sample(2, 20)], "30s"));
  assert.equal(applyRecent(held, undefined), held);
});

test("sliding interval charts hide the current bucket while window totals stay live", () => {
  const full = {
    ...sample(0, 2), running: 3, queued: 2, lookups: 10, hits: 6,
    downloadedBytes: 70, uploadedBytes: 30, evictedBytes: 5,
  };
  const empty = { ...sample(1), running: 7, queued: 4 };
  const growing = {
    ...empty, completed: 1, lookups: 4, hits: 3,
    downloadedBytes: 11, uploadedBytes: 13, evictedBytes: 2,
  };
  let r = applyRecent(recent([], "50s"), recent([full], "50s"));
  const check = (
    expected: {
      lookups: (number | null)[]; hits: (number | null)[];
      downloaded: (number | null)[]; uploaded: (number | null)[];
      evicted: (number | null)[]; done: (number | null)[];
      totalLookups: number; totalHits: number; totalDownloaded: number; totalUploaded: number;
    },
  ) => {
    const cache = cacheWindow(r);
    assert.deepEqual(cache.lookupsPerStep, expected.lookups);
    assert.deepEqual(cache.hitsPerStep, expected.hits);
    assert.deepEqual(bytesSeries(r, "downloadedBytes"), expected.downloaded);
    assert.deepEqual(bytesSeries(r, "uploadedBytes"), expected.uploaded);
    assert.deepEqual(evictedSeries(r), expected.evicted);
    assert.deepEqual(doneSeries(r), expected.done);
    assert.equal(cache.lookups, expected.totalLookups);
    assert.equal(cache.hits, expected.totalHits);
    assert.equal(cache.rate, expected.totalHits / expected.totalLookups);
    assert.deepEqual(transferred(r), { downloaded: expected.totalDownloaded, uploaded: expected.totalUploaded });
  };

  check({
    lookups: [null, null, null, null, null], hits: [null, null, null, null, null],
    downloaded: [null, null, null, null, null], uploaded: [null, null, null, null, null],
    evicted: [null, null, null, null, null], done: [null, null, null, null, null],
    totalLookups: 10, totalHits: 6, totalDownloaded: 70, totalUploaded: 30,
  });
  r = applyRecent(r, recent([empty], "50s"));
  check({
    lookups: [null, null, null, 10, null], hits: [null, null, null, 6, null],
    downloaded: [null, null, null, 70, null], uploaded: [null, null, null, 30, null],
    evicted: [null, null, null, 5, null], done: [null, null, null, 12, null],
    totalLookups: 10, totalHits: 6, totalDownloaded: 70, totalUploaded: 30,
  });
  assert.equal(runningSeries(r, 7).at(-1), 7);
  assert.equal(queuedSeries(r, 4).at(-1), 4);

  r = applyRecent(r, recent([growing], "50s"));
  check({
    lookups: [null, null, null, 10, null], hits: [null, null, null, 6, null],
    downloaded: [null, null, null, 70, null], uploaded: [null, null, null, 30, null],
    evicted: [null, null, null, 5, null], done: [null, null, null, 12, null],
    totalLookups: 14, totalHits: 9, totalDownloaded: 81, totalUploaded: 43,
  });
  assert.equal(runningSeries(r, 7).at(-1), 7);
  assert.equal(queuedSeries(r, 4).at(-1), 4);

  r = applyRecent(r, recent([{ ...sample(2), running: 7, queued: 4 }], "50s"));
  check({
    lookups: [null, null, 10, 4, null], hits: [null, null, 6, 3, null],
    downloaded: [null, null, 70, 11, null], uploaded: [null, null, 30, 13, null],
    evicted: [null, null, 5, 2, null], done: [null, null, 12, 6, null],
    totalLookups: 14, totalHits: 9, totalDownloaded: 81, totalUploaded: 43,
  });
  r = applyRecent(r, recent([{ ...sample(3), running: 7, queued: 4 }], "50s"));
  check({
    lookups: [null, 10, 4, 0, null], hits: [null, 6, 3, 0, null],
    downloaded: [null, 70, 11, 0, null], uploaded: [null, 30, 13, 0, null],
    evicted: [null, 5, 2, 0, null], done: [null, 12, 6, 0, null],
    totalLookups: 14, totalHits: 9, totalDownloaded: 81, totalUploaded: 43,
  });
});

test("live scheduler gauges retain known zero and leave unknown current observations null", () => {
  assert.deepEqual(runningSeries(recent([], "30s"), 7), [null, null, null]);
  assert.deepEqual(queuedSeries(recent([], "30s"), 4), [null, null, null]);
  let r = applyRecent(recent([], "30s"), recent([{ ...sample(0), running: 7, queued: 4 }], "30s"));
  assert.deepEqual(runningSeries(r, 7), [null, null, 7]);
  assert.deepEqual(queuedSeries(r, 4), [null, null, 4]);
  r = applyRecent(r, recent([sample(1)], "30s"));
  assert.deepEqual(runningSeries(r, 0), [null, 7, 0]);
  assert.deepEqual(queuedSeries(r, 0), [null, 4, 0]);
  r = applyRecent(r, recent([{ ...sample(2), schedulerKnown: false, running: 0, queued: 0 }], "30s"));
  assert.deepEqual(runningSeries(r, 7), [7, 0, 7]);
  assert.deepEqual(queuedSeries(r, 4), [4, 0, 4]);
  assert.deepEqual(runningSeries(r, null), [7, 0, null]);
  assert.deepEqual(queuedSeries(r, null), [4, 0, null]);
  r = applyRecent(r, recent([{ ...sample(3), passes: 0 }], "30s"));
  assert.deepEqual(runningSeries(r, 7), [0, null, 7]);
  assert.deepEqual(queuedSeries(r, 4), [0, null, 4]);
  assert.deepEqual(runningSeries(r, null), [0, null, null]);
  assert.deepEqual(queuedSeries(r, null), [0, null, null]);
});

test("in-service history sums every fleet including the current bucket without inventing zeroes", () => {
  const fleet = snapshotOf(load("live")).fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const a = { ...fleet, name: "fleets/a" };
  const b = { ...fleet, name: "fleets/b" };
  const observed = (name: string, inService: number) => ({
    name, instances: { pending: 0, inService, terminating: 0, warm: 0 }, desired: 0, max: 10,
  });
  const r = recent([
    { ...sample(0), fleets: [observed(a.name, 2), observed(b.name, 3)] },
    { ...sample(1), fleets: [observed(a.name, 0), observed(b.name, 0)] },
    { ...sample(2), fleets: [observed(a.name, 4)] },
    sample(3),
    { ...sample(4), schedulerKnown: false, fleets: [observed(a.name, 6), observed(b.name, 7)] },
  ], "60s");
  assert.deepEqual(inServiceSeries(r, [a, b]), [null, 5, 0, null, null, 13]);
  assert.deepEqual(inServiceSeries(r, []), [null, null, null, null, null, null]);
});

test("slots mark leading steps before the first sample as unrecorded", () => {
  const r = recent([sample(4), sample(5)], "50s");
  assert.deepEqual(slots(r), [null, null, null, sample(4), sample(5)]);
  assert.equal(unrecordedSlots(r), 3);
  assert.equal(unrecordedSlots(recent([sample(0), sample(1)], "20s")), 0);
});

test("done per minute uses complete steps and scales shorter observations", () => {
  const all = recent([1, 2, 3, 4, 5, 6, 99].map((n, i) => sample(i, n)), "70s");
  assert.deepEqual(donePerMinute(all), { perMinute: 21, spanMs: 60000 });
  assert.deepEqual(donePerMinute(recent([sample(0, 2), sample(1, 3), sample(2, 99)])), {
    perMinute: 15, spanMs: 20000,
  });
  assert.equal(donePerMinute(recent([sample(0, 99)])), null);
});

test("done series excludes unrecorded slots and the step in progress", () => {
  assert.deepEqual(doneSeries(recent([sample(0, 2), sample(1, 3), sample(2, 99)], "50s")),
    [null, null, 12, 18, null]);
});

test("pass bars distinguish complete outcomes, the partial step, and unrecorded slots", () => {
  const r = recent([
    { ...sample(0), passes: 1, skipped: 1 },
    { ...sample(1), passes: 2 },
    { ...sample(2), passes: 0 },
    { ...sample(3), passes: 1, skipped: 1 },
  ], "50s");
  const bars = passBars(r);
  assert.deepEqual(bars, ["unrecorded", "skipped", "ok", "idle", "partial"]);
  assert.equal(bars.filter((bar) => bar === "skipped").length, 1);
  assert.deepEqual(passBars(recent([], "30s")), ["unrecorded", "unrecorded", "unrecorded"]);
});

test("cache window weights lookups and distinguishes unrecorded and zero steps", () => {
  const r = recent([{ ...sample(0), lookups: 10, hits: 5 }, { ...sample(1), lookups: 30, hits: 30 }, sample(2)], "40s");
  assert.deepEqual(cacheWindow(r), {
    lookups: 40, hits: 35, rate: 35 / 40,
    lookupsPerStep: [null, 10, 30, null], hitsPerStep: [null, 5, 30, null],
  });
  assert.deepEqual(cacheWindow(recent([sample(0)], "10s")), {
    lookups: 0, hits: 0, rate: null, lookupsPerStep: [null], hitsPerStep: [null],
  });
});

test("cache series reports hit counts preserving observed zero steps", () => {
  const r = recent([
    { ...sample(0), lookups: 10, hits: 9 },
    { ...sample(1), lookups: 0, hits: 0 },
    { ...sample(2), lookups: 100, hits: 1 },
  ], "30s");
  const window = cacheWindow(r);
  assert.deepEqual(window.lookupsPerStep, [10, 0, null]);
  assert.deepEqual(window.hitsPerStep, [9, 0, null]);
  assert.equal(window.lookups, 110);
  assert.equal(window.hits, 10);
  assert.equal(window.rate, 10 / 110);
  assert.ok(window.hitsPerStep.every((hits, i) => {
    const lookups = window.lookupsPerStep[i];
    return hits === null ? lookups === null : lookups != null && hits <= lookups;
  }));
});

test("cache window excludes samples older than the window", () => {
  const r = recent([
    { ...sample(0), lookups: 100, hits: 100 },
    { ...sample(1), lookups: 10, hits: 5 },
    { ...sample(2), lookups: 30, hits: 30 },
  ], "20s");
  assert.deepEqual(cacheWindow(r), {
    lookups: 40, hits: 35, rate: 35 / 40,
    lookupsPerStep: [10, null], hitsPerStep: [5, null],
  });
});

test("transfer and eviction series distinguish missing steps from recorded zero", () => {
  const r = recent([
    { ...sample(0), uploadedBytes: 3, downloadedBytes: 7, evictedBytes: 5 },
    sample(1),
    { ...sample(2), uploadedBytes: 4, downloadedBytes: 6, evictedBytes: 9 },
  ], "40s");
  assert.deepEqual(transferred(r), { uploaded: 7, downloaded: 13 });
  assert.deepEqual(bytesSeries(r, "downloadedBytes"), [null, 7, 0, null]);
  assert.deepEqual(bytesSeries(r, "uploadedBytes"), [null, 3, 0, null]);
  assert.deepEqual(evictedSeries(r), [null, 5, 0, null]);
  assert.equal(transferred(recent([])), null);
});

test("scheduler outages preserve the timeline without fabricating scheduler measurements", () => {
  const r = recent([
    { ...sample(0, 2), running: 3, queued: 4, evictedBytes: 5, lookups: 10, hits: 8 },
    { ...sample(1, 999), schedulerKnown: false, running: 999, queued: 999, evictedBytes: 999, lookups: 12, hits: 9 },
    { ...sample(2, 3), schedulerKnown: true, passes: 0, running: 6, queued: 7, evictedBytes: 8, lookups: 14, hits: 10 },
    sample(3),
  ], "40s");
  assert.equal(schedulerKnown({ ...sample(0), passes: 0 }), false);
  assert.deepEqual(runningSeries(r, 0), [3, null, 6, 0]);
  assert.deepEqual(queuedSeries(r, 0), [4, null, 7, 0]);
  assert.deepEqual(doneSeries(r), [12, null, 18, null]);
  assert.deepEqual(evictedSeries(r), [5, null, 8, null]);
  assert.equal(donePerMinute(r), null);
  assert.deepEqual(cacheWindow(r).hitsPerStep, [8, 9, 10, null]);
  const recovered = recent([...r.samples!, ...[4, 5, 6, 7, 8].map((i) => sample(i, 1))], "90s");
  assert.deepEqual(donePerMinute(recovered), { perMinute: 7, spanMs: 60000 });
});

test("current scheduler availability is independent of bucket completeness with legacy fallback", () => {
  const state = snapshotOf(load("live"));
  const unknown = recent([{ ...sample(0), schedulerKnown: false }]);
  assert.equal(schedulerCurrent({ ...state, recent: unknown, schedulerKnown: true }), true);
  assert.equal(schedulerCurrent({ ...state, recent: recent([sample(0)]), schedulerKnown: false }), false);
  assert.equal(schedulerCurrent({ ...state, recent: unknown }), false);
  assert.equal(schedulerCurrent({ ...state, recent: recent([sample(0)]) }), true);
});
