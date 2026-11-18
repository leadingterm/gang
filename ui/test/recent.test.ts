import assert from "node:assert/strict";
import { test } from "node:test";
import { applyRecent, cacheWindow, donePerMinute, doneSeries, passBars, slots, unrecordedSlots } from "../src/recent.ts";
import type { Recent, Sample } from "../src/wire.ts";
import { iso } from "./sse.ts";

const sample = (step: number, completed = 0): Sample => ({
  start: iso(step * 10000), completed, running: 0, queued: 0, passes: 0, skipped: 0, lookups: 0, hits: 0,
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

test("pass bars distinguish skipped, ok, idle and unrecorded", () => {
  const r = recent([
    { ...sample(0), passes: 1, skipped: 1 },
    { ...sample(1), passes: 2 },
    sample(2),
  ], "40s");
  assert.deepEqual(passBars(r), ["unrecorded", "skipped", "ok", "idle"]);
});

test("cache window weights lookups and distinguishes unrecorded and zero steps", () => {
  const r = recent([{ ...sample(0), lookups: 10, hits: 5 }, { ...sample(1), lookups: 30, hits: 30 }, sample(2)], "40s");
  assert.deepEqual(cacheWindow(r), {
    lookups: 40, hits: 35, rate: 35 / 40,
    lookupsPerStep: [null, 10, 30, 0], hitsPerStep: [null, 5, 30, 0],
  });
  assert.deepEqual(cacheWindow(recent([sample(0)], "10s")), {
    lookups: 0, hits: 0, rate: null, lookupsPerStep: [0], hitsPerStep: [0],
  });
});

test("cache series reports hit counts preserving observed zero steps", () => {
  const r = recent([
    { ...sample(0), lookups: 10, hits: 9 },
    { ...sample(1), lookups: 0, hits: 0 },
    { ...sample(2), lookups: 100, hits: 1 },
  ], "30s");
  const window = cacheWindow(r);
  assert.deepEqual(window.lookupsPerStep, [10, 0, 100]);
  assert.deepEqual(window.hitsPerStep, [9, 0, 1]);
  assert.ok(window.hitsPerStep.every((hits, i) => hits !== null && hits <= window.lookupsPerStep[i]!));
});

test("cache window excludes samples older than the window", () => {
  const r = recent([
    { ...sample(0), lookups: 100, hits: 100 },
    { ...sample(1), lookups: 10, hits: 5 },
    { ...sample(2), lookups: 30, hits: 30 },
  ], "20s");
  assert.deepEqual(cacheWindow(r), {
    lookups: 40, hits: 35, rate: 35 / 40,
    lookupsPerStep: [10, 30], hitsPerStep: [5, 30],
  });
});
