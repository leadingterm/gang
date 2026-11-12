import assert from "node:assert/strict";
import { test } from "node:test";
import { clock, day, duration, gib, humanize, policyDuration, splitTarget, sum, workerInstance, workerShort } from "../src/format.ts";

const AT = Date.UTC(2026, 0, 15, 10, 9, 30);
const WORKER = "i-0abc123-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b";

test("duration is coarse and never negative", () => {
  const cases: [number, string][] = [
    [-5000, "0s"],
    [999, "0s"],
    [42000, "42s"],
    [60000, "1m 00s"],
    [253000, "4m 13s"],
    [3600000, "1h 00m"],
    [7500000, "2h 05m"],
    [86400000, "1d 0h"],
    [273600000, "3d 4h"],
  ];
  for (const [ms, text] of cases) assert.equal(duration(ms), text, `${ms} ms`);
});

test("policy durations read in whole minutes when exact", () => {
  assert.equal(policyDuration("600s"), "10m");
  assert.equal(policyDuration("90s"), "90s");
  assert.equal(policyDuration("45s"), "45s");
});

test("a target splits at its last colon", () => {
  assert.deepEqual(splitTarget("//storage/blob:blob_store"), ["//storage/blob", ":blob_store"]);
  assert.deepEqual(splitTarget("//a:b:c"), ["//a:b", ":c"]);
  assert.deepEqual(splitTarget("plain"), ["", "plain"]);
});

test("a worker id names its instance and a short uuid", () => {
  assert.equal(workerInstance(WORKER), "i-0abc123");
  assert.equal(workerInstance("0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"), "0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b");
  assert.equal(workerShort(WORKER, "i-0abc123"), "0194");
  assert.equal(workerShort("other-id", "i-0abc123"), "othe");
});

test("times are UTC and sizes whole GiB", () => {
  assert.equal(clock(AT), "10:09:30");
  assert.equal(day(AT), "15 Jan 2026");
  assert.equal(gib(64 * 1024 ** 3), "64 GiB");
});

test("an enum name reads as lowercase words", () => {
  assert.equal(humanize("IDLE_TIMEOUT"), "idle timeout");
  assert.equal(humanize("SCHEDULER_UNAVAILABLE"), "scheduler unavailable");
  assert.equal(humanize("DEMAND"), "demand");
});

test("a sum totals one number per row, and is zero without rows", () => {
  assert.equal(sum([{ n: 2 }, { n: 5 }, { n: 0 }], (row) => row.n), 7);
  assert.equal(sum([], () => 1), 0);
});
