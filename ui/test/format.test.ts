import assert from "node:assert/strict";
import { test } from "node:test";
import { bytes, clock, day, duration, gib, humanize, splitTail, sum, workerInstance } from "../src/format.ts";

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


test("an identifier splits at its last colon", () => {
  assert.deepEqual(splitTail("//storage/blob:blob_store"), ["//storage/blob", ":blob_store"]);
  assert.deepEqual(splitTail("//a:b:c"), ["//a:b", ":c"]);
  assert.deepEqual(splitTail("plain"), ["", "plain"]);
  assert.deepEqual(splitTail(":x"), ["", ":x"]);
});

test("worker labels distinguish instances and preserve ids without an instance", () => {
  const a = "i-04be3ef9b3f358c25-1f1e0567-7b3c-6f10-8a4e-1c2d3e4f5a6b";
  const b = "i-092aaa89b3f358c25-1f1e0563-7b3c-6f10-8a4e-1c2d3e4f5a6b";
  assert.equal(workerInstance(a), "i-04be3ef9b3f358c25");
  assert.equal(workerInstance(b), "i-092aaa89b3f358c25");
  assert.notEqual(workerInstance(a), workerInstance(b));
  assert.equal(workerInstance(WORKER), "i-0abc123");
  assert.equal(workerInstance("0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"), "0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b");
  assert.equal(workerInstance("a-long-worker-id-that-does-not-name-an-instance"), "a-long-worker-id-that-does-not-name-an-instance");
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

test("byte units use IEC boundaries and three significant digits", () => {
  assert.equal(bytes(0), "0 B");
  assert.equal(bytes(1023), "1,020 B");
  assert.equal(bytes(1024), "1 KiB");
  assert.equal(bytes(1536), "1.5 KiB");
  assert.equal(bytes(1024 ** 2), "1 MiB");
  assert.equal(bytes(12.345 * 1024 ** 3), "12.3 GiB");
});
