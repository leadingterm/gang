// The ADR-31 bundle budgets and the shape of the precompressed bundle: `node budget.test.mjs <dist>`.
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { brotliDecompressSync, gunzipSync } from "node:zlib";

const JS_BUDGET_GZ = 60 * 1024;
const CSS_BUDGET_GZ = 15 * 1024;

const dist = process.argv[2];
if (!dist) throw new Error("usage: budget.test.mjs <dist>");
const assets = readdirSync(join(dist, "assets"));
const ending = (suffix) => assets.filter((f) => f.endsWith(suffix));
const gzBytes = (suffix) => ending(suffix).reduce((n, f) => n + statSync(join(dist, "assets", f)).size, 0);

test("JavaScript fits its gzip budget", () => {
  const bytes = gzBytes(".js.gz");
  assert.ok(bytes > 0 && bytes <= JS_BUDGET_GZ, `${bytes} B gz against ${JS_BUDGET_GZ} B`);
});

test("CSS fits its gzip budget", () => {
  const bytes = gzBytes(".css.gz");
  assert.ok(bytes > 0 && bytes <= CSS_BUDGET_GZ, `${bytes} B gz against ${CSS_BUDGET_GZ} B`);
});

test("every text asset ships gzip and brotli copies that decompress to it", () => {
  const text = [join(dist, "index.html"), ...[...ending(".js"), ...ending(".css")].map((f) => join(dist, "assets", f))];
  for (const file of text) {
    const identity = readFileSync(file);
    for (const copy of [`${file}.gz`, `${file}.br`]) assert.ok(existsSync(copy), copy);
    assert.deepEqual(gunzipSync(readFileSync(`${file}.gz`)), identity, `${file}.gz`);
    assert.deepEqual(brotliDecompressSync(readFileSync(`${file}.br`)), identity, `${file}.br`);
  }
});

test("fonts ship uncompressed", () => {
  assert.ok(ending(".woff2").length > 0);
  assert.deepEqual([...ending(".woff2.gz"), ...ending(".woff2.br")], []);
});

test("index.html references the hashed bundle", () => {
  const html = readFileSync(join(dist, "index.html"), "utf8");
  for (const main of [...ending(".js"), ...ending(".css")].filter((f) => f.startsWith("main-"))) {
    assert.ok(html.includes(`/ui/assets/${main}`), main);
  }
});
