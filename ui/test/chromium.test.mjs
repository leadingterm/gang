// The DevTools driver fails fast when Chromium goes away.
import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { launch } from "../tools/chromium.mjs";

process.env.FONTCONFIG_FILE = join(process.env.JS_BINARY__RUNFILES, process.env.FONTCONFIG_FILE);

test("a killed browser rejects pending waits and later commands", { timeout: 60000 }, async () => {
  const profile = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), "layout-chromium-"));
  let browser;
  try {
    browser = await launch(join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM), { profile });
    const page = await browser.newPage();
    await page.send("Page.enable");
    const loaded = page.once("Page.loadEventFired"); // nothing navigates, so only a dead browser ends this wait
    process.kill(browser.pid, "SIGKILL"); // as the OOM killer would
    await assert.rejects(loaded, /Chromium/);
    await assert.rejects(page.send("Page.enable"), /Chromium/);
    await assert.rejects(browser.closed, /Chromium/);
  } finally {
    if (browser) await browser.close();
    rmSync(profile, { recursive: true, force: true });
  }
});

test("closing after the browser leader dies kills surviving helpers", { timeout: 60000 }, async () => {
  const profile = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), "layout-chromium-"));
  let browser;
  let pids;
  let helperSocket;
  try {
    const program = join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM);
    writeFileSync(join(profile, "fixture.json"), JSON.stringify({ program }));
    browser = await launch(join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM_FIXTURE), { profile });
    pids = JSON.parse(readFileSync(join(profile, "fixture-pids.json"), "utf8"));
    helperSocket = createConnection({ host: "127.0.0.1", port: pids.port });
    const helperOutcome = new Promise((resolve, reject) => {
      helperSocket.once("data", (data) => resolve(data.toString()));
      helperSocket.once("close", () => resolve("closed"));
      helperSocket.on("error", (error) => {
        if (error.code !== "ECONNRESET" && error.code !== "EPIPE") reject(error);
      });
    });
    helperOutcome.catch(() => {});
    await once(helperSocket, "connect");
    const page = await browser.newPage();
    await page.send("Page.enable");
    const loaded = page.once("Page.loadEventFired");
    process.kill(browser.pid, "SIGKILL");
    await assert.rejects(loaded, /Chromium/);
    await browser.close();
    browser = undefined;
    if (!helperSocket.destroyed) helperSocket.write("?");
    assert.equal(await helperOutcome, "closed", "helper still runs after browser close");
  } finally {
    helperSocket?.destroy();
    for (const pid of pids ? [pids.helper, pids.browser] : []) {
      try {
        process.kill(pid, "SIGKILL");
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
    }
    if (browser) await browser.close();
    rmSync(profile, { recursive: true, force: true });
  }
});

test("graceful browser close kills surviving helpers", { timeout: 60000 }, async (t) => {
  const profile = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), "layout-chromium-"));
  let browser;
  let pids;
  let helperSocket;
  try {
    const program = join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM);
    writeFileSync(join(profile, "fixture.json"), JSON.stringify({ program }));
    browser = await launch(join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM_FIXTURE), { profile });
    pids = JSON.parse(readFileSync(join(profile, "fixture-pids.json"), "utf8"));
    helperSocket = createConnection({ host: "127.0.0.1", port: pids.port });
    const helperOutcome = new Promise((resolve, reject) => {
      helperSocket.once("data", (data) => resolve(data.toString()));
      helperSocket.once("close", () => resolve("closed"));
      helperSocket.on("error", (error) => {
        if (error.code !== "ECONNRESET" && error.code !== "EPIPE") reject(error);
      });
    });
    helperOutcome.catch(() => {});
    await once(helperSocket, "connect");
    const timers = new Set();
    const scheduled = [];
    t.mock.method(globalThis, "setTimeout", (callback, delay, ...args) => {
      const handle = {
        callback, delay, args, referenced: true,
        unref() { this.referenced = false; return this; },
      };
      timers.add(handle);
      scheduled.push(handle);
      return handle;
    });
    t.mock.method(globalThis, "clearTimeout", (handle) => timers.delete(handle));
    await browser.close();
    const deadline = scheduled.find((handle) => handle.delay === 5000);
    assert.ok(deadline, "browser close must schedule a force deadline");
    assert.equal(deadline.referenced, false);
    assert.equal(timers.has(deadline), false, "browser close must clear its force deadline");
    browser = undefined;
    if (!helperSocket.destroyed) helperSocket.write("?");
    assert.equal(await helperOutcome, "closed", "helper still runs after browser close");
  } finally {
    helperSocket?.destroy();
    for (const pid of pids ? [pids.helper, pids.browser] : []) {
      try {
        process.kill(pid, "SIGKILL");
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
    }
    if (browser) await browser.close();
    rmSync(profile, { recursive: true, force: true });
  }
});

test("browser close waits for inherited stderr after the leader exits", { timeout: 60000 }, async () => {
  const profile = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), "layout-chromium-"));
  let browser;
  let pids;
  let helperSocket;
  try {
    const program = join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM);
    writeFileSync(join(profile, "fixture.json"), JSON.stringify({ program, detachHelper: true }));
    browser = await launch(join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM_FIXTURE), { profile });
    pids = JSON.parse(readFileSync(join(profile, "fixture-pids.json"), "utf8"));
    helperSocket = createConnection({ host: "127.0.0.1", port: pids.port });
    await once(helperSocket, "connect");
    const page = await browser.newPage();
    await page.send("Page.enable");
    const loaded = page.once("Page.loadEventFired");
    process.kill(browser.pid, "SIGKILL");
    await assert.rejects(loaded, /Chromium/);
    const closing = browser.close();
    const response = once(helperSocket, "data").then(([data]) => data.toString());
    helperSocket.write("?");
    assert.equal(await Promise.race([closing.then(() => "early"), response.then(() => "held")]), "held");
    assert.equal(await response, "A");
    helperSocket.write("!");
    await closing;
    browser = undefined;
  } finally {
    helperSocket?.destroy();
    for (const pid of pids ? [pids.helper, pids.browser] : []) {
      try {
        process.kill(pid, "SIGKILL");
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
    }
    if (browser) await browser.close();
    rmSync(profile, { recursive: true, force: true });
  }
});
