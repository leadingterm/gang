import { spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const args = process.argv.slice(2);
const profile = args.find((arg) => arg.startsWith("--user-data-dir=")).slice("--user-data-dir=".length);
const { program, detachHelper = false } = JSON.parse(readFileSync(join(profile, "fixture.json"), "utf8"));
const helper = spawn(process.execPath, [fileURLToPath(new URL("./chromium-helper.mjs", import.meta.url))], {
  detached: detachHelper,
  stdio: ["ignore", "ignore", "inherit", "ipc"],
});
helper.once("message", ({ pid, port }) => {
  const browser = spawn(program, args, { stdio: ["ignore", "ignore", "pipe"] });
  writeFileSync(join(profile, "fixture-pids.json"), JSON.stringify({ helper: pid, browser: browser.pid, port }));
  browser.stderr.pipe(process.stderr);
  browser.once("error", (error) => { throw error; });
  browser.once("exit", (code, signal) => {
    if (signal) process.kill(process.pid, signal);
    else process.exit(code);
  });
});
helper.once("error", (error) => { throw error; });
