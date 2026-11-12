import assert from "node:assert/strict";
import { test } from "node:test";
import { eventKind, identityOf, idOf, ms, seconds, splitIdentity } from "../src/wire-parse.ts";
import type { ScalingEvent } from "../src/wire.ts";

test("timestamps parse to unix milliseconds, in any offset", () => {
  assert.equal(ms("2026-01-15T10:09:30.250Z"), Date.UTC(2026, 0, 15, 10, 9, 30, 250));
  assert.equal(ms("2026-01-15T11:09:30.000+01:00"), Date.UTC(2026, 0, 15, 10, 9, 30));
});

test("durations parse to seconds", () => {
  assert.equal(seconds("600s"), 600);
  assert.equal(seconds("0.250s"), 0.25);
  assert.equal(seconds("1.500s"), 1.5);
});

test("an ID is the percent-decoded text after the collection", () => {
  assert.equal(idOf("fleets/amd64-spot"), "amd64-spot");
  assert.equal(idOf("events/995"), "995");
  assert.equal(identityOf("principals/google%3Aops%40example.org"), "google:ops@example.org");
  assert.equal(
    identityOf("principals/github-actions%3Arepo%3Aexample%2Fmonorepo%3Apull_request"),
    "github-actions:repo:example/monorepo:pull_request",
  );
});

test("an identity splits at its first colon", () => {
  assert.deepEqual(splitIdentity("github-actions:repo:example/monorepo"), ["github-actions", "repo:example/monorepo"]);
  assert.deepEqual(splitIdentity("google:ops@example.org"), ["google", "ops@example.org"]);
});

test("an event names the oneof member it carries, and none it does not know", () => {
  const common = { name: "events/1", createTime: "2026-01-15T10:09:30.000Z", fleet: "fleets/a" };
  assert.equal(eventKind({ ...common, scaledOut: { from: 0, to: 1 } }), "scaledOut");
  assert.equal(eventKind({ ...common, terminated: { instance: "instances/i-1" } }), "terminated");
  assert.equal(
    eventKind({ ...common, exitedEarly: { instance: "instances/local-1", failure: "worker failed" } }),
    "exitedEarly",
  );
  assert.equal(eventKind({ ...common, hibernated: { instance: "instances/i-1" } } as unknown as ScalingEvent), undefined);
});
