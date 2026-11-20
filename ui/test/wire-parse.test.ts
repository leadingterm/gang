import assert from "node:assert/strict";
import { test } from "node:test";
import { eventKind, identityOf, idOf, ms, scalingLog, seconds, splitIdentity } from "../src/wire-parse.ts";
import type { Fleet, QueuedAction, ScalingEvent } from "../src/wire.ts";

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

test("fleet launch failure and queued action refusal parse as optional fields", () => {
  const fleet = JSON.parse('{"launchFailure":{"reason":"capacity unavailable"}}') as Fleet;
  const queued = JSON.parse('{"refusal":"cpu_count 99 is above the largest fleet\'s 15"}') as QueuedAction;
  assert.deepEqual(fleet.launchFailure, { reason: "capacity unavailable" });
  assert.equal(queued.refusal, "cpu_count 99 is above the largest fleet's 15");
  assert.equal((JSON.parse("{}") as Fleet).launchFailure, undefined);
  assert.equal((JSON.parse("{}") as QueuedAction).refusal, undefined);
});

test("launch failure and withdrawal events parse and identify their kinds", () => {
  const failed = JSON.parse('{"name":"events/2","createTime":"2026-01-15T10:09:30.000Z","fleet":"fleets/a","launchFailed":{"from":2,"to":4,"reason":"capacity unavailable"}}') as ScalingEvent;
  const withdrawn = JSON.parse('{"name":"events/3","createTime":"2026-01-15T10:09:30.000Z","fleet":"fleets/a","withdrawn":{"from":4,"to":2}}') as ScalingEvent;
  assert.equal(eventKind(failed), "launchFailed");
  assert.equal(eventKind(withdrawn), "withdrawn");
  assert.deepEqual("launchFailed" in failed && failed.launchFailed, { from: 2, to: 4, reason: "capacity unavailable" });
  assert.deepEqual("withdrawn" in withdrawn && withdrawn.withdrawn, { from: 4, to: 2 });
});

test("AWS statuses merge by source time and unknown events survive", () => {
  const base = { createTime: "2026-01-15T10:09:30Z", fleet: "fleets/a" };
  const unknown = { ...base, name: "events/3", unknownFuture: { x: 1 } } as unknown as ScalingEvent;
  const aws: ScalingEvent = { ...base, name: "awsEvents/a", aws: { event: {
    id: "a", at: base.createTime, kind: { type: "capacityUnavailable", reason: "no capacity" },
  } } };
  const older: ScalingEvent = { ...base, name: "events/2", createTime: "2026-01-15T10:08:00Z", scaledOut: { from: 0, to: 1 } };
  assert.equal(eventKind(aws), "aws");
  assert.equal(eventKind(unknown), undefined);
  assert.deepEqual(scalingLog([unknown, older], [aws, aws]).map((e) => e.name), ["events/2", "awsEvents/a", "events/3"]);
  const enriched: ScalingEvent = { ...base, name: "awsEvents/a", aws: { event: {
    id: "a", at: base.createTime, kind: { type: "replacementLaunched", instance: "instances/i-new", replacing: "instances/i-old" },
  } } };
  assert.deepEqual(scalingLog([aws], [enriched]), [enriched]);
  assert.deepEqual(scalingLog([unknown])[0], unknown);
});

test("unknown AWS variants remain identifiable and retain their payload", () => {
  const value = JSON.parse('{"name":"awsEvents/a","createTime":"2026-01-15T10:09:30Z","fleet":"fleets/a","aws":{"event":{"id":"a","at":"2026-01-15T10:09:30Z","kind":{"type":"futureNotice","detail":"kept"}}}}') as ScalingEvent;
  assert.equal(eventKind(value), "aws");
  assert.deepEqual(JSON.parse(JSON.stringify(scalingLog([value])[0])), value);
});

test("equal-time audit IDs keep decimal order without losing u64 precision", () => {
  const common = { createTime: "2026-01-15T10:09:30Z", fleet: "fleets/a" };
  const names = ["events/9007199254740993", "events/100", "events/10", "events/99", "events/9", "events/9007199254740992"];
  const audit: ScalingEvent[] = names.map((name) => ({ ...common, name, scaledOut: { from: 0, to: 1 } }));
  const aws: ScalingEvent = { ...common, name: "awsEvents/a", aws: { event: {
    id: "a", at: common.createTime, kind: { type: "capacityUnavailable", reason: "capacity" },
  } } };
  assert.deepEqual(scalingLog(audit, [aws, aws]).map((e) => e.name), [
    "awsEvents/a", "events/9", "events/10", "events/99", "events/100", "events/9007199254740992", "events/9007199254740993",
  ]);
  const earlier = { ...audit[0]!, createTime: "2026-01-15T10:08:00Z" };
  assert.equal(scalingLog([audit[4]!, earlier])[0], earlier);
});
