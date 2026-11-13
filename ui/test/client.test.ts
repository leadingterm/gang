import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { setImmediate as flush } from "node:timers/promises";
import { loginUrl, StateClient, type Environment, type FrameData, type StreamSource } from "../src/client.ts";
import { idOf, ms } from "../src/wire-parse.ts";
import { PATHS, type Frame, type ScalingEvent, type Session } from "../src/wire.ts";
import { iso, load, snapshotOf } from "./sse.ts";

const session = JSON.parse(readFileSync(new URL("../fixtures/session.json", import.meta.url), "utf8")) as Session;
const defaultSession = { ...session, expireTime: undefined };
const anonymous: Session = { name: session.name, permissions: [] };
const snapshot = snapshotOf(load("live"));
const t0 = ms(snapshot.updateTime);

const PRINCIPALS = `${PATHS.listPrincipals}?pageSize=500`;
const REVOKED = `${PATHS.listPrincipals}?pageSize=500&filter=revokeTime%3A*`;

class FakeSource implements StreamSource {
  readonly listeners = new Map<string, (ev: MessageEvent<string>) => void>();
  onerror: ((ev: Event) => void) | null = null;
  closed = false;
  addEventListener(type: string, listener: (ev: MessageEvent<string>) => void): void {
    this.listeners.set(type, listener);
  }
  close(): void {
    this.closed = true;
  }
  emit<E extends Frame["event"]>(event: E, data: FrameData<E>): void {
    this.listeners.get(event)?.({ data: JSON.stringify(data) } as MessageEvent<string>);
  }
  emitRaw(type: string, data: string): void {
    this.listeners.get(type)?.({ data } as MessageEvent<string>);
  }
  fail(): void {
    this.onerror?.(new Event("error"));
  }
}

class FakeEnvironment implements Environment {
  readonly sources: FakeSource[] = [];
  readonly scheduled: { run: () => void; delay: number }[] = [];
  readonly navigations: string[] = [];
  readonly responses = new Map<string, unknown>([[PATHS.getSession, defaultSession]]);
  readonly fetched: string[] = [];
  readonly posts: string[] = [];
  readonly postResults: (number | Error)[] = [];
  clock = 1000;
  openStream(): StreamSource {
    const source = new FakeSource();
    this.sources.push(source);
    return source;
  }
  async fetchJson(path: string): Promise<unknown> {
    this.fetched.push(path);
    const body = this.responses.get(path);
    if (body instanceof Error || body === undefined) throw body ?? new Error(`no fixture for ${path}`);
    return body;
  }
  async post(path: string): Promise<number> {
    this.posts.push(path);
    const result = this.postResults.shift() ?? 204;
    if (result instanceof Error) throw result;
    return result;
  }
  now(): number {
    return this.clock;
  }
  schedule(run: () => void, delay: number): () => void {
    this.scheduled.push({ run, delay });
    return () => {};
  }
  navigate(url: string): void {
    this.navigations.push(url);
  }
  here(): string {
    return "/ui/workers";
  }
  get source(): FakeSource {
    const source = this.sources.at(-1);
    assert.ok(source, "a stream is open");
    return source;
  }
}

const live = async (): Promise<{ env: FakeEnvironment; client: StateClient }> => {
  const env = new FakeEnvironment();
  const client = new StateClient(env);
  await client.start();
  env.source.emit("snapshot", snapshot);
  assert.equal(env.sources.length, 1);
  assert.equal(client.link.value.kind, "live");
  assert.equal(client.state.value?.seq, snapshot.seq);
  return { env, client };
};

/** An event of the ledger as `base` with another position and time. */
const eventAt = (base: ScalingEvent, seq: number, createTime: number): ScalingEvent => ({
  ...base,
  name: `events/${seq}`,
  createTime: iso(createTime),
});

const seqs = (events: readonly ScalingEvent[] | undefined): number[] => (events ?? []).map((e) => Number(idOf(e.name)));

test("a delta on another base reconnects", async () => {
  const { env } = await live();
  const first = env.source;
  first.emit("delta", { seq: snapshot.seq + 2, base: snapshot.seq + 1, updateTime: snapshot.updateTime });
  assert.ok(first.closed);
  assert.equal(env.sources.length, 2);
});

test("a frame that is not JSON reopens the stream", async () => {
  const { env, client } = await live();
  const before = client.state.value;
  const first = env.source;
  first.emitRaw("delta", "{not json");
  assert.ok(first.closed);
  assert.equal(env.sources.length, 2);
  assert.equal(client.state.value, before);
});

test("a tick ahead of the state keeps the stream, a tick behind it reconnects", async () => {
  const { env } = await live();
  env.source.emit("tick", { seq: snapshot.seq + 1, time: snapshot.updateTime });
  assert.equal(env.sources.length, 1);
  env.source.emit("tick", { seq: snapshot.seq - 1, time: snapshot.updateTime });
  assert.equal(env.sources.length, 2);
});

test("VIEWER_CAP freezes the rows and retries after retryDelay", async () => {
  const { env, client } = await live();
  const first = env.source;
  first.emit("closed", { reason: "VIEWER_CAP", retryDelay: "5s" });
  assert.ok(first.closed);
  assert.equal(client.link.value.kind, "lost");
  assert.deepEqual(
    env.scheduled.map((s) => s.delay),
    [5000],
  );
  env.scheduled[0]?.run();
  await flush();
  assert.equal(env.sources.length, 2);
});

test("REVOKED ends the stream for good", async () => {
  const { env, client } = await live();
  env.source.emit("closed", { reason: "REVOKED" });
  assert.equal(client.link.value.kind, "revoked");
  assert.equal(env.scheduled.length, 0);
});

test("renews before expireTime", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, expireTime: iso(env.clock + 10 * 60_000) });
  const client = new StateClient(env);
  await client.start();
  const renewal = env.scheduled.find((entry) => entry.delay === 8 * 60_000);
  assert.ok(renewal);
  renewal.run();
  assert.deepEqual(env.posts, [PATHS.renew]);
  await flush();
  assert.deepEqual(env.fetched, [PATHS.getSession, PATHS.getSession]);
  assert.equal(env.sources.length, 2);
  assert.ok(env.sources[0]?.closed);
});

test("a one-minute session renews after thirty seconds, not at once", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, expireTime: iso(env.clock + 60_000) });
  const client = new StateClient(env);
  await client.start();
  assert.deepEqual(env.scheduled.map((entry) => entry.delay), [30_000]);
  assert.deepEqual(env.posts, []);
});

test("EXPIRED renews and reconnects without navigating", async () => {
  const { env } = await live();
  env.postResults.push(204);
  env.source.emit("closed", { reason: "EXPIRED" });
  await flush();
  assert.deepEqual(env.posts, [PATHS.renew]);
  assert.deepEqual(env.navigations, []);
  assert.equal(env.sources.length, 2);
});

test("a 401 or 403 from renewal sends the browser to sign in", async () => {
  for (const status of [401, 403]) {
    const { env } = await live();
    env.postResults.push(status);
    env.source.emit("closed", { reason: "EXPIRED" });
    await flush();
    assert.deepEqual(env.navigations, [loginUrl(env.here())]);
    assert.equal(env.sources.length, 1);
  }
  assert.equal(loginUrl("/ui/workers"), "/ui/login?next=%2Fui%2Fworkers");
});

test("a failed renewal retries later", async () => {
  const { env, client } = await live();
  env.postResults.push(503);
  env.source.emit("closed", { reason: "EXPIRED" });
  await flush();
  assert.deepEqual(env.navigations, []);
  assert.equal(client.link.value.kind, "lost");
  assert.deepEqual(env.scheduled.map((entry) => entry.delay), [1000]);
});

test("a failed renewal retries the renewal, not the session probe", async () => {
  const { env, client } = await live();
  env.postResults.push(503);
  env.source.emit("closed", { reason: "EXPIRED" });
  await flush();
  // By the retry the session cookie has expired; only the renewal cookie is left.
  env.responses.set(PATHS.getSession, { ...anonymous, login: session.login });
  const retry = env.scheduled.at(-1);
  assert.ok(retry);
  retry.run();
  // A renewal sent by the retry succeeds: the server now answers with the renewed session.
  env.responses.set(PATHS.getSession, defaultSession);
  await flush();
  assert.deepEqual(env.posts, [PATHS.renew, PATHS.renew]);
  assert.notEqual(client.link.value.kind, "signed_out");
  assert.equal(env.sources.length, 2);
});

test("a reason this client does not know backs off like a shutdown", async () => {
  const { env, client } = await live();
  // A newer server may send reasons this client's `ClosedReason` does not list yet.
  env.source.emit("closed", { reason: "MAINTENANCE" } as unknown as FrameData<"closed">);
  assert.equal(client.link.value.kind, "lost");
  assert.deepEqual(
    env.scheduled.map((s) => s.delay),
    [1000],
  );
});

test("a failed stream without a principal renews before signing in", async () => {
  const { env } = await live();
  env.responses.set(PATHS.getSession, { ...anonymous, login: session.login });
  env.postResults.push(401);
  env.source.fail();
  await flush();
  assert.deepEqual(env.posts, [PATHS.renew]);
  assert.deepEqual(env.navigations, [loginUrl("/ui/workers")]);
});

test("a failed stream without state.read is forbidden", async () => {
  const { env, client } = await live();
  env.responses.set(PATHS.getSession, { ...session, permissions: [] });
  env.source.fail();
  await flush();
  assert.equal(client.link.value.kind, "forbidden");
});

test("a failed stream with no session answer backs off", async () => {
  const { env, client } = await live();
  env.responses.set(PATHS.getSession, new Error("offline"));
  env.source.fail();
  await flush();
  assert.equal(client.link.value.kind, "lost");
  env.scheduled[0]?.run();
  await flush();
  assert.deepEqual(
    env.scheduled.map((s) => s.delay),
    [1000, 2000],
  );
});

test("reconnect delays double up to 30 seconds", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, new Error("offline"));
  const client = new StateClient(env);
  await client.start();
  for (let attempt = 1; attempt < 7; attempt += 1) {
    env.scheduled.at(-1)?.run();
    await flush();
  }
  assert.deepEqual(
    env.scheduled.map((s) => s.delay),
    [1000, 2000, 4000, 8000, 16000, 30000, 30000],
  );
});

test("a snapshot resets the reconnect backoff", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, new Error("offline"));
  const client = new StateClient(env);
  await client.start();
  env.scheduled.at(-1)?.run();
  await flush();
  env.responses.set(PATHS.getSession, defaultSession);
  env.scheduled.at(-1)?.run();
  await flush();
  env.source.emit("snapshot", snapshot);
  env.responses.set(PATHS.getSession, new Error("offline"));
  env.source.fail();
  await flush();
  assert.deepEqual(
    env.scheduled.map((s) => s.delay),
    [1000, 2000, 1000],
  );
});

test("the server clock follows every frame", async () => {
  const { env, client } = await live();
  env.clock = 3500;
  assert.equal(client.serverNow(), t0 + 2500);
  env.source.emit("delta", { seq: snapshot.seq + 1, base: snapshot.seq, updateTime: iso(t0 + 9000) });
  assert.equal(client.serverNow(), t0 + 9000);
  env.clock = 4500;
  assert.equal(client.serverNow(), t0 + 10000);
  env.source.emit("tick", { seq: snapshot.seq + 1, time: iso(t0 + 12000) });
  assert.equal(client.serverNow(), t0 + 12000);
});

test("load earlier asks for the held events and a page more, then walks the page token", async () => {
  const { env, client } = await live();
  const oldest = snapshot.events[0];
  assert.ok(oldest);
  const oldestSeq = Number(idOf(oldest.name));
  const older = eventAt(oldest, oldestSeq - 1, ms(oldest.createTime) - 60000);
  const evenOlder = eventAt(oldest, oldestSeq - 2, ms(oldest.createTime) - 120000);
  const first = `${PATHS.listEvents}?pageSize=${snapshot.events.length + 200}`;
  const second = `${PATHS.listEvents}?pageSize=200&pageToken=t2`;
  // The ledger lists newest first, so the first answer also holds every event the client has.
  env.responses.set(first, { events: [...snapshot.events].reverse().concat(older), nextPageToken: "t2" });
  env.responses.set(second, { events: [evenOlder] });
  await client.loadEarlier();
  assert.deepEqual(client.earlier.value, { kind: "loaded", events: [older], loading: false, next: "t2" });
  await client.loadEarlier();
  assert.deepEqual(client.earlier.value, { kind: "loaded", events: [evenOlder, older], loading: false, next: undefined });
  await client.loadEarlier();
  assert.deepEqual(env.fetched.filter((p) => p.startsWith(PATHS.listEvents)), [first, second]);
});

test("the first page of earlier events is at most 1000", async () => {
  const { env, client } = await live();
  const base = snapshot.events[0];
  assert.ok(base);
  const many = Array.from({ length: 900 }, (_, i) => eventAt(base, 2000 + i, t0));
  env.source.emit("snapshot", { ...snapshot, events: many, eventsLimit: 900 });
  env.responses.set(`${PATHS.listEvents}?pageSize=1000`, { events: [] });
  await client.loadEarlier();
  assert.deepEqual(client.earlier.value, { kind: "loaded", events: [], loading: false, next: undefined });
});

test("a failed access load is retried for the same epoch", async () => {
  const { env, client } = await live();
  env.responses.set(PRINCIPALS, { principals: [] });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, new Error("offline"));
  await client.loadAccess();
  assert.equal(client.access.value, null);
  env.responses.set(PATHS.listRoles, { roles: [] });
  await client.loadAccess();
  assert.deepEqual(client.access.value, { principals: [], nextPageToken: undefined, revoked: [], roles: [] });
});

test("access loads every revoked principal and every role, and principals page by page", async () => {
  const { env, client } = await live();
  const principal = (name: string, revoked: boolean) => ({
    name: `principals/${name}`,
    provider: "google",
    principal: name,
    createTime: snapshot.updateTime,
    lastSeenTime: snapshot.updateTime,
    ...(revoked && { revokeTime: snapshot.updateTime }),
  });
  const [a, b, c] = [principal("a", false), principal("b", true), principal("c", true)];
  env.responses.set(PRINCIPALS, { principals: [a, b], nextPageToken: "p2" });
  env.responses.set(`${PRINCIPALS}&pageToken=p2`, { principals: [c] });
  env.responses.set(REVOKED, { principals: [b], nextPageToken: "r2" });
  env.responses.set(`${REVOKED}&pageToken=r2`, { principals: [c] });
  env.responses.set(PATHS.listRoles, { roles: [{ name: "roles/viewer", permissions: [], principals: [] }], nextPageToken: "o2" });
  env.responses.set(`${PATHS.listRoles}?pageToken=o2`, { roles: [{ name: "roles/admin", permissions: [], principals: [] }] });
  await client.loadAccess();
  assert.deepEqual(client.access.value?.revoked, [b, c]);
  assert.deepEqual(
    client.access.value?.roles.map((r) => r.name),
    ["roles/viewer", "roles/admin"],
  );
  assert.equal(client.access.value?.nextPageToken, "p2");
  await client.loadMorePrincipals();
  assert.deepEqual(client.access.value?.principals, [a, b, c]);
  assert.equal(client.access.value?.nextPageToken, undefined);
});

test("an access answer for an epoch that moved on is dropped", async () => {
  const { env, client } = await live();
  const { promise, resolve } = Promise.withResolvers<unknown>();
  env.responses.set(PRINCIPALS, promise);
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  const loading = client.loadAccess();
  env.source.emit("delta", {
    seq: snapshot.seq + 1,
    base: snapshot.seq,
    updateTime: snapshot.updateTime,
    principalsEpoch: snapshot.principalsEpoch + 1,
  });
  resolve({ principals: [] });
  await loading;
  assert.equal(client.access.value, null);
});

const principalRow = (name: string) => ({
  name: `principals/${name}`,
  provider: "google",
  principal: name,
  createTime: snapshot.updateTime,
  lastSeenTime: snapshot.updateTime,
});

test("a second request for more principals while the first is in flight does not append the page twice", async () => {
  const { env, client } = await live();
  const [a, b, c] = [principalRow("a"), principalRow("b"), principalRow("c")];
  const { promise, resolve } = Promise.withResolvers<unknown>();
  env.responses.set(PRINCIPALS, { principals: [a], nextPageToken: "p2" });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  env.responses.set(`${PRINCIPALS}&pageToken=p2`, promise);
  await client.loadAccess();
  const first = client.loadMorePrincipals();
  const second = client.loadMorePrincipals();
  resolve({ principals: [b, c] });
  await Promise.all([first, second]);
  assert.deepEqual(client.access.value?.principals, [a, b, c]);
  assert.equal(env.fetched.filter((path) => path === `${PRINCIPALS}&pageToken=p2`).length, 1);
});

test("a failed request for more principals keeps the list and allows a retry", async () => {
  const { env, client } = await live();
  const [a, b] = [principalRow("a"), principalRow("b")];
  env.responses.set(PRINCIPALS, { principals: [a], nextPageToken: "p2" });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  env.responses.set(`${PRINCIPALS}&pageToken=p2`, new Error("offline"));
  await client.loadAccess();
  const before = client.access.value;
  await client.loadMorePrincipals();
  assert.equal(client.access.value, before);
  env.responses.set(`${PRINCIPALS}&pageToken=p2`, { principals: [b] });
  await client.loadMorePrincipals();
  assert.deepEqual(client.access.value?.principals, [a, b]);
  assert.equal(client.access.value?.nextPageToken, undefined);
});

test("more principals for an access list that was replaced are dropped", async () => {
  const { env, client } = await live();
  const [a, b, d] = [principalRow("a"), principalRow("b"), principalRow("d")];
  const { promise, resolve } = Promise.withResolvers<unknown>();
  env.responses.set(PRINCIPALS, { principals: [a], nextPageToken: "p2" });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  env.responses.set(`${PRINCIPALS}&pageToken=p2`, promise);
  await client.loadAccess();
  const loading = client.loadMorePrincipals();
  env.source.emit("delta", {
    seq: snapshot.seq + 1,
    base: snapshot.seq,
    updateTime: snapshot.updateTime,
    principalsEpoch: snapshot.principalsEpoch + 1,
  });
  env.responses.set(PRINCIPALS, { principals: [d] });
  await client.loadAccess();
  resolve({ principals: [b] });
  await loading;
  assert.deepEqual(client.access.value?.principals, [d]);
});

test("shutdown retries after retryDelay, or backs off without it", async () => {
  const withRetry = await live();
  withRetry.env.source.emit("closed", { reason: "SHUTDOWN", retryDelay: "5s" });
  assert.equal(withRetry.client.link.value.kind, "lost");
  assert.deepEqual(
    withRetry.env.scheduled.map((s) => s.delay),
    [5000],
  );
  const without = await live();
  without.env.source.emit("closed", { reason: "SHUTDOWN" });
  assert.deepEqual(
    without.env.scheduled.map((s) => s.delay),
    [1000],
  );
  without.env.scheduled[0]?.run();
  await flush();
  assert.equal(without.env.sources.length, 2);
});

test("events trimmed from the stream after loading earlier move onto the loaded page", async () => {
  const { env, client } = await live();
  const full: typeof snapshot = { ...snapshot, eventsLimit: snapshot.events.length };
  env.source.emit("snapshot", full);
  const [oldest, second] = full.events;
  assert.ok(oldest && second);
  const older = eventAt(oldest, Number(idOf(oldest.name)) - 1, ms(oldest.createTime) - 60000);
  env.responses.set(`${PATHS.listEvents}?pageSize=${full.events.length + 200}`, { events: [older], nextPageToken: "t" });
  await client.loadEarlier();
  const appended = eventAt(oldest, Number(idOf(full.events.at(-1)?.name ?? "")) + 1, t0);
  env.source.emit("delta", { seq: full.seq + 1, base: full.seq, updateTime: full.updateTime, events: [appended] });
  assert.deepEqual(client.state.value?.events[0], second);
  assert.equal(client.earlier.value.kind, "loaded");
  if (client.earlier.value.kind === "loaded") assert.deepEqual(client.earlier.value.events, [older, oldest]);
});

test("a snapshot keeps the loaded page only when it continues the held events", async () => {
  const { env, client } = await live();
  const [oldest, second] = snapshot.events;
  assert.ok(oldest && second);
  const older = eventAt(oldest, Number(idOf(oldest.name)) - 1, ms(oldest.createTime) - 60000);
  env.responses.set(`${PATHS.listEvents}?pageSize=${snapshot.events.length + 200}`, { events: [older], nextPageToken: "t" });
  await client.loadEarlier();
  env.source.emit("snapshot", { ...snapshot, events: snapshot.events.slice(1) });
  assert.equal(client.earlier.value.kind, "loaded");
  if (client.earlier.value.kind === "loaded") assert.deepEqual(client.earlier.value.events, [older, oldest]);
  env.source.emit("snapshot", { ...snapshot, events: [] });
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });
});

test("a revoked stream stops the observed span at the last frame", async () => {
  const { env, client } = await live();
  env.source.emit("closed", { reason: "REVOKED" });
  assert.deepEqual(client.history.value?.gaps, [{ from: t0, to: null }]);
});

test("events identical but for their name stay distinct on the loaded page", async () => {
  const { env, client } = await live();
  const base = snapshot.events[0];
  assert.ok(base);
  const twin = (seq: number): ScalingEvent => eventAt(base, seq, t0);
  const held: typeof snapshot = { ...snapshot, events: [twin(10), twin(11), twin(12)], eventsLimit: 3 };
  env.source.emit("snapshot", held);
  env.responses.set(`${PATHS.listEvents}?pageSize=203`, { events: [twin(9)], nextPageToken: "t" });
  await client.loadEarlier();
  env.source.emit("delta", { seq: held.seq + 1, base: held.seq, updateTime: held.updateTime, events: [twin(13)] });
  assert.equal(client.earlier.value.kind, "loaded");
  if (client.earlier.value.kind === "loaded") assert.deepEqual(seqs(client.earlier.value.events), [9, 10]);
  assert.deepEqual(seqs(client.state.value?.events), [11, 12, 13]);
});

test("an empty earlier page retains events subsequently evicted from the stream", async () => {
  const { env, client } = await live();
  const base = snapshot.events[0];
  assert.ok(base);
  const event = (seq: number): ScalingEvent => eventAt(base, seq, t0);
  const held: typeof snapshot = { ...snapshot, events: [event(10), event(11)], eventsLimit: 3 };
  env.source.emit("snapshot", held);
  env.responses.set(`${PATHS.listEvents}?pageSize=202`, { events: [event(11), event(10)] });
  await client.loadEarlier();
  assert.deepEqual(client.earlier.value, { kind: "loaded", events: [], next: undefined, loading: false });
  env.source.emit("delta", { seq: held.seq + 1, base: held.seq, updateTime: held.updateTime, events: [event(12)] });
  env.source.emit("delta", { seq: held.seq + 2, base: held.seq + 1, updateTime: held.updateTime, events: [event(13)] });
  env.source.emit("delta", { seq: held.seq + 3, base: held.seq + 2, updateTime: held.updateTime, events: [event(14)] });
  assert.deepEqual(client.earlier.value, { kind: "loaded", events: [event(10), event(11)], next: undefined, loading: false });
  assert.deepEqual(seqs(client.state.value?.events), [12, 13, 14]);
});

test("a pending earlier page is discarded after a snapshot gap", async () => {
  const { env, client } = await live();
  const base = snapshot.events[0];
  assert.ok(base);
  const { promise, resolve } = Promise.withResolvers<unknown>();
  env.responses.set(`${PATHS.listEvents}?pageSize=${snapshot.events.length + 200}`, promise);
  const pending = client.loadEarlier();
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: true });
  env.source.emit("snapshot", { ...snapshot, events: [eventAt(base, 1000000, t0)] });
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });
  resolve({ events: [base], nextPageToken: "stale" });
  await pending;
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });
});
