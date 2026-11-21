import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test, type TestContext } from "node:test";
import { setImmediate as flush } from "node:timers/promises";
import { BUILDS_REFRESH_MS, HttpError, loginUrl, PAGE_MAX, StateClient, type Environment, type FrameData, type StreamSource } from "../src/client.ts";
import { idOf, ms } from "../src/wire-parse.ts";
import { PATHS, type Build, type Frame, type ScalingEvent, type Session } from "../src/wire.ts";
import { iso, load, snapshotOf } from "./sse.ts";

const session = JSON.parse(readFileSync(new URL("../fixtures/session.json", import.meta.url), "utf8")) as Session;
const defaultSession = { ...session, expireTime: undefined };
const anonymous: Session = { name: session.name, permissions: [] };
const snapshot = snapshotOf(load("live"));
const build = JSON.parse(readFileSync(new URL("../fixtures/build.json", import.meta.url), "utf8")) as Build;
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

interface Scheduled {
  readonly delay: number;
  fired: boolean;
  run(): void;
}

class FakeEnvironment implements Environment {
  readonly sources: FakeSource[] = [];
  readonly scheduled: Scheduled[] = [];
  readonly navigations: string[] = [];
  readonly responses = new Map<string, unknown>([[PATHS.getSession, defaultSession]]);
  readonly fetched: string[] = [];
  readonly posts: string[] = [];
  readonly postResults: (number | Error | Promise<number>)[] = [];
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
    const entry: Scheduled = { delay, fired: false, run: () => { entry.fired = true; run(); } };
    this.scheduled.push(entry);
    // As `clearTimeout`: cancelling removes a pending entry; a fired or already cancelled one is left alone.
    return () => {
      const i = this.scheduled.indexOf(entry);
      if (!entry.fired && i !== -1) this.scheduled.splice(i, 1);
    };
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

test("the fake environment cancels a pending entry, and nothing once it fired", () => {
  const env = new FakeEnvironment();
  const ran: string[] = [];
  const cancelA = env.schedule(() => ran.push("a"), 1);
  const cancelB = env.schedule(() => ran.push("b"), 2);
  cancelA();
  cancelA();
  assert.deepEqual(env.scheduled.map((s) => s.delay), [2]);
  env.scheduled[0]?.run();
  cancelB();
  assert.deepEqual(env.scheduled.map((s) => s.delay), [2]);
  assert.deepEqual(ran, ["b"]);
});

test("a forbidden initial session is revoked and never retries", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, new HttpError(403));
  const client = new StateClient(env);
  await client.start();
  const initialLink = client.link.value.kind;
  const requests = env.fetched.filter((path) => path === PATHS.getSession).length;
  for (const entry of env.scheduled.filter((scheduled) => !scheduled.fired)) {
    env.clock += entry.delay;
    entry.run();
  }
  await flush();
  assert.equal(initialLink, "revoked");
  assert.equal(requests, 1);
  assert.equal(env.fetched.filter((path) => path === PATHS.getSession).length, requests);
});

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

const loadedAccess = (client: StateClient) => {
  const state = client.access.value;
  assert.ok(state.kind === "loaded");
  return state.data;
};

const LIST = `${PATHS.listBuilds}?pageSize=200`;
/** A ten-minute session, admitted at the fake's start clock, renews after eight minutes. */
const EXPIRING = { expireTime: iso(1000 + 10 * 60_000) };
const RENEWAL_MS = 8 * 60_000;
const pathOf = (id: string): string => PATHS.getBuild.replace("{build}", encodeURIComponent(id));
const named = (id: string): Build => ({ ...build, name: `builds/${id}` });

const buildOnly = async (over: Partial<Session> = {}): Promise<{ env: FakeEnvironment; client: StateClient }> => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...defaultSession, permissions: ["builds.read"], ...over });
  const client = new StateClient(env);
  await client.start();
  return { env, client };
};

const pendingRefreshes = (env: FakeEnvironment): Scheduled[] =>
  env.scheduled.filter((entry) => entry.delay === BUILDS_REFRESH_MS && !entry.fired);

const probes = (env: FakeEnvironment): number => env.fetched.filter((path) => path === PATHS.getSession).length;

/** The delays of every timer still pending. */
const pendingDelays = (env: FakeEnvironment): number[] => env.scheduled.filter((entry) => !entry.fired).map((entry) => entry.delay);

/** Drives the existing request fake with mock timers, advancing its clock before callbacks run. */
const timedRenewals = (t: TestContext, env: FakeEnvironment): ((elapsed: number) => Promise<void>) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const schedule = env.schedule.bind(env);
  t.mock.method(env, "schedule", (run: () => void, delay: number) => {
    const cancel = schedule(run, delay);
    const entry = env.scheduled.at(-1);
    assert.ok(entry);
    const timer = setTimeout(() => entry.run(), delay);
    return () => {
      clearTimeout(timer);
      cancel();
    };
  });
  return async (elapsed) => {
    env.clock += elapsed;
    t.mock.timers.tick(elapsed);
    await flush();
  };
};

/** Answers a pending request: an `Error` rejects it, anything else is its body. */
const settle = (pending: PromiseWithResolvers<unknown>, answer: unknown): void => {
  if (answer instanceof Error) pending.reject(answer);
  else pending.resolve(answer);
};

/** Fires the one scheduled builds refresh without waiting for it. */
const startRefresh = (env: FakeEnvironment): void => {
  const pending = pendingRefreshes(env);
  assert.equal(pending.length, 1, "exactly one builds refresh is scheduled");
  const [entry] = pending;
  assert.ok(entry);
  entry.run();
};

const refreshBuilds = async (env: FakeEnvironment): Promise<void> => {
  startRefresh(env);
  await flush();
};

/** Fires the latest pending renewal (the clock must not have moved since that admission). */
const renew = async (env: FakeEnvironment): Promise<void> => {
  const entry = env.scheduled.filter((s) => s.delay === RENEWAL_MS && !s.fired).at(-1);
  assert.ok(entry, "a renewal is scheduled");
  entry.run();
  await flush();
};

/** An event of the ledger as `base` with another position and time. */
const eventAt = (base: ScalingEvent, seq: number, createTime: number): ScalingEvent => ({
  ...base,
  name: `events/${seq}`,
  createTime: iso(createTime),
});

const seqs = (events: readonly ScalingEvent[] | undefined): number[] => (events ?? []).map((e) => Number(idOf(e.name)));

test("build loading encodes ids, keeps the same detail during refresh, and handles 404", async () => {
  const env = new FakeEnvironment();
  const client = new StateClient(env);
  const id = "build/with space";
  const path = "/v1/builds/build%2Fwith%20space";
  env.responses.set(path, build);
  await client.loadBuild(id);
  assert.equal(env.fetched.at(-1), path);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 1000, failing: false });
  const refresh = client.loadBuild(id);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 1000, failing: false });
  await refresh;
  env.responses.set("/v1/builds/missing", new HttpError(404));
  await client.loadBuild("missing");
  assert.deepEqual(client.build.value, { kind: "missing", id: "missing" });
});

test("held builds load successive pages without duplicate names", async () => {
  const env = new FakeEnvironment();
  const client = new StateClient(env);
  env.responses.set("/v1/builds?pageSize=200", { builds: [build], nextPageToken: "next" });
  env.responses.set("/v1/builds?pageSize=200&pageToken=next", { builds: [build, { ...build, name: "builds/other" }] });
  await client.loadEarlierBuilds();
  await client.loadEarlierBuilds();
  const earlier = client.earlierBuilds.value;
  assert.equal(earlier.kind, "loaded");
  if (earlier.kind === "loaded") {
    assert.deepEqual(earlier.builds.map((b) => b.name), [build.name, "builds/other"]);
    assert.equal(earlier.next, undefined);
  }
});

test("a failed first page of held builds is an error, a failed later page keeps the list", async () => {
  const env = new FakeEnvironment();
  const client = new StateClient(env);
  env.responses.set("/v1/builds?pageSize=200", new HttpError(500));
  await client.loadEarlierBuilds();
  assert.deepEqual(client.earlierBuilds.value, { kind: "error", loading: false });
  env.responses.set("/v1/builds?pageSize=200", { builds: [build], nextPageToken: "next" });
  env.responses.set("/v1/builds?pageSize=200&pageToken=next", new HttpError(500));
  await client.loadEarlierBuilds();
  await client.loadEarlierBuilds();
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: "next", loading: false, asOf: 1000, failing: false });
});

test("build deltas refresh held rows and removals invalidate pages", async () => {
  const { env, client } = await live();
  client.earlierBuilds.value = { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: t0, failing: false };
  const updated = { ...build, state: "IDLE" as const };
  env.source.emit("delta", { seq: snapshot.seq + 1, base: snapshot.seq, updateTime: snapshot.updateTime, builds: { upsert: [updated] } });
  const held = client.earlierBuilds.value;
  assert.equal(held.kind, "loaded");
  if (held.kind === "loaded") assert.deepEqual(held.builds[0], updated);
  env.source.emit("delta", { seq: snapshot.seq + 2, base: snapshot.seq + 1, updateTime: snapshot.updateTime, builds: { remove: [build.name] } });
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
});

test("a snapshot invalidates held pages and discards their in-flight response", async () => {
  const { env, client } = await live();
  const { promise, resolve } = Promise.withResolvers<unknown>();
  env.responses.set("/v1/builds?pageSize=200", promise);
  const pending = client.loadEarlierBuilds();
  env.source.emit("snapshot", snapshot);
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
  resolve({ builds: [build] });
  await pending;
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
});

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

test("a fresh twenty-second token and its clipped ten-second tail renew at half-life, joining concurrent requests", async (t) => {
  for (const admittedAfter of [0, 10_000]) {
    await t.test(admittedAfter === 0 ? "fresh token" : "same deadline, clipped tail", async (t) => {
      const env = new FakeEnvironment();
      const deadline = env.clock + 20_000;
      env.responses.set(PATHS.getSession, { ...session, expireTime: iso(deadline) });
      env.clock += admittedAfter;
      const advance = timedRenewals(t, env);
      const client = new StateClient(env);
      t.after(() => client.stop());
      const answer = Promise.withResolvers<number>();
      env.postResults.push(answer.promise);
      await client.start();
      const delay = (20_000 - admittedAfter) / 2;
      assert.deepEqual(pendingDelays(env), [delay]);
      assert.deepEqual(env.posts, []);
      assert.equal(probes(env), 1);
      await advance(delay - 1);
      assert.deepEqual(env.posts, []);
      assert.equal(probes(env), 1);
      await advance(1);
      assert.deepEqual(env.posts, [PATHS.renew]);
      assert.equal(probes(env), 1, "the session is not probed before renewal succeeds");
      env.source.emit("closed", { reason: "EXPIRED" });
      await flush();
      assert.deepEqual(env.posts, [PATHS.renew], "expiry joins the scheduled renewal");
      answer.resolve(204);
      await flush();
      assert.deepEqual(env.posts, [PATHS.renew]);
      assert.deepEqual(env.fetched, [PATHS.getSession, PATHS.getSession]);
      assert.deepEqual(pendingDelays(env), [delay / 2], "renewal keeps the absolute deadline");
      assert.deepEqual(env.navigations, []);
    });
  }
});

test("a fixed twenty-second deadline halves repeatedly and ends on 401 without an immediate renewal burst", async (t) => {
  const env = new FakeEnvironment();
  const deadline = env.clock + 20_000;
  env.responses.set(PATHS.getSession, { ...session, expireTime: iso(deadline) });
  const advance = timedRenewals(t, env);
  const client = new StateClient(env);
  t.after(() => client.stop());
  await client.start();
  let requests = 0;
  for (const delay of [10_000, 5000, 2500, 1250]) {
    assert.deepEqual(pendingDelays(env), [delay]);
    await advance(delay - 1);
    assert.equal(env.posts.length, requests, "no request before the next half-life");
    await advance(1);
    requests += 1;
    assert.deepEqual(env.posts, Array(requests).fill(PATHS.renew));
    assert.equal(probes(env), requests + 1);
    assert.deepEqual(pendingDelays(env), [delay / 2]);
    await advance(0);
    assert.equal(env.posts.length, requests, "successful renewal does not cause a zero-delay burst");
    assert.deepEqual(env.navigations, []);
  }
  const answer = Promise.withResolvers<number>();
  env.postResults.push(answer.promise);
  await advance(624);
  assert.equal(env.posts.length, requests);
  await advance(1);
  requests += 1;
  assert.deepEqual(env.posts, Array(requests).fill(PATHS.renew));
  assert.equal(probes(env), requests);
  assert.deepEqual(pendingDelays(env), []);
  // The last request crosses the absolute deadline while the server is answering.
  await advance(deadline - env.clock);
  assert.equal(env.clock, deadline);
  assert.equal(env.posts.length, requests);
  answer.resolve(401);
  await flush();
  assert.deepEqual(env.navigations, [loginUrl(env.here())]);
  assert.deepEqual(pendingDelays(env), []);
  await advance(60_000);
  assert.deepEqual(env.posts, Array(requests).fill(PATHS.renew));
  assert.equal(probes(env), requests, "401 does not probe or re-admit the expired session");
  assert.deepEqual(env.navigations, [loginUrl("/ui/workers")]);
});

test("429 on a twenty-second token uses the existing bounded backoff, then 401 signs in", async (t) => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, expireTime: iso(env.clock + 20_000) });
  const advance = timedRenewals(t, env);
  const client = new StateClient(env);
  t.after(() => client.stop());
  const backoffs = [1000, 2000, 4000, 8000, 16_000, 30_000, 30_000];
  env.postResults.push(...backoffs.map(() => 429), 401);
  await client.start();
  await advance(9999);
  assert.deepEqual(env.posts, []);
  await advance(1);
  let requests = 1;
  assert.deepEqual(env.posts, [PATHS.renew]);
  for (const delay of backoffs) {
    assert.equal(client.link.value.kind, "lost");
    assert.deepEqual(pendingDelays(env), [delay]);
    assert.deepEqual(env.navigations, []);
    assert.equal(probes(env), 1, "429 retries renewal, not the session probe");
    await advance(delay - 1);
    assert.equal(env.posts.length, requests, "backoff never retries early");
    await advance(1);
    requests += 1;
    assert.deepEqual(env.posts, Array(requests).fill(PATHS.renew));
  }
  assert.equal(probes(env), 1);
  assert.deepEqual(env.navigations, [loginUrl("/ui/workers")]);
  assert.deepEqual(pendingDelays(env), []);
  await advance(60_000);
  assert.equal(env.posts.length, requests);
  assert.equal(probes(env), 1);
  assert.deepEqual(env.navigations, [loginUrl("/ui/workers")]);
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

test("visibility suspends the stream and resumes with a fresh snapshot without losing rows", async () => {
  const { env, client } = await live();
  const first = env.source;
  const links: string[] = [];
  const unsubscribe = client.link.subscribe((link) => links.push(link.kind));
  client.setVisible(false);
  client.setVisible(false);
  assert.equal(first.closed, true);
  assert.equal(client.state.value?.seq, snapshot.seq);
  const before = probes(env);
  first.fail();
  await flush();
  assert.equal(probes(env), before);
  env.clock += 10 * 60_000;
  client.tick();
  assert.deepEqual(pendingDelays(env), []);
  client.setVisible(true);
  client.setVisible(true);
  assert.equal(env.sources.length, 2);
  assert.equal(client.link.value.kind, "connecting");
  assert.equal(client.state.value?.seq, snapshot.seq);
  env.source.emit("snapshot", { ...snapshot, seq: snapshot.seq + 10, updateTime: iso(t0 + 10 * 60_000) });
  assert.equal(client.link.value.kind, "live");
  assert.equal(client.state.value?.seq, snapshot.seq + 10);
  assert.ok(!links.includes("lost"));
  unsubscribe();
});

test("a resumed stream failure is classified and backs off, and hiding cancels its retry", async () => {
  const { env, client } = await live();
  client.setVisible(false);
  client.setVisible(true);
  env.source.emit("snapshot", snapshot);
  env.source.fail();
  await flush();
  assert.equal(client.link.value.kind, "lost");
  assert.deepEqual(pendingDelays(env), [1000]);
  client.setVisible(false);
  assert.deepEqual(pendingDelays(env), []);
  client.setVisible(true);
  assert.equal(client.link.value.kind, "connecting");
  assert.equal(env.sources.length, 3);
  assert.deepEqual(pendingDelays(env), []);
});

test("a superseded failure probe cannot revoke or mark the resumed stream lost", async () => {
  for (const revoked of [false, true]) {
    const { env, client } = await live();
    const { promise, resolve, reject } = Promise.withResolvers<unknown>();
    env.responses.set(PATHS.getSession, promise);
    env.source.fail();
    client.setVisible(false);
    client.setVisible(true);
    env.source.emit("snapshot", { ...snapshot, seq: snapshot.seq + 1 });
    if (revoked) reject(new HttpError(403));
    else resolve(defaultSession);
    await flush();
    assert.equal(client.link.value.kind, "live");
    assert.equal(client.state.value?.seq, snapshot.seq + 1);
    assert.equal(env.sources.length, 2);
    assert.deepEqual(pendingDelays(env), []);
  }
});

test("hidden initial admission waits for visibility and stop prevents reopening", async () => {
  const env = new FakeEnvironment();
  const client = new StateClient(env);
  client.setVisible(false);
  await client.start();
  assert.equal(env.sources.length, 0);
  assert.equal(client.session.value?.principal, defaultSession.principal);
  client.setVisible(true);
  assert.equal(env.sources.length, 1);
  client.stop();
  assert.equal(env.source.closed, true);
  client.setVisible(false);
  client.setVisible(true);
  assert.equal(env.sources.length, 1);
  assert.deepEqual(pendingDelays(env), []);
});

test("visibility resumes a failed initial admission without retaining its retry", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, new Error("offline"));
  const client = new StateClient(env);
  await client.start();
  assert.deepEqual(pendingDelays(env), [1000]);
  client.setVisible(false);
  assert.deepEqual(pendingDelays(env), []);
  env.responses.set(PATHS.getSession, defaultSession);
  client.setVisible(true);
  await flush();
  assert.equal(env.sources.length, 1);
  assert.equal(client.link.value.kind, "connecting");
});

test("visibility changes do not bypass signed-out, forbidden or revoked admissions", async () => {
  for (const [response, kind] of [
    [anonymous, "signed_out"],
    [{ ...defaultSession, permissions: [] }, "forbidden"],
    [new HttpError(403), "revoked"],
  ] as const) {
    const env = new FakeEnvironment();
    env.responses.set(PATHS.getSession, response);
    const client = new StateClient(env);
    await client.start();
    client.setVisible(false);
    client.setVisible(true);
    await flush();
    assert.equal(client.link.value.kind, kind);
    assert.equal(env.sources.length, 0);
    assert.equal(probes(env), 1);
    assert.deepEqual(pendingDelays(env), []);
  }
});

test("hiding retains session renewal and its retry", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...defaultSession, ...EXPIRING });
  const client = new StateClient(env);
  await client.start();
  env.source.emit("snapshot", snapshot);
  client.setVisible(false);
  assert.deepEqual(pendingDelays(env), [RENEWAL_MS]);
  env.postResults.push(503);
  env.scheduled.find((entry) => entry.delay === RENEWAL_MS)?.run();
  await flush();
  assert.equal(client.link.value.kind, "live");
  assert.deepEqual(pendingDelays(env), [1000]);
  client.setVisible(true);
  assert.equal(env.sources.length, 2);
  assert.deepEqual(pendingDelays(env), [1000]);
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
  const status = { name: "awsEvents/source", createTime: snapshot.updateTime, fleet: "fleets/a", aws: { event: {
    id: "source", at: snapshot.updateTime, kind: { type: "capacityUnavailable" as const, reason: "capacity" },
  } } };
  env.source.emit("snapshot", { ...snapshot, statusEvents: [status] });
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
  env.source.emit("delta", { seq: snapshot.seq + 1, base: snapshot.seq, updateTime: snapshot.updateTime, statusEvents: [] });
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
  assert.equal(client.access.value.kind, "error");
  const { promise, resolve } = Promise.withResolvers<unknown>();
  env.responses.set(PATHS.listRoles, promise);
  const retry = client.loadAccess();
  assert.equal(client.access.value.kind, "loading");
  resolve({ roles: [] });
  await retry;
  assert.deepEqual(loadedAccess(client), { principals: [], nextPageToken: undefined, revoked: [], roles: [] });
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
  assert.deepEqual(loadedAccess(client).revoked, [b, c]);
  assert.deepEqual(
    loadedAccess(client).roles.map((r) => r.name),
    ["roles/viewer", "roles/admin"],
  );
  assert.equal(loadedAccess(client).nextPageToken, "p2");
  await client.loadMorePrincipals();
  assert.deepEqual(loadedAccess(client).principals, [a, b, c]);
  assert.equal(loadedAccess(client).nextPageToken, undefined);
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
  assert.equal(client.access.value.kind, "unloaded");
});

const principalRow = (name: string) => ({
  name: `principals/${name}`,
  provider: "google",
  principal: name,
  createTime: snapshot.updateTime,
  lastSeenTime: snapshot.updateTime,
});

test("a failed access refresh keeps its observations and retry replaces them", async () => {
  const { env, client } = await live();
  const [a, b] = [principalRow("a"), principalRow("b")];
  env.responses.set(PRINCIPALS, { principals: [a] });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  await client.loadAccess();
  const initial = client.access.value;
  assert.ok(initial.kind === "loaded");
  env.clock += 1000;
  const { promise, reject } = Promise.withResolvers<unknown>();
  env.responses.set(PATHS.listRoles, promise);
  const refresh = client.loadAccess();
  const refreshing = client.access.value;
  assert.ok(refreshing.kind === "loaded" && refreshing.loading);
  assert.deepEqual(loadedAccess(client).principals, [a]);
  reject(new Error("offline"));
  await refresh;
  const failed = client.access.value;
  assert.ok(failed.kind === "loaded" && failed.failing && !failed.loading);
  assert.equal(failed.asOf, initial.asOf);
  assert.deepEqual(loadedAccess(client).principals, [a]);
  env.clock += 1000;
  env.responses.set(PRINCIPALS, { principals: [b] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  await client.loadAccess();
  const retried = client.access.value;
  assert.ok(retried.kind === "loaded" && !retried.failing && !retried.loading);
  assert.equal(retried.asOf, client.serverNow());
  assert.ok(retried.asOf > initial.asOf);
  assert.deepEqual(loadedAccess(client).principals, [b]);
});

test("a superseded access answer cannot change a newer loading request", async () => {
  const { env, client } = await live();
  const old = Promise.withResolvers<unknown>();
  const next = Promise.withResolvers<unknown>();
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  env.responses.set(PRINCIPALS, old.promise);
  const first = client.loadAccess();
  env.responses.set(PRINCIPALS, next.promise);
  const second = client.loadAccess();
  old.resolve({ principals: [principalRow("old")] });
  await first;
  assert.equal(client.access.value.kind, "loading");
  next.resolve({ principals: [principalRow("new")] });
  await second;
  assert.deepEqual(loadedAccess(client).principals, [principalRow("new")]);
});

test("an obsolete refresh retains loaded observations and releases loading", async () => {
  const { env, client } = await live();
  const a = principalRow("a");
  env.responses.set(PRINCIPALS, { principals: [a] });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  await client.loadAccess();
  const initial = client.access.value;
  assert.ok(initial.kind === "loaded");
  const pending = Promise.withResolvers<unknown>();
  env.responses.set(PRINCIPALS, pending.promise);
  const refresh = client.loadAccess();
  env.source.emit("delta", {
    seq: snapshot.seq + 1,
    base: snapshot.seq,
    updateTime: snapshot.updateTime,
    principalsEpoch: snapshot.principalsEpoch + 1,
  });
  pending.resolve({ principals: [principalRow("obsolete")] });
  await refresh;
  const state = client.access.value;
  assert.ok(state.kind === "loaded" && state.failing && !state.loading);
  assert.equal(state.asOf, initial.asOf);
  assert.deepEqual(loadedAccess(client).principals, [a]);
});

for (const withState of [true, false]) {
  test(`access reload refreshes principals without an epoch change ${withState ? "with" : "without"} state.read`, async () => {
    let env: FakeEnvironment;
    let client: StateClient;
    if (withState) {
      ({ env, client } = await live());
    } else {
      env = new FakeEnvironment();
      env.responses.set(PATHS.getSession, { ...defaultSession, permissions: ["principals.read"] });
      client = new StateClient(env);
      await client.start();
    }
    const epoch = client.state.value?.principalsEpoch;
    const a = principalRow("a");
    const updatedA = { ...a, lastSeenTime: iso(ms(a.lastSeenTime) + 1000) };
    const b = principalRow("b");
    env.responses.set(PRINCIPALS, { principals: [a] });
    env.responses.set(REVOKED, { principals: [] });
    env.responses.set(PATHS.listRoles, { roles: [] });
    await client.loadAccess();
    assert.deepEqual(loadedAccess(client).principals, [a]);
    env.responses.set(PRINCIPALS, { principals: [updatedA, b] });
    await client.loadAccess();
    assert.equal(client.state.value?.principalsEpoch, epoch);
    assert.deepEqual(loadedAccess(client).principals, [updatedA, b]);
  });
}

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
  assert.ok(client.access.value.kind === "loaded" && client.access.value.moreLoading);
  const second = client.loadMorePrincipals();
  resolve({ principals: [b, c] });
  await Promise.all([first, second]);
  assert.deepEqual(loadedAccess(client).principals, [a, b, c]);
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
  await client.loadMorePrincipals();
  assert.deepEqual(loadedAccess(client).principals, [a]);
  assert.equal(loadedAccess(client).nextPageToken, "p2");
  assert.ok(client.access.value.kind === "loaded" && client.access.value.moreFailed && !client.access.value.moreLoading);
  env.responses.set(`${PRINCIPALS}&pageToken=p2`, { principals: [b] });
  await client.loadMorePrincipals();
  assert.deepEqual(loadedAccess(client).principals, [a, b]);
  assert.equal(loadedAccess(client).nextPageToken, undefined);
  assert.ok(client.access.value.kind === "loaded" && !client.access.value.moreFailed);
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
  env.responses.set(PRINCIPALS, { principals: [d], nextPageToken: "p3" });
  const next = Promise.withResolvers<unknown>();
  env.responses.set(`${PRINCIPALS}&pageToken=p3`, next.promise);
  await client.loadAccess();
  const newer = client.loadMorePrincipals();
  resolve({ principals: [b] });
  await loading;
  assert.deepEqual(loadedAccess(client).principals, [d]);
  assert.ok(client.access.value.kind === "loaded" && client.access.value.moreLoading);
  next.resolve({ principals: [a] });
  await newer;
  assert.deepEqual(loadedAccess(client).principals, [d, a]);
});

test("a re-admission without principals.read drops loaded and in-flight access", async () => {
  for (const [how, downgrade] of [
    ["state.read only", { ...session, permissions: ["state.read"], ...EXPIRING }],
    ["signed out", anonymous],
  ] as const) {
    for (const inFlight of [false, true]) {
      const env = new FakeEnvironment();
      env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
      const client = new StateClient(env);
      await client.start();
      const answer = Promise.withResolvers<unknown>();
      env.responses.set(PRINCIPALS, inFlight ? answer.promise : { principals: [principalRow("a")] });
      env.responses.set(REVOKED, { principals: [] });
      env.responses.set(PATHS.listRoles, { roles: [] });
      const loading = client.loadAccess();
      if (!inFlight) await loading;
      env.responses.set(PATHS.getSession, downgrade);
      await renew(env);
      answer.resolve({ principals: [principalRow("a")] });
      await loading;
      assert.deepEqual(client.access.value, { kind: "unloaded" }, `${how}, in flight: ${inFlight}`);
    }
  }
});

test("a re-admission that keeps principals.read keeps the loaded access", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
  const client = new StateClient(env);
  await client.start();
  env.responses.set(PRINCIPALS, { principals: [principalRow("a")] });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  await client.loadAccess();
  const loaded = client.access.value;
  await renew(env);
  assert.equal(client.access.value, loaded);
});

/** A session that may read access and nothing else: its link is forbidden, with nothing streamed or refreshed. */
const accessOnly = async (): Promise<{ env: FakeEnvironment; client: StateClient }> => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...defaultSession, permissions: ["principals.read"] });
  const client = new StateClient(env);
  await client.start();
  assert.equal(client.link.value.kind, "forbidden");
  env.responses.set(PRINCIPALS, { principals: [principalRow("a")], nextPageToken: "p2" });
  env.responses.set(REVOKED, { principals: [] });
  env.responses.set(PATHS.listRoles, { roles: [] });
  return { env, client };
};

/** Has the first principals page, or the next one after a loaded first page, refused with `status`. */
const refuseAccess = async (env: FakeEnvironment, client: StateClient, page: "first" | "more", status: number): Promise<void> => {
  if (page === "first") {
    env.responses.set(PRINCIPALS, new HttpError(status));
    await client.loadAccess();
  } else {
    await client.loadAccess();
    env.responses.set(`${PRINCIPALS}&pageToken=p2`, new HttpError(status));
    await client.loadMorePrincipals();
  }
  await flush();
};

test("access refusals on a principals.read-only session classify revocation", async () => {
  for (const page of ["first", "more"] as const) {
    for (const status of [401, 403]) {
      const { env, client } = await accessOnly();
      env.responses.set(PATHS.getSession, new HttpError(403));
      await refuseAccess(env, client, page, status);
      assert.equal(client.link.value.kind, "revoked", `${page} ${status}`);
    }
  }
});

test("access refusals renew an expired principals.read-only session", async () => {
  for (const page of ["first", "more"] as const) {
    const { env, client } = await accessOnly();
    env.responses.set(PATHS.getSession, { ...anonymous, login: session.login });
    env.postResults.push(503);
    await refuseAccess(env, client, page, 401);
    assert.deepEqual(env.posts, [PATHS.renew], page);
    assert.equal(client.link.value.kind, "forbidden", page);
    assert.deepEqual(pendingDelays(env), [1000], page);
  }
});

test("a re-admission after an access refusal reloads an errored or failing access", async () => {
  for (const shown of ["error", "failing"] as const) {
    const { env, client } = await accessOnly();
    if (shown === "failing") await client.loadAccess();
    const probe = Promise.withResolvers<unknown>();
    env.responses.set(PATHS.getSession, probe.promise);
    env.responses.set(PRINCIPALS, new HttpError(401));
    await client.loadAccess();
    const refusedAccess = client.access.value;
    assert.ok(shown === "error" ? refusedAccess.kind === "error" : refusedAccess.kind === "loaded" && refusedAccess.failing, shown);
    env.responses.set(PATHS.getSession, { ...defaultSession, permissions: ["principals.read"] });
    env.responses.set(PRINCIPALS, { principals: [principalRow("b")] });
    probe.resolve({ ...anonymous, login: session.login });
    await flush();
    assert.deepEqual(env.posts, [PATHS.renew], shown);
    assert.deepEqual(loadedAccess(client).principals, [principalRow("b")], shown);
    assert.ok(client.access.value.kind === "loaded" && !client.access.value.failing, shown);
  }
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

test("earlier history overflow invalidates a pending page and reloads from the stream head", async () => {
  const { env, client } = await live();
  const base = snapshot.events[0];
  assert.ok(base);
  const event = (seq: number): ScalingEvent => eventAt(base, seq, t0);
  const held: typeof snapshot = { ...snapshot, events: [event(10), event(11)], eventsLimit: 2 };
  env.source.emit("snapshot", held);
  const first = `${PATHS.listEvents}?pageSize=202`;
  const next = `${PATHS.listEvents}?pageSize=200&pageToken=old-token`;
  env.responses.set(first, { events: [event(11), event(10), event(9)], nextPageToken: "old-token" });
  await client.loadEarlier();

  // Keep the page exactly at the retention limit before starting the stale request.
  for (let seq = 12; seq <= 5010; seq++) {
    const frame = held.seq + seq - 11;
    env.source.emit("delta", { seq: frame, base: frame - 1, updateTime: held.updateTime, events: [event(seq)] });
  }
  assert.equal(client.earlier.value.kind, "loaded");
  if (client.earlier.value.kind === "loaded") {
    assert.equal(client.earlier.value.events.length, 5000);
    assert.equal(client.earlier.value.next, "old-token");
  }
  assert.deepEqual(seqs(client.state.value?.events), [5009, 5010]);

  const pendingPage = Promise.withResolvers<unknown>();
  env.responses.set(next, pendingPage.promise);
  const pending = client.loadEarlier();
  assert.equal(client.earlier.value.loading, true);
  const finalFrame = held.seq + 5000;
  env.source.emit("delta", { seq: finalFrame, base: finalFrame - 1, updateTime: held.updateTime, events: [event(5011)] });
  assert.equal(client.earlier.value.kind, "unloaded");
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });

  pendingPage.resolve({ events: [event(8)], nextPageToken: "stale-token" });
  await pending;
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });
  assert.deepEqual(seqs(client.state.value?.events), [5010, 5011]);

  env.responses.set(first, {
    events: Array.from({ length: 202 }, (_, i) => event(5011 - i)),
    nextPageToken: "fresh-token",
  });
  await client.loadEarlier();
  assert.deepEqual(client.earlier.value, {
    kind: "loaded",
    events: Array.from({ length: 200 }, (_, i) => event(4810 + i)),
    next: "fresh-token",
    loading: false,
  });
  assert.deepEqual(env.fetched.filter((path) => path.startsWith(PATHS.listEvents)), [first, next, first]);
});

test("loading earlier pages past the retention limit restarts from the stream head", async () => {
  const { env, client } = await live();
  const base = snapshot.events[0];
  assert.ok(base);
  const event = (seq: number): ScalingEvent => eventAt(base, seq, t0);
  env.source.emit("snapshot", { ...snapshot, events: [event(6001), event(6002)] });
  const first = `${PATHS.listEvents}?pageSize=202`;
  const next = `${PATHS.listEvents}?pageSize=200&pageToken=next`;
  env.responses.set(first, {
    events: Array.from({ length: 202 }, (_, i) => event(6002 - i)),
    nextPageToken: "next",
  });
  await client.loadEarlier();
  for (let page = 1; page <= 25; page++) {
    env.responses.set(next, {
      events: Array.from({ length: 200 }, (_, i) => event(5800 - (page - 1) * 200 - i)),
      nextPageToken: "next",
    });
    await client.loadEarlier();
    if (page === 24) {
      assert.equal(client.earlier.value.kind, "loaded");
      if (client.earlier.value.kind === "loaded") assert.equal(client.earlier.value.events.length, 5000);
    }
  }
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });
  await client.loadEarlier();
  assert.equal(env.fetched.at(-1), first);
  assert.equal(client.earlier.value.kind, "loaded");
  if (client.earlier.value.kind === "loaded") {
    assert.equal(client.earlier.value.events.length, 200);
    assert.equal(client.earlier.value.next, "next");
  }
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

test("a build-only session reloads the open build until it is idle", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  env.responses.set(pathOf(id), build);
  await client.loadBuild(id);
  const updated: Build = { ...build, state: "IDLE", usage: { ...build.usage, executions: { ...build.usage.executions, completed: build.usage.executions.completed + 1 } } };
  env.responses.set(pathOf(id), updated);
  env.clock = 6_000;
  await refreshBuilds(env);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build: updated, asOf: 6_000, failing: false });
  assert.equal(pendingRefreshes(env).length, 1);
});

test("stop ends the builds refresh, also one in flight", async () => {
  const first = await buildOnly();
  assert.equal(pendingRefreshes(first.env).length, 1);
  first.client.stop();
  assert.equal(pendingRefreshes(first.env).length, 0);
  const { env, client } = await buildOnly();
  env.responses.set(LIST, { builds: [build] });
  await client.loadEarlierBuilds();
  const answer = Promise.withResolvers<unknown>();
  env.responses.set(LIST, answer.promise);
  startRefresh(env);
  client.stop();
  answer.resolve({ builds: [build] });
  await flush();
  assert.equal(pendingRefreshes(env).length, 0);
});

test("every admission leaves exactly one builds refresh", async () => {
  const { env, client } = await buildOnly(EXPIRING);
  await renew(env);
  await renew(env);
  assert.equal(pendingRefreshes(env).length, 1);
  env.responses.set(LIST, { builds: [build] });
  await client.loadEarlierBuilds();
  const answer = Promise.withResolvers<unknown>();
  env.responses.set(LIST, answer.promise);
  startRefresh(env);
  await renew(env);
  answer.resolve({ builds: [build] });
  await flush();
  assert.equal(pendingRefreshes(env).length, 1);
});

test("load earlier during a refresh continues the page its token belongs to, whichever answers first", async () => {
  for (const refreshFirst of [true, false]) {
    const { env, client } = await buildOnly();
    const [b1, b2, b3, n1] = ["b1", "b2", "b3", "n1"].map(named) as [Build, Build, Build, Build];
    env.responses.set(LIST, { builds: [b1, b2], nextPageToken: "tok" });
    await client.loadEarlierBuilds();
    const refresh = Promise.withResolvers<unknown>();
    const page = Promise.withResolvers<unknown>();
    env.responses.set(LIST, refresh.promise);
    env.responses.set(`${LIST}&pageToken=tok`, page.promise);
    startRefresh(env);
    const loading = client.loadEarlierBuilds();
    if (refreshFirst) {
      refresh.resolve({ builds: [n1, b1], nextPageToken: "tok-new" });
      await flush();
      page.resolve({ builds: [b3] });
    } else {
      page.resolve({ builds: [b3] });
      await flush();
      refresh.resolve({ builds: [n1, b1], nextPageToken: "tok-new" });
    }
    await loading;
    await flush();
    assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [b1, b2, b3], next: undefined, loading: false, asOf: 1000, failing: false }, `refreshFirst=${refreshFirst}`);
    assert.equal(pendingRefreshes(env).length, 1);
  }
});

test("a refresh leaves a loading page alone and the later page preserves failed freshness", async () => {
  const { env, client } = await buildOnly();
  const b1 = named("b1");
  const b3 = named("b3");
  env.responses.set(LIST, { builds: [b1], nextPageToken: "tok" });
  await client.loadEarlierBuilds();
  env.clock = 4_000;
  env.responses.set(LIST, new HttpError(500));
  await refreshBuilds(env);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [b1], next: "tok", loading: false, asOf: 1000, failing: true });
  const page = Promise.withResolvers<unknown>();
  env.responses.set(`${LIST}&pageToken=tok`, page.promise);
  const loading = client.loadEarlierBuilds();
  const asked = env.fetched.length;
  await refreshBuilds(env);
  assert.equal(env.fetched.length, asked);
  assert.equal(client.earlierBuilds.value.loading, true);
  env.clock = 8_000;
  page.resolve({ builds: [b3] });
  await loading;
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [b1, b3], next: undefined, loading: false, asOf: 1000, failing: true });
  assert.equal(pendingRefreshes(env).length, 1);
});

test("each build-only resource says its own as of", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  env.responses.set(LIST, { builds: [build] });
  env.responses.set(pathOf(id), build);
  env.clock = 10_000;
  await client.loadEarlierBuilds();
  env.clock = 20_000;
  await client.loadBuild(id);
  env.clock = 30_000;
  env.responses.set(LIST, new HttpError(500));
  await refreshBuilds(env);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: 10_000, failing: true });
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 30_000, failing: false });
  env.clock = 40_000;
  env.responses.set(LIST, { builds: [build] });
  env.responses.set(pathOf(id), new HttpError(500));
  await refreshBuilds(env);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: 40_000, failing: false });
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 30_000, failing: true });
  env.clock = 50_000;
  await refreshBuilds(env);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 30_000, failing: true });
});

test("a refresh with nothing loaded asks for nothing and changes nothing", async () => {
  const { env, client } = await buildOnly();
  const asked = env.fetched.length;
  await refreshBuilds(env);
  assert.equal(env.fetched.length, asked);
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
  assert.equal(client.build.value, null);
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a refresh superseded by opening another build marks nothing failing", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  const other = named("other");
  env.responses.set(pathOf(id), build);
  env.responses.set(pathOf("other"), other);
  await client.loadBuild(id);
  env.responses.set(pathOf(id), new Error("offline"));
  await refreshBuilds(env);
  assert.equal(client.build.value?.kind, "loaded");
  assert.equal(client.build.value?.kind === "loaded" && client.build.value.failing, true);
  const answer = Promise.withResolvers<unknown>();
  env.responses.set(pathOf(id), answer.promise);
  startRefresh(env);
  env.clock = 7_000;
  await client.loadBuild("other");
  answer.reject(new Error("offline"));
  await flush();
  assert.deepEqual(client.build.value, { kind: "loaded", id: "other", build: other, asOf: 7_000, failing: false });
});

test("a refreshed build that is no longer held is missing, and is not asked for again", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  env.responses.set(pathOf(id), build);
  await client.loadBuild(id);
  env.responses.set(pathOf(id), new HttpError(404));
  await refreshBuilds(env);
  assert.deepEqual(client.build.value, { kind: "missing", id });
  const asked = env.fetched.length;
  await refreshBuilds(env);
  assert.equal(env.fetched.length, asked);
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a build-only refresh replaces the held builds with the server's", async () => {
  const { env, client } = await buildOnly();
  const kept = named("kept");
  const evicted = named("evicted");
  const added = named("added");
  env.responses.set(LIST, { builds: [kept, evicted] });
  await client.loadEarlierBuilds();
  const idle = { ...kept, state: "IDLE" as const };
  env.responses.set(LIST, { builds: [added, idle] });
  env.clock = 9_000;
  await refreshBuilds(env);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [added, idle], next: undefined, loading: false, asOf: 9_000, failing: false });
  assert.equal(pendingRefreshes(env).length, 1);
});

test("only a signed-in session with builds.read and without state.read refreshes builds", async () => {
  for (const answer of [
    { ...defaultSession, permissions: ["state.read", "builds.read"] },
    { ...defaultSession, permissions: [] },
    anonymous,
  ]) {
    const env = new FakeEnvironment();
    env.responses.set(PATHS.getSession, answer);
    await new StateClient(env).start();
    assert.equal(pendingRefreshes(env).length, 0);
  }
});

test("a refresh asks for the held builds and the open build together", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  env.responses.set(LIST, { builds: [build] });
  env.responses.set(pathOf(id), build);
  await client.loadEarlierBuilds();
  await client.loadBuild(id);
  const list = Promise.withResolvers<unknown>();
  env.responses.set(LIST, list.promise);
  const asked = env.fetched.length;
  startRefresh(env);
  await flush();
  assert.deepEqual(env.fetched.slice(asked), [LIST, pathOf(id)]);
  assert.equal(pendingRefreshes(env).length, 0);
  list.resolve({ builds: [build] });
  await flush();
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a closed build is neither refreshed nor overwritten by its pending load", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  env.responses.set(pathOf(id), build);
  await client.loadBuild(id);
  client.closeBuild();
  assert.equal(client.build.value, null);
  const asked = env.fetched.length;
  await refreshBuilds(env);
  assert.equal(env.fetched.length, asked);
  const answer = Promise.withResolvers<unknown>();
  env.responses.set(pathOf(id), answer.promise);
  const pending = client.loadBuild(id);
  client.closeBuild();
  answer.resolve(build);
  await pending;
  assert.equal(client.build.value, null);
});

test("a builds refresh never asks for more than the largest page", async () => {
  const { env, client } = await buildOnly();
  const rows = Array.from({ length: PAGE_MAX + 1 }, (_, i) => named(String(i)));
  client.earlierBuilds.value = { kind: "loaded", builds: rows, next: "rest", loading: false, asOf: 1000, failing: false };
  env.responses.set(`${PATHS.listBuilds}?pageSize=${PAGE_MAX}`, { builds: rows.slice(0, PAGE_MAX), nextPageToken: "rest" });
  await refreshBuilds(env);
  assert.equal(env.fetched.at(-1), `${PATHS.listBuilds}?pageSize=${PAGE_MAX}`);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: rows.slice(0, PAGE_MAX), next: "rest", loading: false, asOf: 1000, failing: false });
  env.responses.set(`${LIST}&pageToken=rest`, { builds: [rows[PAGE_MAX]] });
  await client.loadEarlierBuilds();
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: rows, next: undefined, loading: false, asOf: 1000, failing: false });
});

test("losing state.read resets partial held builds and drops the streamed state", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, permissions: ["state.read", "builds.read"], ...EXPIRING });
  const client = new StateClient(env);
  await client.start();
  env.source.emit("snapshot", snapshot);
  const stream = env.source;
  env.responses.set(LIST, { builds: [] });
  await client.loadEarlierBuilds();
  stream.emit("delta", { seq: snapshot.seq + 1, base: snapshot.seq, updateTime: snapshot.updateTime, builds: { upsert: [build] } });
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [], next: undefined, loading: false, asOf: t0, failing: false });
  assert.ok(client.state.value?.builds.some((row) => row.name === build.name));
  env.responses.set(PATHS.getSession, { ...session, permissions: ["builds.read"], ...EXPIRING });
  await renew(env);
  assert.equal(stream.closed, true);
  assert.equal(env.sources.length, 1);
  assert.equal(client.state.value, null);
  assert.deepEqual(client.earlier.value, { kind: "unloaded", loading: false });
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
  assert.equal(client.link.value.kind, "forbidden");
  assert.equal(pendingRefreshes(env).length, 1);
  stream.emit("snapshot", snapshot);
  assert.equal(client.state.value, null);
});

test("gaining state.read clears held pages, connects and freezes if the stream closes before a snapshot", async () => {
  const { env, client } = await buildOnly(EXPIRING);
  const id = idOf(build.name);
  env.responses.set(pathOf(id), build);
  env.responses.set(LIST, { builds: [build] });
  await client.loadEarlierBuilds();
  await client.loadBuild(id);
  env.responses.set(pathOf(id), new HttpError(500));
  await refreshBuilds(env);
  env.responses.set(pathOf(id), build);
  env.responses.set(PATHS.getSession, { ...defaultSession, permissions: ["state.read", "builds.read"], ...EXPIRING });
  await renew(env);
  assert.equal(pendingRefreshes(env).length, 0);
  assert.equal(env.sources.length, 1);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 1000, failing: false });
  assert.equal(client.link.value.kind, "connecting");
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
  env.source.emit("closed", { reason: "VIEWER_CAP", retryDelay: "5s" });
  assert.equal(client.link.value.kind, "lost");
});

test("a session that loses builds.read drops the builds it was shown", async () => {
  const { env, client } = await buildOnly(EXPIRING);
  const id = idOf(build.name);
  env.responses.set(LIST, { builds: [build] });
  env.responses.set(pathOf(id), build);
  await client.loadEarlierBuilds();
  await client.loadBuild(id);
  env.responses.set(PATHS.getSession, { ...defaultSession, permissions: [], ...EXPIRING });
  await renew(env);
  assert.equal(client.build.value, null);
  assert.deepEqual(client.earlierBuilds.value, { kind: "unloaded", loading: false });
  assert.equal(client.link.value.kind, "forbidden");
  assert.equal(pendingRefreshes(env).length, 0);
});

test("a failed renewal leaves a build-only session's pages and refresh running", async () => {
  const { env, client } = await buildOnly(EXPIRING);
  env.responses.set(LIST, { builds: [build] });
  await client.loadEarlierBuilds();
  env.postResults.push(503);
  await renew(env);
  assert.equal(client.link.value.kind, "forbidden");
  assert.deepEqual(env.scheduled.filter((s) => !s.fired).map((s) => s.delay).sort((a, b) => a - b), [1000, BUILDS_REFRESH_MS]);
  env.clock = 6_000;
  await refreshBuilds(env);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: 6_000, failing: false });
});

test("a refresh refused for an expired session renews it and refreshes again", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  env.responses.set(pathOf(id), build);
  await client.loadBuild(id);
  env.responses.set(pathOf(id), new HttpError(401));
  env.responses.set(PATHS.getSession, { ...anonymous, login: session.login });
  env.postResults.push(503);
  await refreshBuilds(env);
  assert.deepEqual(env.posts, [PATHS.renew]);
  assert.equal(client.link.value.kind, "forbidden");
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 1000, failing: true });
  assert.equal(pendingRefreshes(env).length, 0);
  env.responses.set(PATHS.getSession, { ...defaultSession, permissions: ["builds.read"] });
  env.responses.set(pathOf(id), build);
  env.clock = 3_000;
  const retry = env.scheduled.find((s) => s.delay === 1000 && !s.fired);
  assert.ok(retry);
  retry.run();
  await flush();
  assert.deepEqual(env.posts, [PATHS.renew, PATHS.renew]);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 3_000, failing: false });
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a refused refresh is classified by the session, as a failed stream is", async () => {
  const rows = [
    { name: "still permitted", answer: { ...defaultSession, permissions: ["builds.read"] }, link: "forbidden", kept: true, refreshes: 1 },
    { name: "no session answer", answer: new Error("offline"), link: "forbidden", kept: true, refreshes: 1 },
    { name: "builds.read removed", answer: { ...defaultSession, permissions: [] }, link: "forbidden", kept: false, refreshes: 0 },
    { name: "signed out", answer: anonymous, link: "signed_out", kept: false, refreshes: 0 },
    { name: "revoked", answer: new HttpError(403), link: "revoked", kept: true, refreshes: 0 },
  ];
  for (const row of rows) {
    const { env, client } = await buildOnly();
    const id = idOf(build.name);
    env.responses.set(LIST, { builds: [build] });
    env.responses.set(pathOf(id), build);
    await client.loadEarlierBuilds();
    await client.loadBuild(id);
    env.responses.set(pathOf(id), new HttpError(403));
    env.responses.set(PATHS.getSession, row.answer);
    await refreshBuilds(env);
    assert.equal(client.link.value.kind, row.link, row.name);
    assert.equal(pendingRefreshes(env).length, row.refreshes, row.name);
    assert.equal(client.build.value?.kind === "loaded" && client.build.value.failing, row.kept, row.name);
    assert.equal(client.earlierBuilds.value.kind, row.kept ? "loaded" : "unloaded", row.name);
    assert.deepEqual(env.posts, [], row.name);
  }
});

test("a failed reload keeps the build shown, marked failing; a failed first load is an error", async () => {
  const env = new FakeEnvironment();
  const client = new StateClient(env);
  const id = idOf(build.name);
  env.responses.set(pathOf(id), build);
  await client.loadBuild(id);
  env.responses.set(pathOf(id), new Error("offline"));
  await client.loadBuild(id);
  assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 1000, failing: true });
  env.responses.set(pathOf("other"), new Error("offline"));
  await client.loadBuild("other");
  assert.deepEqual(client.build.value, { kind: "error", id: "other" });
});

test("a builds refresh retains every loaded row between one page and the server limit", async () => {
  const { env, client } = await buildOnly();
  const rows = Array.from({ length: 400 }, (_, i) => named(String(i)));
  client.earlierBuilds.value = { kind: "loaded", builds: rows, next: undefined, loading: false, asOf: 1000, failing: false };
  const path = `${PATHS.listBuilds}?pageSize=400`;
  env.responses.set(path, { builds: rows });
  await refreshBuilds(env);
  assert.equal(env.fetched.at(-1), path);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: rows, next: undefined, loading: false, asOf: 1000, failing: false });
});

test("a refused held-builds refresh classifies a list-only session", async () => {
  for (const status of [401, 403]) {
    const { env, client } = await buildOnly();
    env.responses.set(LIST, { builds: [build] });
    await client.loadEarlierBuilds();
    env.responses.set(LIST, new HttpError(status));
    env.responses.set(PATHS.getSession, new HttpError(403));
    await refreshBuilds(env);
    assert.equal(client.build.value, null);
    assert.equal(client.link.value.kind, "revoked", `list ${status}`);
    assert.equal(pendingRefreshes(env).length, 0);
    assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: 1000, failing: true });
  }
});

test("first-load build and list refusals classify revocation and stop refreshing", async () => {
  for (const resource of ["build", "list"] as const) {
    for (const status of [401, 403]) {
      const { env, client } = await buildOnly();
      const id = idOf(build.name);
      env.responses.set(resource === "build" ? pathOf(id) : LIST, new HttpError(status));
      env.responses.set(PATHS.getSession, new HttpError(403));
      if (resource === "build") await client.loadBuild(id);
      else await client.loadEarlierBuilds();
      await flush();
      assert.equal(client.link.value.kind, "revoked", `${resource} ${status}`);
      assert.equal(pendingRefreshes(env).length, 0);
    }
  }
});

test("first-load build and list refusals renew an expired build-only session", async () => {
  for (const resource of ["build", "list"] as const) {
    const { env, client } = await buildOnly();
    const id = idOf(build.name);
    env.responses.set(resource === "build" ? pathOf(id) : LIST, new HttpError(401));
    env.responses.set(PATHS.getSession, { ...anonymous, login: session.login });
    env.postResults.push(503);
    if (resource === "build") await client.loadBuild(id);
    else await client.loadEarlierBuilds();
    await flush();
    assert.deepEqual(env.posts, [PATHS.renew], resource);
    assert.equal(client.link.value.kind, "forbidden", resource);
    assert.equal(pendingRefreshes(env).length, 0, resource);
    assert.ok(env.scheduled.some((entry) => entry.delay === 1000 && !entry.fired), resource);
  }
});

test("re-admission retries an errored open build and an errored first list page", async () => {
  for (const resource of ["build", "list"] as const) {
    const { env, client } = await buildOnly();
    const id = idOf(build.name);
    const path = resource === "build" ? pathOf(id) : LIST;
    const probe = Promise.withResolvers<unknown>();
    env.responses.set(path, new HttpError(403));
    env.responses.set(PATHS.getSession, probe.promise);
    if (resource === "build") await client.loadBuild(id);
    else await client.loadEarlierBuilds();
    assert.equal(pendingRefreshes(env).length, 0, resource);
    env.responses.set(path, resource === "build" ? build : { builds: [build] });
    probe.resolve({ ...defaultSession, permissions: ["builds.read"] });
    await flush();
    assert.equal(client.link.value.kind, "forbidden", resource);
    assert.equal(pendingRefreshes(env).length, 1, resource);
    if (resource === "build") {
      assert.deepEqual(client.build.value, { kind: "loaded", id, build, asOf: 1000, failing: false });
    } else {
      assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: 1000, failing: false });
    }
  }
});

test("a refused first page of held builds is retried once per admission, not in a loop", async () => {
  const { env, client } = await buildOnly();
  env.responses.set(LIST, new HttpError(403));
  await client.loadEarlierBuilds();
  await flush();
  assert.equal(env.fetched.filter((path) => path === LIST).length, 2);
  assert.equal(probes(env), 2);
  assert.deepEqual(client.earlierBuilds.value, { kind: "error", loading: false });
  assert.equal(client.link.value.kind, "forbidden");
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a superseded or closed build load's refusal never reaches the session probe", async () => {
  for (const how of ["superseded", "closed"] as const) {
    const { env, client } = await buildOnly();
    const id = idOf(build.name);
    const answer = Promise.withResolvers<unknown>();
    env.responses.set(pathOf(id), answer.promise);
    env.responses.set(pathOf("other"), build);
    const first = client.loadBuild(id);
    if (how === "superseded") await client.loadBuild("other");
    else client.closeBuild();
    answer.reject(new HttpError(403));
    await first;
    await flush();
    assert.equal(probes(env), 1, how);
    assert.equal(client.link.value.kind, "forbidden", how);
    assert.equal(pendingRefreshes(env).length, 1, how);
    assert.deepEqual(client.build.value, how === "superseded" ? { kind: "loaded", id: "other", build, asOf: 1000, failing: false } : null, how);
  }
});

test("a stopped client ignores the refusal of a request in flight", async () => {
  for (const resource of ["build", "list"] as const) {
    const { env, client } = await buildOnly();
    const id = idOf(build.name);
    const answer = Promise.withResolvers<unknown>();
    env.responses.set(resource === "build" ? pathOf(id) : LIST, answer.promise);
    const loading = resource === "build" ? client.loadBuild(id) : client.loadEarlierBuilds();
    client.stop();
    answer.reject(new HttpError(403));
    await loading;
    await flush();
    assert.equal(probes(env), 1, resource);
    assert.equal(pendingRefreshes(env).length, 0, resource);
  }
});

test("a held-builds refresh superseded by loading the next page hands no refusal to the session", async () => {
  const { env, client } = await buildOnly();
  const [b1, b3] = ["b1", "b3"].map(named) as [Build, Build];
  env.responses.set(LIST, { builds: [b1], nextPageToken: "tok" });
  await client.loadEarlierBuilds();
  const refresh = Promise.withResolvers<unknown>();
  const page = Promise.withResolvers<unknown>();
  env.responses.set(LIST, refresh.promise);
  env.responses.set(`${LIST}&pageToken=tok`, page.promise);
  startRefresh(env);
  const loading = client.loadEarlierBuilds();
  refresh.reject(new HttpError(403));
  await flush();
  page.resolve({ builds: [b3] });
  await loading;
  await flush();
  assert.equal(probes(env), 1);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [b1, b3], next: undefined, loading: false, asOf: 1000, failing: false });
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a refused build or list request on a streamed session never reopens the stream", async () => {
  for (const resource of ["build", "list"] as const) {
    const { env, client } = await live();
    const id = idOf(build.name);
    env.responses.set(resource === "build" ? pathOf(id) : LIST, new HttpError(403));
    if (resource === "build") await client.loadBuild(id);
    else await client.loadEarlierBuilds();
    await flush();
    assert.equal(env.sources.length, 1, resource);
    assert.equal(probes(env), 1, resource);
    assert.equal(client.link.value.kind, "live", resource);
  }
});

test("a streamed renewal keeps the live or lost link and the loaded held builds until the new snapshot", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
  const client = new StateClient(env);
  await client.start();
  env.source.emit("snapshot", snapshot);
  env.responses.set(LIST, { builds: [build] });
  await client.loadEarlierBuilds();
  const loaded = client.earlierBuilds.value;
  assert.equal(loaded.kind, "loaded");
  await renew(env);
  assert.equal(env.sources.length, 2);
  assert.equal(client.link.value.kind, "live");
  assert.equal(client.earlierBuilds.value, loaded);
  env.source.emit("closed", { reason: "EXPIRED" });
  await flush();
  assert.equal(env.sources.length, 3);
  assert.equal(client.link.value.kind, "lost");
  assert.equal(client.earlierBuilds.value, loaded);
});

test("a newer first-load refusal supersedes an in-flight refusal probe", async () => {
  const { env, client } = await buildOnly();
  const id = idOf(build.name);
  const first = Promise.withResolvers<unknown>();
  const second = Promise.withResolvers<unknown>();
  env.responses.set(pathOf(id), new HttpError(401));
  env.responses.set(LIST, new HttpError(403));
  env.responses.set(PATHS.getSession, first.promise);
  await client.loadBuild(id);
  env.responses.set(PATHS.getSession, second.promise);
  await client.loadEarlierBuilds();
  assert.equal(pendingRefreshes(env).length, 0);
  second.reject(new Error("offline"));
  first.resolve(anonymous);
  await flush();
  assert.equal(client.link.value.kind, "forbidden");
  assert.equal(pendingRefreshes(env).length, 1);
  assert.deepEqual(env.posts, []);
});

test("a build-only renewal preserves its loaded held builds", async () => {
  const { env, client } = await buildOnly(EXPIRING);
  env.responses.set(LIST, { builds: [build] });
  await client.loadEarlierBuilds();
  await renew(env);
  assert.deepEqual(client.earlierBuilds.value, { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: 1000, failing: false });
  assert.equal(pendingRefreshes(env).length, 1);
});

test("a failed renewal after revocation never replaces the revoked link", async () => {
  for (const source of ["builds", "stream"] as const) {
    let env: FakeEnvironment;
    let client: StateClient;
    if (source === "builds") {
      ({ env, client } = await buildOnly(EXPIRING));
      env.responses.set(LIST, { builds: [build] });
      await client.loadEarlierBuilds();
      env.responses.set(LIST, new HttpError(403));
      env.responses.set(PATHS.getSession, new HttpError(403));
      await refreshBuilds(env);
    } else {
      env = new FakeEnvironment();
      env.responses.set(PATHS.getSession, { ...session, permissions: ["state.read", "builds.read"], ...EXPIRING });
      client = new StateClient(env);
      await client.start();
      env.source.emit("closed", { reason: "REVOKED" });
    }
    assert.equal(client.link.value.kind, "revoked", source);
    env.postResults.push(503);
    await renew(env);
    assert.equal(client.link.value.kind, "revoked", source);
  }
});

test("a client stopped while its session probe is pending does nothing more", async () => {
  for (const [how, answer] of [
    ["streamed", defaultSession],
    ["expiring", { ...session, ...EXPIRING }],
    ["offline", new Error("offline")],
    ["revoked", new HttpError(403)],
  ] as const) {
    const env = new FakeEnvironment();
    const probe = Promise.withResolvers<unknown>();
    env.responses.set(PATHS.getSession, probe.promise);
    const client = new StateClient(env);
    const starting = client.start();
    client.stop();
    settle(probe, answer);
    await starting;
    await flush();
    assert.equal(env.sources.length, 0, how);
    assert.deepEqual(pendingDelays(env), [], how);
    assert.deepEqual(client.link.value, { kind: "connecting" }, how);
    assert.equal(client.session.value, null, how);
  }
});

test("a client stopped while a failed stream is classified does nothing more", async () => {
  for (const [how, answer] of [
    ["still streamed", defaultSession],
    ["expired", { ...anonymous, login: session.login }],
    ["forbidden", { ...defaultSession, permissions: ["builds.read"] }],
    ["offline", new Error("offline")],
    ["revoked", new HttpError(403)],
  ] as const) {
    const { env, client } = await live();
    const probe = Promise.withResolvers<unknown>();
    env.responses.set(PATHS.getSession, probe.promise);
    env.source.fail();
    client.stop();
    const link = client.link.value;
    const state = client.state.value;
    settle(probe, answer);
    await flush();
    assert.equal(client.link.value, link, how);
    assert.equal(client.state.value, state, how);
    assert.equal(env.sources.length, 1, how);
    assert.deepEqual(env.posts, [], how);
    assert.deepEqual(pendingDelays(env), [], how);
  }
});

test("a client stopped while renewing does nothing more", async () => {
  for (const status of [204, 401, 503]) {
    const env = new FakeEnvironment();
    env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
    const client = new StateClient(env);
    await client.start();
    const answer = Promise.withResolvers<number>();
    env.postResults.push(answer.promise);
    await renew(env);
    assert.deepEqual(env.posts, [PATHS.renew]);
    client.stop();
    const link = client.link.value;
    answer.resolve(status);
    await flush();
    const how = String(status);
    assert.equal(probes(env), 1, how);
    assert.equal(env.sources.length, 1, how);
    assert.deepEqual(env.navigations, [], how);
    assert.deepEqual(pendingDelays(env), [], how);
    assert.equal(client.link.value, link, how);
  }
});

test("a client stopped while access and earlier events load keeps them as they were", async () => {
  for (const outcome of ["answered", "failed"] as const) {
    const { env, client } = await live();
    const base = snapshot.events[0];
    assert.ok(base);
    const principals = Promise.withResolvers<unknown>();
    const events = Promise.withResolvers<unknown>();
    env.responses.set(PRINCIPALS, principals.promise);
    env.responses.set(REVOKED, { principals: [] });
    env.responses.set(PATHS.listRoles, { roles: [] });
    env.responses.set(`${PATHS.listEvents}?pageSize=${snapshot.events.length + 200}`, events.promise);
    const loading = Promise.all([client.loadAccess(), client.loadEarlier()]);
    client.stop();
    const access = client.access.value;
    const earlier = client.earlier.value;
    if (outcome === "answered") {
      principals.resolve({ principals: [principalRow("a")] });
      events.resolve({ events: [eventAt(base, Number(idOf(base.name)) - 1, t0)] });
    } else {
      principals.reject(new Error("offline"));
      events.reject(new Error("offline"));
    }
    await loading;
    assert.equal(client.access.value, access, `access ${outcome}`);
    assert.equal(client.earlier.value, earlier, `earlier ${outcome}`);
  }
});

test("a renewal requested while one is in flight joins it, so a late failure cannot undo its success", async () => {
  for (const late of [429, 503]) {
    const env = new FakeEnvironment();
    env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
    const client = new StateClient(env);
    await client.start();
    const first = Promise.withResolvers<number>();
    const second = Promise.withResolvers<number>();
    env.postResults.push(first.promise, second.promise);
    await renew(env);
    env.source.emit("closed", { reason: "EXPIRED" });
    first.resolve(204);
    await flush();
    env.source.emit("snapshot", snapshot);
    second.resolve(late);
    await flush();
    const how = String(late);
    assert.deepEqual(env.posts, [PATHS.renew], how);
    assert.equal(client.link.value.kind, "live", how);
    assert.deepEqual(pendingDelays(env), [RENEWAL_MS], how);
  }
});

test("a renewal left in flight by a stopped client neither blocks nor ends the restarted client's renewal", async () => {
  const env = new FakeEnvironment();
  env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
  const client = new StateClient(env);
  await client.start();
  const old = Promise.withResolvers<number>();
  const fresh = Promise.withResolvers<number>();
  // A third renewal would be a duplicate: it fails, so it would also show as a retry.
  env.postResults.push(old.promise, fresh.promise, 503);
  await renew(env);
  client.stop();
  await client.start();
  await renew(env);
  assert.deepEqual(env.posts, [PATHS.renew, PATHS.renew]);
  old.resolve(204);
  await flush();
  assert.equal(probes(env), 2);
  assert.equal(env.sources.length, 2);
  env.source.emit("closed", { reason: "EXPIRED" });
  fresh.resolve(204);
  await flush();
  env.source.emit("snapshot", snapshot);
  assert.deepEqual(env.posts, [PATHS.renew, PATHS.renew]);
  assert.equal(probes(env), 3);
  assert.equal(env.sources.length, 3);
  assert.equal(client.link.value.kind, "live");
  assert.deepEqual(env.navigations, []);
  assert.deepEqual(pendingDelays(env), [RENEWAL_MS]);
});

test("an old renewal neither blocks nor ends a re-admitted client's renewal without stop", async () => {
  for (const oldStatus of [204, 401, 503]) {
    const env = new FakeEnvironment();
    env.responses.set(PATHS.getSession, { ...session, ...EXPIRING });
    const client = new StateClient(env);
    await client.start();
    const old = Promise.withResolvers<number>();
    const fresh = Promise.withResolvers<number>();
    env.postResults.push(old.promise, fresh.promise, 503);
    await renew(env);
    await client.start();
    await renew(env);
    const how = String(oldStatus);
    assert.deepEqual(env.posts, [PATHS.renew, PATHS.renew], how);
    old.resolve(oldStatus);
    await flush();
    assert.equal(probes(env), 2, how);
    assert.equal(env.sources.length, 2, how);
    assert.deepEqual(env.navigations, [], how);
    env.source.emit("closed", { reason: "EXPIRED" });
    fresh.resolve(204);
    await flush();
    env.source.emit("snapshot", snapshot);
    assert.deepEqual(env.posts, [PATHS.renew, PATHS.renew], how);
    assert.equal(probes(env), 3, how);
    assert.equal(env.sources.length, 3, how);
    assert.equal(client.link.value.kind, "live", how);
    assert.deepEqual(env.navigations, [], how);
    assert.deepEqual(pendingDelays(env), [RENEWAL_MS], how);
  }
});
