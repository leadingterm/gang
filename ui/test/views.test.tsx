import { test, type TestContext } from "node:test";
import type { ComponentChildren } from "preact";
import { render } from "preact-render-to-string";
import type { EarlierBuilds } from "../src/client.ts";
import { clock } from "../src/format.ts";
import type { Screen } from "../src/routes.ts";
import { Shell } from "../src/shell.tsx";
import { ClientContext, Tile, Tiles } from "../src/ui.tsx";
import { Actions } from "../src/views/actions.tsx";
import { Activity } from "../src/views/activity.tsx";
import { Build, BuildView } from "../src/views/build.tsx";
import { autoLoadBuilds, Builds, filterBuilds } from "../src/views/builds.tsx";
import { cellStates, fleetCounts, fleetTotals, Legend, Tape, TapeKey } from "../src/views/fleet.tsx";
import { Forbidden, SignIn, SignInError } from "../src/views/gate.tsx";
import { EventRow, Overview } from "../src/views/overview.tsx";
import { WorkerDetail, Workers } from "../src/views/workers.tsx";
import { ms } from "../src/wire-parse.ts";
import type { AwsEventKind, ScalingEvent } from "../src/wire.ts";
import { build, clientFor, session, signedOut, type Scenario } from "./scenario.ts";
import { iso, load, snapshotOf } from "./sse.ts";

Object.defineProperty(globalThis, "location", { value: new URL("https://gang.example.org/ui/") });


const html = (scenario: Scenario, children: ComponentChildren): string =>
  render(<ClientContext.Provider value={clientFor(scenario)}>{children}</ClientContext.Provider>);

const screen = (id: Screen, scenario: Scenario, view: ComponentChildren): string =>
  html(scenario, <Shell screen={id}>{view}</Shell>);

const live = snapshotOf(load("live"));
const stale = snapshotOf(load("stale"));
const lagging = snapshotOf(load("lagging"));
const empty = snapshotOf(load("empty"));
const cold = snapshotOf(load("cold"));
const { actions: _actions, ...withoutActions } = live;

const SCREENS: [string, () => string][] = [
  ["overview live", () => screen("overview", {}, <Overview />)],
  ["overview stale", () => screen("overview", { state: stale }, <Overview />)],
  ["overview empty", () => screen("overview", { state: empty }, <Overview />)],
  ["overview cold", () => screen("overview", { state: cold }, <Overview />)],
  [
    "overview disconnected",
    () => screen("overview", { link: { kind: "lost", frozenAt: ms(live.updateTime), retryAt: ms(live.updateTime) + 4000 } }, <Overview />),
  ],
  ["overview revoked", () => screen("overview", { link: { kind: "revoked" } }, <Overview />)],
  ["actions live", () => screen("actions", {}, <Actions />)],
  ["actions lagging", () => screen("actions", { state: lagging }, <Actions />)],
  ["actions without a sample", () => screen("actions", { state: withoutActions }, <Actions />)],
  ["actions empty", () => screen("actions", { state: empty }, <Actions />)],
  ["workers live", () => screen("workers", {}, <Workers />)],
  ["workers cold", () => screen("workers", { state: cold }, <Workers />)],
  ["activity live", () => screen("activity", {}, <Activity />)],
  [
    "activity without events.read",
    () => screen("activity", { session: { ...session, permissions: ["state.read"] } }, <Activity />),
  ],
  [
    "forbidden",
    () =>
      screen(
        "overview",
        { state: null, session: { ...session, permissions: [] }, link: { kind: "forbidden" } },
        <Forbidden />,
      ),
  ],
  ["sign in", () => html({ state: null, session: { ...signedOut, permissions: [] } }, <SignIn />)],
  [
    "sign in unavailable",
    () => html({ state: null, session: { name: signedOut.name, permissions: [] } }, <SignIn />),
  ],
  ["sign-in error denied", () => html({ state: null }, <SignInError reason="denied" />)],
];

const visibleText = (html: string): string =>
  html
    .replace(/<(script|style|svg)\b[^>]*>[\s\S]*?<\/\1>/g, "")
    .replace(/<\/(p|div|section|li|tr|h[1-6]|header|footer|main|aside|table|button|a)>/g, "$&\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .split("\n")
    .map((line) => line.trim().replace(/ +/g, " "))
    .filter((line) => line !== "")
    .join("\n");

/** The landmark a user relies on in each scenario: a heading, or the banner's live region. */
const LANDMARKS: Partial<Record<string, RegExp>> = {
  forbidden: /<h1[\s>]/,
  "sign in": /<h1[\s>]/,
  "overview stale": /role="alert"/,
  "overview disconnected": /role="status"/,
  "overview revoked": /role="alert"/,
};

for (const [name, rendered] of SCREENS) {
  const landmark = LANDMARKS[name];
  if (!landmark) continue;
  test(name, (t) => {
    t.assert.match(rendered(), landmark, `${name}: landmark missing`);
  });
}

test("public connection saturation and recovered refusals are visible", (t) => {
  const saturated = { ...live, connections: { active: 10, limit: 10, refused: 3 } };
  const full = screen("overview", { state: saturated }, <Overview />);
  t.assert.match(full, /Public connection limit reached/);
  t.assert.match(visibleText(full), /10 \/ 10 open · 3 refused since server start/);
  const recovered = { ...saturated, connections: { active: 2, limit: 10, refused: 3 } };
  const history = screen("overview", { state: recovered }, <Overview />);
  t.assert.match(visibleText(history), /3 connections refused since server start/);
  t.assert.match(visibleText(history), /2 \/ 10 open/);
  t.assert.doesNotMatch(history, /Public connection limit reached/);
});

test("zero and missing public connection metrics do not warn", (t) => {
  const { connections: _connections, ...oldState } = live;
  for (const state of [oldState, { ...live, connections: { active: 0, limit: 0, refused: 0 } }]) {
    const markup = screen("overview", { state: state as typeof live }, <Overview />);
    t.assert.doesNotMatch(markup, /Public connection limit reached|connections refused since server start/);
  }
});

test("stream loss, revocation and skipped passes precede public saturation", (t) => {
  const state = { ...live, connections: { active: 10, limit: 10, refused: 3 } };
  const lost = screen("overview", { state, link: { kind: "lost", frozenAt: null, retryAt: ms(live.updateTime) + 4000 } }, <Overview />);
  const revoked = screen("overview", { state, link: { kind: "revoked" } }, <Overview />);
  const skipped = screen("overview", { state: { ...state, pass: { ...state.pass, state: "SKIPPED" } } }, <Overview />);
  for (const [markup, expected] of [[lost, /Stream lost/], [revoked, /Access revoked/], [skipped, /Pass skipped/]] as const) {
    t.assert.match(markup, expected);
    t.assert.doesNotMatch(markup, /Public connection limit reached/);
  }
});

test("build list shows the live build, hit rate and link", (t) => {
  const markup = html({}, <Builds />);
  t.assert.match(markup, /ops@example.org/);
  t.assert.match(markup, /80%/);
  t.assert.match(markup, /href=\"\/ui\/builds\/0123456789abcdef\"/);
});

test("build filters use principal, invocation and mine and sort newest first", (t) => {
  const rows = live.builds ?? [];
  t.assert.equal(rows.length, 4);
  const all = filterBuilds(rows.toReversed(), false, session.principal, "", "");
  t.assert.deepEqual(all.map((b) => b.name), ["builds/0123456789abcdef", "builds/1111111111111111", "builds/2222222222222222", "builds/3333333333333333"]);
  t.assert.equal(filterBuilds(rows, true, session.principal, "", "").length, 2);
  t.assert.equal(filterBuilds(rows, false, session.principal, "GITHUB-ACTIONS", "").length, 2);
  t.assert.equal(filterBuilds(rows, false, session.principal, "", "COLD").length, 1);
  t.assert.equal(filterBuilds(rows, true, session.principal, "github-actions", "").length, 0);
  const first = rows[0];
  const second = rows[1];
  if (!first || !second) throw new Error("fixture has two builds");
  const tied = filterBuilds([{ ...second, lastActiveTime: first.lastActiveTime }, first], false, session.principal, "", "");
  t.assert.deepEqual(tied.map((b) => b.name), [first.name, second.name]);
});

test("only build-only forbidden sessions auto-load held builds", (t) => {
  t.assert.equal(autoLoadBuilds(true, { kind: "forbidden" }, false), true);
  t.assert.equal(autoLoadBuilds(true, { kind: "connecting" }, false), false);
  t.assert.equal(autoLoadBuilds(true, { kind: "live", beats: 0 }, false), false);
  t.assert.equal(autoLoadBuilds(false, { kind: "forbidden" }, false), false);
  t.assert.equal(autoLoadBuilds(true, { kind: "forbidden" }, true), false);
});

test("a one-bucket timeline spans the chart", (t) => {
  const bucket = build.timeline?.buckets?.[0];
  if (!bucket || !build.timeline) throw new Error("fixture has a timeline");
  const markup = html({}, <BuildView build={{ ...build, timeline: { ...build.timeline, buckets: [bucket] } }} />);
  t.assert.match(markup, /class="build-chart__executing" points="0,[^ ]+ 300,[^ ]+ 600,/);
});

test("build-only sessions see held builds without state.read", async (t) => {
  const { App, route } = await import("../src/app.tsx");
  const previous = route.value;
  const client = clientFor({ state: null, session: { ...session, permissions: ["builds.read"] }, link: { kind: "forbidden" } });
  client.earlierBuilds.value = { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: ms(live.updateTime), failing: false };
  route.value = { kind: "screen", screen: "builds" };
  try {
    const markup = render(<App client={client} />);
    t.assert.match(markup, /github-actions:repository_owner_id:000000000:repository_id:0000000000:ref:refs\/heads\/build\/remote-build-aarch64/);
    t.assert.doesNotMatch(markup, /No access/);
  } finally {
    route.value = previous;
  }
});

test("a build-only session never claims no builds before its first page", (t) => {
  const client = clientFor({ state: null, session: { ...session, permissions: ["builds.read"] }, link: { kind: "forbidden" } });
  const text = (earlier: EarlierBuilds): string => {
    client.earlierBuilds.value = earlier;
    return visibleText(render(<ClientContext.Provider value={client}><Builds /></ClientContext.Provider>));
  };
  const loading = text({ kind: "unloaded", loading: true });
  t.assert.match(loading, /Loading builds/);
  t.assert.doesNotMatch(loading, /No builds/);
  const error = text({ kind: "error", loading: false });
  t.assert.match(error, /Could not load builds/);
  t.assert.doesNotMatch(error, /No builds/);
  const loaded = text({ kind: "loaded", builds: [], next: undefined, loading: false, asOf: ms(live.updateTime), failing: false });
  t.assert.match(loaded, /No builds/);
  t.assert.doesNotMatch(loaded, /Loading builds/);
});

test("each build-only page says when its own builds are as of while its refresh fails", (t) => {
  const client = clientFor({ state: null, session: { ...session, permissions: ["builds.read"] }, link: { kind: "forbidden" } });
  const id = "0123456789abcdef";
  const detailAsOf = ms(live.updateTime);
  const listAsOf = detailAsOf + 60_000;
  client.build.value = { kind: "loaded", id, build, asOf: detailAsOf, failing: true };
  client.earlierBuilds.value = { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: listAsOf, failing: true };
  const page = (view: ComponentChildren): string => render(<ClientContext.Provider value={client}>{view}</ClientContext.Provider>);
  const detail = page(<Build id={id} />);
  const list = page(<Builds />);
  t.assert.match(detail, /<p class="caution" role="status">Could not refresh this build; showing it as of /);
  t.assert.match(list, /<p class="caution" role="status">Could not refresh builds; showing them as of /);
  t.assert.equal(visibleText(detail).includes(`showing it as of ${clock(detailAsOf)}`), true);
  t.assert.equal(visibleText(list).includes(`showing them as of ${clock(listAsOf)}`), true);
  t.assert.equal(visibleText(list).includes(`showing them as of ${clock(detailAsOf)}`), false);
  client.build.value = { kind: "loaded", id, build, asOf: detailAsOf, failing: false };
  client.earlierBuilds.value = { kind: "loaded", builds: [build], next: undefined, loading: false, asOf: listAsOf, failing: false };
  t.assert.doesNotMatch(page(<Build id={id} />), /Could not refresh/);
  t.assert.doesNotMatch(page(<Builds />), /Could not refresh/);
});

test("builds require builds.read", (t) => {
  const markup = html({ session: { ...session, permissions: ["state.read"] } }, <Builds />);
  t.assert.match(markup, /builds.read/);
  t.assert.doesNotMatch(markup, /3f2b8c1e-7a4d-4e9b-9c2f-5d6e7f8a9b0c/);
});

test("actions link to their build and offer it as a filter", (t) => {
  const markup = html({}, <Actions />);
  t.assert.match(markup, /href="\/ui\/builds\/0123456789abcdef"/);
  t.assert.match(markup, /<option value="builds\/0123456789abcdef">/);
});

test("empty build details explain the missing observations", (t) => {
  const { mnemonics: _mnemonics, otherMnemonics: _other, missedTargets: _targets, slowest: _slowest, ...emptyBuild } = build;
  const markup = html({}, <BuildView build={emptyBuild} />);
  const text = visibleText(markup);
  t.assert.match(text, /No mnemonic misses recorded/);
  t.assert.match(text, /No missed targets recorded/);
  t.assert.match(text, /No completed actions recorded/);
  t.assert.doesNotMatch(markup, /<table\b/);
});

test("mnemonic rows with no misses do not appear", (t) => {
  const usage = { ...build.usage, cache: { ...build.usage.cache, misses: 0 } };
  const markup = html({}, <BuildView build={{ ...build, mnemonics: [{ mnemonic: "NoMisses", usage }], otherMnemonics: usage }} />);
  t.assert.doesNotMatch(markup, /NoMisses/);
  t.assert.match(markup, /No mnemonic misses recorded/);
});

test("build detail shows its observed work", (t) => {
  const markup = html({}, <BuildView build={build} />);
  for (const expected of [/3f2b8c1e-7a4d-4e9b-9c2f-5d6e7f8a9b0c/, /bazel 8.4.2/, /Remote work Gang saw; local actions, disk-cache hits and analysis are not included\./, /Remote cache/, /80%/, /Slowest actions/, /parser/, /Joined\s+3/]) t.assert.match(visibleText(markup), expected);
});

test("idle build and missing observations retain their separate warnings", (t) => {
  const idle = visibleText(html({}, <BuildView build={{ ...build, state: "IDLE", unrecorded: 3 }} />));
  t.assert.match(idle, /No observation for a while; Gang cannot know if it finished\./);
  t.assert.match(idle, /3 observations not recorded/);
  const active = visibleText(html({}, <BuildView build={{ ...build, state: "ACTIVE", unrecorded: 0 }} />));
  t.assert.doesNotMatch(active, /No observation for a while|observations not recorded/);
});

test("a missing build says it is not held", (t) => {
  const client = clientFor({});
  client.build.value = { kind: "missing", id: "missing" };
  const markup = render(<ClientContext.Provider value={client}><Build id="missing" /></ClientContext.Provider>);
  t.assert.match(markup, /build not held: only the latest builds are kept in memory/);
});

test("overview includes action cache hit rate", (t) => {
  const markup = html({}, <Overview />);
  t.assert.match(markup, /Action cache/);
  t.assert.match(markup, /80%/);
});

test("the app renders a loaded build at its direct route", async (t) => {
  // The app reads location during module initialization; install the test location first.
  const { App, route } = await import("../src/app.tsx");
  const previous = route.value;
  const client = clientFor({});
  client.build.value = { kind: "loaded", id: "0123456789abcdef", build, asOf: ms(live.updateTime), failing: false };
  route.value = { kind: "build", id: "0123456789abcdef" };
  try {
    const markup = render(<App client={client} />);
    t.assert.match(visibleText(markup), /3f2b8c1e-7a4d-4e9b-9c2f-5d6e7f8a9b0c/);
  } finally {
    route.value = previous;
  }
});

const WORKER = "i-04828e21626b14dd2-0ee112f6-7a36-7faf-be44-719a1246ef6b";
const common = { name: "events/1022", createTime: live.updateTime, fleet: "fleets/amd64-on-demand" };

test("worker events name the worker and its verb", (t) => {
  const row = (e: ScalingEvent) => render(<EventRow withCause e={e} />);
  t.assert.match(row({ ...common, workerRegistered: { worker: `workers/${WORKER}` } }), /<span class="line mono"><span class="ident" title="i-04828e21626b14dd2">i-04828e21626b14dd2<\/span> <span>joined<\/span><\/span>/);
  t.assert.match(row({ ...common, workerDeregistered: { worker: `workers/${WORKER}` } }), /<span class="line mono"><span class="ident" title="i-04828e21626b14dd2">i-04828e21626b14dd2<\/span> <span>left<\/span><\/span>/);
});

test("a minimum cause is shown without caution styling", (t) => {
  const row = render(<EventRow withCause e={{ ...common, cause: "MINIMUM", scaledOut: { from: 0, to: 1 } }} />);
  t.assert.match(row, /<span class="event__cause">minimum<\/span>/);
  t.assert.doesNotMatch(row, /caution/);
});

test("a utilization cordon explains the dwell without caution styling", (t) => {
  const row = render(<EventRow withCause e={{ ...common, cause: "UTILIZATION", drainStarted: { worker: `workers/${WORKER}`, detail: "load 0.0 worker-equivalents" } }} />);
  t.assert.match(row, /<span class="event__cause">measured fleet load stayed below the waterline for the dwell<\/span>/);
  t.assert.doesNotMatch(row, /caution/);
});

test("an early exit names the instance and shows its failure", (t) => {
  const failure = "fail  gang worker: the scratch volume is too small";
  const row = render(<EventRow withCause e={{ ...common, exitedEarly: { instance: "instances/local-1", failure } }} />);
  t.assert.match(row, /Early exit/);
  t.assert.match(row, /local-1/);
  t.assert.match(row, /fail  gang worker: the scratch volume is too small/);
});

test("an inherited property name is not a sign-in error reason", (t) => {
  const fallback = html({ state: null }, <SignInError reason="failed" />);
  for (const name of ["toString", "constructor", "__proto__"]) {
    t.assert.equal(html({ state: null }, <SignInError reason={name} />), fallback, name);
  }
});

/** The inner markup of an event row's glyph element, or undefined when the row renders none. */
const glyphOf = (rendered: string): string | undefined => /<svg class="event__glyph"[^>]*>([\s\S]*?)<\/svg>/.exec(rendered)?.[1];

test("an event of an unknown kind renders without a glyph or a detail", (t) => {
  const unknown = { ...common, hibernated: { instance: "instances/i-0abc" } } as unknown as ScalingEvent;
  const rendered = render(<EventRow withCause e={unknown} />);
  t.assert.match(rendered, /<span class="visually-hidden">Event<\/span>/);
  t.assert.match(rendered, /amd64-on-demand/);
  t.assert.doesNotMatch(rendered, /hibernated|i-0abc/);
  t.assert.equal(glyphOf(rendered), "", "an unknown kind draws no glyph");

  const known = render(<EventRow withCause e={{ ...common, terminated: { instance: "instances/i-0abc" } }} />);
  t.assert.match(glyphOf(known) ?? "", /<path class="g-term"/, "a known kind draws its glyph, so the check above can fail");
});

test("a fleet's counts are totalled across fleets and name every state with the legend's words", (t) => {
  const [spot, onDemand] = live.fleets;
  const worker = live.workers[0];
  const instance = live.instances[0];
  if (!spot || !onDemand || !worker || !instance) throw new Error("fixture has two fleets, a worker and an instance");
  const rows = (fleet: string, phase: "BUSY" | "IDLE" | "DRAINING", n: number) =>
    Array.from({ length: n }, (_, i) => ({ ...worker, fleet, phase, running: phase === "BUSY" ? 1 : 0, name: `workers/${fleet}-${phase}-${i}`, instance: `instances/${fleet}-${phase}-${i}` }));
  const census = {
    workers: [...rows(spot.name, "BUSY", 9), ...rows(spot.name, "IDLE", 1), ...rows(spot.name, "DRAINING", 1), ...rows(onDemand.name, "BUSY", 2), ...rows(onDemand.name, "IDLE", 1)],
    instances: (["IN_SERVICE", "PENDING", "PENDING", "TERMINATING"] as const).map((lifecycle, i) => ({ ...instance, name: `instances/test-${i}`, fleet: spot.name, lifecycle })),
  };
  t.assert.deepEqual(fleetCounts(spot, census), [
    ["busy", 9],
    ["idle", 1],
    ["draining", 1],
    ["unconnected", 1],
    ["pending", 2],
    ["terminating", 1],
    ["replacing", 0],
    ["replacement", 0],
    ["unknown", 0],
    ["warm", 4],
  ]);
  t.assert.deepEqual(fleetTotals([spot, onDemand], census).slice(0, 3), [
    ["busy", 11],
    ["idle", 2],
    ["draining", 1],
  ]);
  t.assert.match(html({}, <Tape f={spot} census={census} />), /9 busy, 1 idle, 1 draining, 1 no worker, 2 pending, 1 terminating, 0 replacing, 0 replacement, 0 unknown, of 24 maximum/);
});

test("fleet details show measured readiness and expected wait independently, including zero", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const { running, queued, candidates, needed, vcpus, memory } = fleet.demand;
  const details = (readiness?: string, expectedWait?: string) =>
    render(<TapeKey census={live} f={{ ...fleet, demand: {
      running, queued, candidates, needed, vcpus, memory,
      ...(readiness !== undefined ? { readiness } : {}),
      ...(expectedWait !== undefined ? { expectedWait } : {}),
    } }} />);
  const readiness = /<dt>measured readiness<\/dt><dd><time>12s<\/time><\/dd>/;
  const wait = /<dt>expected wait<\/dt><dd><time>34s<\/time><\/dd>/;

  const both = details("12.5s", "34.5s");
  t.assert.match(both, readiness);
  t.assert.match(both, wait);
  const onlyReadiness = details("12.5s");
  t.assert.match(onlyReadiness, readiness);
  t.assert.doesNotMatch(onlyReadiness, /<dt>expected wait<\/dt>/);
  const onlyWait = details(undefined, "34.5s");
  t.assert.doesNotMatch(onlyWait, /<dt>measured readiness<\/dt>/);
  t.assert.match(onlyWait, wait);
  const neither = details();
  t.assert.doesNotMatch(neither, /<dt>measured readiness<\/dt>|<dt>expected wait<\/dt>/);
  const zero = details("0s", "0s");
  t.assert.match(zero, /<dt>measured readiness<\/dt><dd><time>0s<\/time><\/dd>/);
  t.assert.match(zero, /<dt>expected wait<\/dt><dd><time>0s<\/time><\/dd>/);
});

test("fleet details show rate diagnostics independently, preserving zero and units", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const { running, queued, candidates, needed, vcpus, memory } = fleet.demand;
  const details = (load?: number, backlog?: string, utilization?: number) =>
    render(<TapeKey census={live} f={{ ...fleet, demand: {
      running, queued, candidates, needed, vcpus, memory,
      ...(load !== undefined ? { load } : {}),
      ...(backlog !== undefined ? { backlog } : {}),
      ...(utilization !== undefined ? { utilization } : {}),
    } }} />);
  const all = details(125, "42s", 735);
  t.assert.match(all, /<dt>load<\/dt><dd>12\.5 worker-equivalents<\/dd>/);
  t.assert.match(all, /<dt>backlog<\/dt><dd>42 worker-equivalent-seconds<\/dd>/);
  t.assert.match(all, /<dt>utilization<\/dt><dd>73\.5%<\/dd>/);
  const zero = details(0, "0s", 0);
  t.assert.match(zero, /<dt>load<\/dt><dd>0\.0 worker-equivalents<\/dd>/);
  t.assert.match(zero, /<dt>backlog<\/dt><dd>0 worker-equivalent-seconds<\/dd>/);
  t.assert.match(zero, /<dt>utilization<\/dt><dd>0%<\/dd>/);
  t.assert.doesNotMatch(details(), /<dt>load<\/dt>|<dt>backlog<\/dt>|<dt>utilization<\/dt>/);
  t.assert.doesNotMatch(details(1), /<dt>backlog<\/dt>|<dt>utilization<\/dt>/);
  t.assert.doesNotMatch(details(undefined, "1s"), /<dt>load<\/dt>|<dt>utilization<\/dt>/);
  t.assert.doesNotMatch(details(undefined, undefined, 1), /<dt>load<\/dt>|<dt>backlog<\/dt>/);
});

const executingReadout = (state: typeof live): string =>
  /<h2 class="label">Executing<\/h2>[\s\S]*?(?=<div class="readout">)/.exec(html({ state }, <Overview />))?.[0] ?? "";

for (const [name, caps] of [
  ["unlimited workers", [undefined]],
  ["mixed bounded and unlimited workers", [1, undefined]],
  ["no workers", []],
] as const) {
  test(`overview shows only the executing count with ${name}`, (t) => {
    const fleet = live.fleets[0];
    const worker = live.workers[0];
    if (!fleet || !worker) throw new Error("the live recording has a fleet and worker");
    const { slots: _slots, ...unlimited } = worker;
    const state = {
      ...live,
      fleets: [{ ...fleet, demand: { ...fleet.demand, running: 24 } }],
      workers: caps.map((slots, i) => ({ ...unlimited, name: `workers/executing-${i}`, ...(slots !== undefined ? { slots } : {}) })),
    };
    const readout = executingReadout(state);
    t.assert.match(readout, /<div class="readout__big"><span[^>]*>24<\/span><\/div>/);
    t.assert.doesNotMatch(readout, /readout__of|class="meter"/);
    t.assert.match(readout, /aria-label="Executing"/);
  });
}

test("overview executing count ignores worker slots even at scale", (t) => {
  const fleet = live.fleets[0];
  const worker = live.workers[0];
  if (!fleet || !worker) throw new Error("the live recording has a fleet and worker");
  const { slots: _slots, ...uncapped } = worker;
  for (const slots of [undefined, 0, 1, 16]) {
    const state = {
      ...live,
      fleets: [{ ...fleet, demand: { ...fleet.demand, running: 120 } }],
      workers: Array.from({ length: 120 }, (_, i) => ({
        ...uncapped, name: `workers/executing-${i}`, running: 1, ...(slots !== undefined ? { slots } : {}),
      })),
    };
    const readout = executingReadout(state);
    t.assert.match(readout, /<div class="readout__big"><span[^>]*>120<\/span><\/div>/);
    t.assert.doesNotMatch(readout, /readout__of|class="meter"|\/0|slots/i);
  }
});

test("worker rows and details show only running counts regardless of slots", (t) => {
  const worker = live.workers[0];
  if (!worker) throw new Error("the live recording has a worker");
  const { slots: _slots, ...uncapped } = worker;
  for (const slots of [undefined, 0, 1, 16]) {
    const w = { ...uncapped, running: 3, ...(slots !== undefined ? { slots } : {}) };
    const state = { ...live, workers: [w] };
    const row = html({ state }, <Workers />);
    const detail = html({ state }, <WorkerDetail w={w} state={state} />);
    t.assert.match(row, /<span>3 running<\/span>/);
    t.assert.match(detail, /<dt>running<\/dt><dd class="mono">3<\/dd>/);
    t.assert.doesNotMatch(row + detail, /slots|\/0|<i(?: class="on")?><\/i>/i);
  }
});

test("worker ledger totals show admission capacity independently of slots and sampled actions", (t) => {
  const worker = live.workers[0];
  if (!worker) throw new Error("the live recording has a worker");
  const reserved = { cpuMillicores: 6000, memory: 2 * 1024 ** 3 };
  const capacity = { cpuMillicores: 8000, memory: 4 * 1024 ** 3 };
  const measured = { ...worker, reserved, capacity };
  const state = { ...live, workers: [measured] };
  const expected = /CPU admission 6000 millicores \/ 8000 millicores · memory 2 GiB \/ 4 GiB reserved \/ capacity/;
  const row = html({ state }, <Workers />);
  t.assert.match(visibleText(row), /CPU 6000 \/ 8000 mCPU/);
  t.assert.match(visibleText(row), /Memory 2 GiB \/ 4 GiB/);
  t.assert.match(row, /title="Reserved \/ admission capacity"/);
  t.assert.match(row, /aria-label="CPU admission 6000 millicores \/ 8000 millicores/);
  t.assert.match(visibleText(html({ state }, <WorkerDetail w={measured} state={state} />)), expected);

  const { reserved: _reserved, capacity: _capacity, ...unknown } = measured;
  const missing = { ...live, workers: [unknown] };
  const unknownTotals = /CPU admission — \/ — · memory — \/ — reserved \/ capacity/;
  const unknownRow = visibleText(html({ state: missing }, <Workers />));
  t.assert.match(unknownRow, /CPU — \/ — mCPU/);
  t.assert.match(unknownRow, /Memory — \/ —/);
  t.assert.match(visibleText(html({ state: missing }, <WorkerDetail w={unknown} state={missing} />)), unknownTotals);

  const zero = { ...unknown, reserved: { cpuMillicores: 0, memory: 0 } };
  const partial = { ...live, workers: [zero] };
  const partialRow = visibleText(html({ state: partial }, <Workers />));
  t.assert.match(partialRow, /CPU 0 \/ — mCPU/);
  t.assert.match(partialRow, /Memory 0 B \/ —/);
  t.assert.match(visibleText(html({ state: partial }, <WorkerDetail w={zero} state={partial} />)), /CPU admission 0 millicores \/ — · memory 0 B \/ —/);

  const capacityOnly = { ...unknown, capacity: { cpuMillicores: 0, memory: 0 } };
  const capacityState = { ...live, workers: [capacityOnly] };
  const capacityRow = visibleText(html({ state: capacityState }, <Workers />));
  t.assert.match(capacityRow, /CPU — \/ 0 mCPU/);
  t.assert.match(capacityRow, /Memory — \/ 0 B/);
  t.assert.match(visibleText(html({ state: capacityState }, <WorkerDetail w={capacityOnly} state={capacityState} />)), /CPU admission — \/ 0 millicores · memory — \/ 0 B/);

  const outage = { ...state, schedulerKnown: false };
  const outageRow = html({ state: outage }, <Workers />);
  t.assert.match(visibleText(outageRow), /Reservation unknown/);
  t.assert.doesNotMatch(outageRow, /6000|8000|Reserved \/ admission capacity/);
  t.assert.match(visibleText(html({ state: outage }, <WorkerDetail w={measured} state={outage} />)), /reservation\s*unknown/);
});

test("worker cache distinguishes measured, zero, missing and stale reports", (t) => {
  const worker = live.workers[0];
  if (!worker) throw new Error("the live recording has a worker");
  const cache = {
    local: { requests: 12, bytes: 2048 },
    remote: { requests: 3, bytes: 4096 },
    evictions: 2, evicted: 1024, used: 2048, budget: 8192,
  };
  const measured = { ...worker, cache };
  const state = { ...live, schedulerKnown: true, workers: [measured] };
  const detail = html({ state }, <WorkerDetail w={measured} state={state} />);
  t.assert.match(detail, /<dt>local reads<\/dt><dd class="mono">12 requests · 2 KiB<\/dd>/);
  t.assert.match(detail, /<dt>slow-store fetches<\/dt><dd class="mono">3 requests · 4 KiB<\/dd>/);
  t.assert.match(detail, /<dt>evictions<\/dt><dd class="mono">2 · 1 KiB<\/dd>/);
  t.assert.match(detail, /<dt>used \/ budget<\/dt><dd class="mono">2 KiB \/ 8 KiB<\/dd>/);

  const zero = { ...measured, cache: { local: { requests: 0, bytes: 0 }, remote: { requests: 0, bytes: 0 }, evictions: 0, evicted: 0, used: 0, budget: 0 } };
  const zeroDetail = html({ state }, <WorkerDetail w={zero} state={state} />);
  t.assert.match(zeroDetail, /<dt>local reads<\/dt><dd class="mono">0 requests · 0 B<\/dd>/);
  t.assert.match(zeroDetail, /<dt>used \/ budget<\/dt><dd class="mono">0 B \/ 0 B<\/dd>/);
  const { cache: _cache, ...missing } = measured;
  const unknown = html({ state }, <WorkerDetail w={missing} state={state} />);
  const staleState = { ...state, schedulerKnown: false };
  const staleDetail = html({ state: staleState }, <WorkerDetail w={measured} state={staleState} />);
  for (const markup of [unknown, staleDetail]) {
    for (const label of ["local reads", "slow-store fetches", "evictions", "used / budget"]) {
      t.assert.equal(markup.includes(`<dt>${label}</dt><dd class="mono">unknown</dd>`), true, label);
    }
  }
});

test("worker fleet cache totals show coverage, measured zeros and stale or absent reports", (t) => {
  const worker = live.workers[0];
  const fleet = live.fleets.find((f) => f.name === worker?.fleet);
  if (!worker || !fleet) throw new Error("the live recording has a worker and its fleet");
  const totals = { local: { requests: 7, bytes: 1024 }, remote: { requests: 2, bytes: 2048 }, evictions: 1, evicted: 4096, used: 8192, budget: 16384 };
  const measuredFleet = { ...fleet, workerCache: { reporting: 1, workers: 2, totals } };
  const state = { ...live, schedulerKnown: true, fleets: [measuredFleet], workers: [worker] };
  const markup = html({ state }, <Workers />);
  t.assert.match(markup, /1 \/ 2 workers reporting/);
  t.assert.match(markup, /<dt>local reads<\/dt><dd class="mono">7 requests · 1 KiB<\/dd>/);
  t.assert.match(markup, /<dt>slow-store fetches<\/dt><dd class="mono">2 requests · 2 KiB<\/dd>/);
  t.assert.match(markup, /<dt>evictions<\/dt><dd class="mono">1 · 4 KiB<\/dd>/);
  t.assert.match(markup, /<dt>used \/ budget<\/dt><dd class="mono">8 KiB \/ 16 KiB<\/dd>/);

  const zeroTotals = { local: { requests: 0, bytes: 0 }, remote: { requests: 0, bytes: 0 }, evictions: 0, evicted: 0, used: 0, budget: 0 };
  const zeroState = { ...state, fleets: [{ ...measuredFleet, workerCache: { reporting: 1, workers: 1, totals: zeroTotals } }] };
  t.assert.match(html({ state: zeroState }, <Workers />), /<dt>local reads<\/dt><dd class="mono">0 requests · 0 B<\/dd>/);
  const noneState = { ...state, fleets: [{ ...measuredFleet, workerCache: { reporting: 0, workers: 2, totals: zeroTotals } }] };
  const none = html({ state: noneState }, <Workers />);
  t.assert.match(none, /0 \/ 2 workers reporting/);
  t.assert.match(none, /<dt>local reads<\/dt><dd class="mono">unknown<\/dd>/);
  const { workerCache: _workerCache, ...legacyFleet } = measuredFleet;
  const legacyState = { ...state, fleets: [legacyFleet as typeof measuredFleet] };
  const legacyMarkup = html({ state: legacyState }, <Workers />);
  t.assert.match(legacyMarkup, /Reporting coverage unknown/);
  t.assert.match(legacyMarkup, /<dt>local reads<\/dt><dd class="mono">unknown<\/dd>/);
  const staleState = { ...state, schedulerKnown: false };
  const staleMarkup = html({ state: staleState }, <Workers />);
  t.assert.match(staleMarkup, /Reporting coverage unknown/);
  t.assert.match(staleMarkup, /<dt>local reads<\/dt><dd class="mono">unknown<\/dd>/);
  t.assert.doesNotMatch(staleMarkup, /1 \/ 2 workers reporting/);
});

test("action lists and worker detail show admitted and learned reservations", (t) => {
  const worker = live.workers[0];
  const running = live.actions?.running[0];
  const queued = cold.actions?.queued[0];
  if (!worker || !running || !queued || !live.actions) throw new Error("the recordings have a worker and actions");
  const { reservation: _reservation, learnedReservation: _learnedReservation, ...unknownAction } = running;
  const state = {
    ...live,
    actions: {
      ...live.actions,
      running: [
        { ...running, worker: worker.name, reservation: { cpuMillicores: 2000, memory: 1024 ** 3 }, learnedReservation: { cpuMillicores: 4000, memory: 2 * 1024 ** 3 } },
        { ...unknownAction, name: "actions/unknown", worker: worker.name },
      ],
      queued: [{ ...queued, reservation: { cpuMillicores: 0, memory: 0 }, learnedReservation: { cpuMillicores: 3000, memory: 1024 ** 3 } }],
    },
  };
  const list = visibleText(html({ state }, <Actions />));
  const detail = visibleText(html({ state }, <WorkerDetail w={worker} state={state} />));
  const fitted = /Reservation: 2000 millicores CPU · 1 GiB memory · Learned: 4000 millicores CPU · 2 GiB memory/;
  t.assert.match(list, fitted);
  t.assert.match(detail, fitted);
  t.assert.match(list, /Reservation: 0 millicores CPU · 0 B memory · Learned: 3000 millicores CPU · 1 GiB memory/);
  t.assert.match(list, /Reservation: —/);
  t.assert.match(detail, /Reservation: —/);
});

test("open worker detail names running actions and falls back to the digest", (t) => {
  const worker = live.workers[0];
  const action = live.actions?.running[0];
  if (!worker || !action || !live.actions) throw new Error("the live recording has a worker and a running action");
  const detailed = { ...worker, phases: [{ phase: worker.phase, start: worker.phaseTime }] };
  const state = {
    ...live,
    workers: [detailed, ...live.workers.slice(1)],
    actions: { ...live.actions, running: [
      { ...action, worker: worker.name, target: "//pkg:target", mnemonic: "Compile" },
      { ...action, name: "actions/fallback", worker: worker.name, target: "", mnemonic: "", digest: "abcdef1234567890/123" },
    ] },
  };
  const markup = html({ state }, <WorkerDetail w={detailed} state={state} />);
  t.assert.match(markup, /<thead role="rowgroup">[\s\S]*Target[\s\S]*Mnemonic[\s\S]*Running[\s\S]*<\/thead>/);
  t.assert.match(visibleText(markup), /\/\/pkg:target/);
  t.assert.match(markup, /class="[^"]*(?:quiet|dim)[^"]*"[^>]*>abcdef123456</);
  t.assert.match(visibleText(markup), /—/);
});

test("fleet launch failure shows the unmet request and provider reason", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const state = { ...live, fleets: [{ ...fleet, desired: 4, instances: { ...fleet.instances, inService: 2 }, launchFailure: { reason: "spot quota exceeded" } }] };
  const text = visibleText(html({ state }, <Overview />));
  t.assert.match(text, /can't launch/);
  t.assert.match(text, /2 of 4 · spot quota exceeded/);
});

const recentState = (count: number) => {
  const last = Math.floor(ms(live.updateTime) / 10000) * 10000;
  return {
    ...live,
    recent: {
      window: "900s",
      step: "10s",
      samples: Array.from({ length: count }, (_, i) => ({
        start: iso(last - (count - 1 - i) * 10000),
        completed: 10, running: 100, queued: 5, passes: 5, skipped: 0, lookups: 0, hits: 0, uploadedBytes: 0, downloadedBytes: 0, evictedBytes: 0,
      })),
    },
  };
};

const chart = (markup: string, label: string): string =>
  new RegExp(`<svg\\b[^>]*aria-label="${label}"[^>]*>[\\s\\S]*?<\\/svg>`).exec(markup)?.[0] ?? "";

const passStrip = (markup: string): string =>
  /<div class="passbar"[^>]*>[\s\S]*?<\/div>/.exec(markup)?.[0] ?? "";

test("fleet instance histories retain independent observed series through missing samples", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const base = recentState(5);
  const first = { ...fleet, name: "fleets/history-a" };
  const second = { ...fleet, name: "fleets/history-b" };
  const observation = (name: string, count: number, max: number) => ({
    name, instances: { pending: count, inService: count, terminating: count, warm: count }, desired: count, max,
  });
  const samples = base.recent.samples.map((sample, i) => ({
    ...sample,
    schedulerKnown: false,
    ...(i === 1 ? {} : { fleets: i === 3
      ? [observation(second.name, 1, 4)]
      : [observation(first.name, i === 2 ? 0 : 1, 2), observation(second.name, 1, 4)] }),
  }));
  const state = { ...base, schedulerKnown: false, fleets: [first, second], recent: { ...base.recent, window: "50s", samples } };
  const markup = html({ state }, <Overview />);
  const a = chart(markup, `Instance history for ${first.name}; latest sample: Pending 1, In service 1, Terminating 1, Warm 1, Desired 1, Max 2`);
  const b = chart(markup, `Instance history for ${second.name}; latest sample: Pending 1, In service 1, Terminating 1, Warm 1, Desired 1, Max 4`);
  t.assert.equal(Boolean(a && b), true);
  const series = (svg: string, name: string): string =>
    new RegExp(`<g\\b[^>]*class="[^"]*\\bspark__series--${name}\\b[^"]*"[^>]*>[\\s\\S]*?</g>`).exec(svg)?.[0] ?? "";
  const path = (group: string, name: string): string =>
    new RegExp(`<path\\b(?=[^>]*\\bdata-series="${name}")[^>]*>`).exec(group)?.[0] ?? "";
  for (const name of ["pending", "inService", "terminating", "warm", "desired", "max"]) {
    const line = series(a, name);
    t.assert.match(path(line, name), /\bd="[^"]*M[^"]*M[^"]*M/);
    t.assert.match(line, /<circle\b[^>]*class="spark__point"/);
    t.assert.match(path(series(b, name), name), /\bd="[^"]*M[^"]*M/);
  }
  for (const name of ["desired", "max"]) {
    t.assert.match(path(series(a, name), name), /\bclass="[^"]*\bspark__run--reference\b/);
  }
  t.assert.match(a, /<title>not recorded<\/title>/);
  t.assert.match(series(a, "pending"), /<circle class="spark__point"[^>]*cy="42"/);
  t.assert.notEqual(series(a, "pending"), series(b, "pending"));
  const aId = /<pattern id="([^"]+)"/.exec(a)?.[1];
  const bId = /<pattern id="([^"]+)"/.exec(b)?.[1];
  t.assert.equal(Boolean(aId && bId), true);
  t.assert.notEqual(aId, bId);
});

test("legacy fleet samples show not recorded instead of zero", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const base = recentState(2);
  const state = { ...base, fleets: [fleet] };
  const markup = html({ state }, <Overview />);
  const svg = chart(markup, `Instance history for ${fleet.name}; latest sample: not recorded`);
  t.assert.equal(Boolean(svg), true);
  t.assert.match(svg, /<title>not recorded<\/title>/);
  t.assert.match(visibleText(markup), /not recorded/);
  t.assert.doesNotMatch(svg, /<circle\b[^>]*class="spark__point"/);
  for (const name of ["pending", "inService", "terminating", "warm", "desired", "max"]) {
    const path = new RegExp(`<path\\b(?=[^>]*\\bdata-series="${name}")[^>]*>`).exec(svg)?.[0] ?? "";
    t.assert.notEqual(path, "");
    t.assert.doesNotMatch(path, /\bd="[^"]+"/);
  }
});

test("action cache describes lookup counts and the configured window", (t) => {
  const base = recentState(3);
  for (const [window, duration] of [["300s", "5m 00s"], ["900s", "15m 00s"]] as const) {
    const noLookups = { ...base, recent: { ...base.recent, window } };
    const noLookupText = visibleText(html({ state: noLookups }, <Overview />));
    t.assert.match(noLookupText, /no\s+lookups/);
    t.assert.match(noLookupText, new RegExp(`hit rate\\s*·\\s*${duration.replace(" ", "\\s+")}`));

    const zeroHits = {
      ...base,
      recent: {
        ...base.recent,
        window,
        samples: base.recent.samples.map((sample, i) => ({
          ...sample,
          lookups: i === 0 ? 160 : 0,
          hits: 0,
        })),
      },
    };
    const zeroHitText = visibleText(html({ state: zeroHits }, <Overview />));
    t.assert.match(zeroHitText, /0%\s+of\s+160\s+lookups/);
    t.assert.match(zeroHitText, new RegExp(`hit rate\\s*·\\s*${duration.replace(" ", "\\s+")}`));
  }
});

test("cache chart connects recorded zero-lookup steps and shows lookup area", (t: TestContext) => {
  const base = recentState(3);
  const lookups = [10, 0, 100];
  const hits = [9, 0, 1];
  const state = {
    ...base,
    recent: {
      ...base.recent,
      window: "30s",
      samples: base.recent.samples.map((sample, i) => ({
        ...sample,
        lookups: lookups[i]!,
        hits: hits[i]!,
      })),
    },
  };
  const svg = chart(html({ state }, <Overview />), "Action-cache lookups as area and hits as line per step");
  const path = /<path class="spark__run" d="([^"]*)"/.exec(svg)?.[1] ?? "";
  const area = /<path class="spark__area" d="([^"]*)"/.exec(svg)?.[1] ?? "";
  t.assert.equal((path.match(/M/g) ?? []).length, 1);
  t.assert.equal((area.match(/M/g) ?? []).length, 1);
  const [hitPoints = [], areaPoints = []] = [path, area].map((d) =>
    [...d.matchAll(/[ML](-?[\d.]+)\s+(-?[\d.]+)/g)]
      .map((match) => [Number(match[1]), Number(match[2])] as const));
  t.assert.equal(hitPoints.length, 3);
  t.assert.equal(areaPoints.length, 5);
  for (let i = 0; i < 3; i++) {
    const hit = hitPoints[i];
    const lookup = areaPoints[i];
    t.assert.ok(hit && lookup);
    t.assert.equal(hit[0], lookup[0]);
    t.assert.ok(hit[1] >= lookup[1], `step ${i}: hits must not exceed lookups`);
  }
  t.assert.equal(hitPoints[1]?.[1], 42);
  t.assert.equal(areaPoints[1]?.[1], 42);
  t.assert.equal(areaPoints[3]?.[1], 42);
  t.assert.equal(areaPoints[4]?.[1], 42);
  t.assert.doesNotMatch(svg, /<circle\b[^>]*class="spark__point"/);
});

test("a page opened once renders Done/min and the full charts from the first frame", (t) => {
  const markup = html({ state: recentState(90) }, <Overview />);
  t.assert.match(markup, /Done \/ min<\/h2><div class="readout__big"><span[^>]*>60<\/span><\/div>/);
  t.assert.doesNotMatch(markup, /over \d+s/);
  for (const label of ["Done per minute", "Executing", "Queued"]) {
    const svg = chart(markup, label);
    const path = /<path class="spark__run" d="([^"]*)"/.exec(svg)?.[1] ?? "";
    const points = (path.match(/[ML]/g) ?? []).length;
    t.assert.equal(points === 89 || points === 90, true, `${label}: ${points} points`);
  }
  const strip = passStrip(markup);
  t.assert.equal((strip.match(/<i\b/g) ?? []).length, 90);
  t.assert.doesNotMatch(strip, /is-unrecorded/);
});

test("a restarted server shows the time before its first sample as not recorded, never zero", (t) => {
  const markup = html({ state: recentState(3) }, <Overview />);
  t.assert.match(markup, /over <time>20s<\/time>/);
  t.assert.equal((passStrip(markup).match(/is-unrecorded/g) ?? []).length, 87);
  for (const label of ["Done per minute", "Executing", "Queued"]) {
    t.assert.match(chart(markup, label), /spark__unrecorded/, label);
  }
});

test("startup distinguishes an unrecorded first step from an incomplete outage", (t) => {
  const base = recentState(1);
  const pending = { ...base, recent: { ...base.recent, samples: base.recent.samples.map((s) => ({ ...s, schedulerKnown: false })) } };
  t.assert.match(visibleText(html({ state: pending }, <Overview />)), /not recorded yet/);
  const outage = { ...pending, schedulerKnown: false };
  t.assert.match(visibleText(html({ state: outage }, <Overview />)), /not available/);
  const recovered = { ...pending, schedulerKnown: true };
  t.assert.match(visibleText(html({ state: recovered }, <Overview />)), /not available/);
});

test("live scheduler readouts recover without resetting incomplete charts", (t) => {
  const base = recentState(4);
  const samples = base.recent.samples.map((s, i) => ({ ...s, schedulerKnown: i !== 3 }));
  const outage = { ...base, schedulerKnown: false, queue: { ...base.queue, total: 17 }, recent: { ...base.recent, window: "40s", samples } };
  const recovered = { ...outage, schedulerKnown: true };
  const before = html({ state: outage }, <Overview />);
  const after = html({ state: recovered }, <Overview />);
  t.assert.match(visibleText(before), /Queue\s+unknown/);
  t.assert.match(visibleText(after), /Queue\s+17/);
  t.assert.doesNotMatch(visibleText(after), /Executing\s+unknown/);
  for (const label of ["Queued", "Executing", "Done per minute", "Evicted per step"]) {
    t.assert.equal(chart(after, label), chart(before, label), label);
    t.assert.match(chart(after, label), /spark__unknown/, label);
  }
  t.assert.match(visibleText(before), /Used unknown/);
  t.assert.doesNotMatch(visibleText(after), /Used unknown/);
  const workersBefore = visibleText(html({ state: outage }, <Workers />));
  const workersAfter = visibleText(html({ state: recovered }, <Workers />));
  t.assert.match(workersBefore, /Scheduler unknown/);
  t.assert.doesNotMatch(workersAfter, /Scheduler unknown/);
});

test("an interior scheduler outage is hatched without interrupting local cache or changing the chart timeline", (t) => {
  const base = recentState(4);
  const samples = base.recent.samples.map((sample, i) => ({
    ...sample, schedulerKnown: i !== 1, lookups: 10, hits: 5, evictedBytes: 10,
  }));
  const state = { ...base, recent: { ...base.recent, window: "40s", samples } };
  const markup = html({ state }, <Overview />);
  for (const label of ["Queued", "Executing", "Done per minute", "Evicted per step"]) {
    const svg = chart(markup, label);
    t.assert.match(svg, /viewBox="0 0 300 44"/, label);
    t.assert.match(svg, /class="spark__unknown"[^>]*><title>scheduler unknown<\/title>/, label);
    t.assert.match(svg, /class="spark__run" d="[^"]*M[^"]*M/, label);
  }
  const cache = chart(markup, "Action-cache lookups as area and hits as line per step");
  t.assert.doesNotMatch(cache, /spark__unknown/);
  t.assert.equal((/<path class="spark__run" d="([^"]*)"/.exec(cache)?.[1]?.match(/[ML]/g) ?? []).length, 4);
  t.assert.match(markup, /Done \/ min<\/h2><div class="readout__big"><span class="num live">–<\/span>/);
  t.assert.match(visibleText(markup), /Evicted \(window, all stores\) unknown/);
  const outage = { ...state, recent: { ...state.recent, samples: samples.map((s, i) => ({ ...s, schedulerKnown: i !== 3 })) } };
  const outageText = visibleText(html({ state: outage }, <Overview />));
  t.assert.match(outageText, /Queue\s+unknown/);
  t.assert.match(outageText, /Executing\s+unknown/);
});

test("worker lanes mark an interior scheduler outage and resume the original phase", (t) => {
  const base = recentState(4);
  const worker = base.workers[0];
  if (!worker) throw new Error("the live recording has a worker");
  const first = base.recent.samples[0]!;
  const state = {
    ...base,
    workers: [{ ...worker, launchTime: first.start, phaseTime: first.start, phases: [{ phase: "BUSY" as const, start: first.start }] }],
    recent: { ...base.recent, window: "40s", samples: base.recent.samples.map((s, i) => ({ ...s, schedulerKnown: i !== 1 })) },
  };
  const markup = html({ state }, <Workers />);
  t.assert.match(markup, /lane__seg--busy/);
  t.assert.match(markup, /lane__seg--unrecorded" title="scheduler unknown"/);
  t.assert.match(markup, /Phase history: (?:off, )?busy, scheduler unknown/);
});

test("isolated chart markers stay entirely inside their viewBox", (t: TestContext) => {
  const base = recentState(3);
  const state = {
    ...base,
    recent: {
      ...base.recent,
      window: "30s",
      samples: base.recent.samples.map((sample, i) => ({
        ...sample,
        passes: i === 1 ? 0 : 1,
        queued: i === 0 ? 5 : 0,
        running: i === 0 ? 5 : 0,
        lookups: i === 1 ? 0 : 100,
        hits: i === 0 ? 100 : 0,
      })),
    },
  };
  const markup = html({ state }, <Overview />);
  const epsilon = 0.01;
  for (const label of ["Queued", "Executing"]) {
    const svg = chart(markup, label);
    const viewBox = /viewBox="([^"]+)"/.exec(svg)?.[1]?.trim().split(/[\s,]+/).map(Number) ?? [];
    t.assert.equal(viewBox.length, 4, `${label}: viewBox`);
    const [minX = NaN, minY = NaN, width = NaN, height = NaN] = viewBox;
    t.assert.equal(minX, 0, `${label}: viewBox x origin`);
    t.assert.equal(minY, 0, `${label}: viewBox y origin`);
    t.assert.ok(width > 0 && height > 0, `${label}: viewBox dimensions`);
    const markers = [...svg.matchAll(/<circle\b[^>]*>/g)];
    t.assert.ok(markers.length >= 2, `${label}: expected at least two markers`);
    for (const [marker] of markers) {
      const attribute = (name: string): number => {
        const value = new RegExp(`\\b${name}="([^"]+)"`).exec(marker)?.[1];
        t.assert.ok(value !== undefined, `${label}: missing ${name}`);
        const number = Number(value);
        t.assert.ok(Number.isFinite(number), `${label}: invalid ${name}`);
        return number;
      };
      const cx = attribute("cx");
      const cy = attribute("cy");
      const r = attribute("r");
      t.assert.ok(r > 0, `${label}: marker radius`);
      t.assert.ok(cx - r >= -epsilon, `${label}: marker extends left`);
      t.assert.ok(cy - r >= -epsilon, `${label}: marker extends above`);
      t.assert.ok(cx + r <= width + epsilon, `${label}: marker extends right`);
      t.assert.ok(cy + r <= height + epsilon, `${label}: marker extends below`);
    }
  }
});

test("cache readout, bounded meter and bucket measurement retain their meaning", (t) => {
  const noSamples = { ...recentState(0), cache: {} };
  const pending = html({ state: noSamples }, <Overview />);
  t.assert.match(pending, /<h2 class="label">Action cache<\/h2>(?:(?!<div class="readout">)[\s\S])*?<div class="readout__sub"><span>hit rate · [\s\S]*?<\/span><span>—<\/span><\/div>/);
  t.assert.match(pending, /Bucket size not measured yet; S3 reports once a day\./);
  const state = { ...recentState(1), cache: { stores: [{ store: "CAS" as const, usedBytes: 25, budgetBytes: 100 }] } };
  const bounded = html({ state }, <Overview />);
  t.assert.match(bounded, /aria-label="CAS 25% of budget"/);
  t.assert.match(bounded, /--g:\s*0\.25/);
  t.assert.doesNotMatch(bounded, /Bucket size not measured yet/);
  const zeroBudget = html({ state: { ...state, cache: { stores: [{ store: "CAS", usedBytes: 25, budgetBytes: 0 }] } } }, <Overview />);
  t.assert.match(zeroBudget, /aria-label="CAS — of budget"/);
  t.assert.doesNotMatch(zeroBudget, /Infinity|NaN/);
  for (const label of ["Transferred per step", "Evicted per step"]) {
    t.assert.match(chart(bounded, label), /spark__unrecorded/, label);
  }
  const measured = iso(ms(live.updateTime) - 86400000);
  const bucket = html({ state: { ...state, cache: { bucket: { bytes: 1024, objects: 42, measureTime: measured } } } }, <Overview />);
  t.assert.match(bucket, /1 KiB/);
  t.assert.match(bucket, /42/);
  t.assert.match(bucket, /as of <time class="mono">1d 0h<\/time>/);
  t.assert.match(bucket, /expiry by bucket lifecycle/);
});

test("cache and build rows retain cell labels when column headers are visually hidden", (t) => {
  const bounded = html({ state: live }, <Overview />);
  for (const label of ["Store", "Used", "Budget", "Usage", "Evicted (window, all stores)"]) {
    t.assert.equal(bounded.includes(`<span class="build-label">${label} </span>`), true, label);
  }
  const bucket = html({ state: stale }, <Overview />);
  for (const label of ["Bytes", "Objects", "Measurement"]) {
    t.assert.equal(bucket.includes(`<span class="build-label">${label} </span>`), true, label);
  }
  t.assert.match(bucket, /<td role="cell" class="a-span"><span class="build-label">Measurement <\/span>/);
  const builds = html({ state: live }, <Builds />);
  t.assert.match(builds, /<td role="cell" class="a-span"><span class="build-label">Transferred <\/span>/);
});

test("transfer directions share a scale and missing steps on one chart, separate from evictions", (t) => {
  const state = recentState(2);
  const [first, second] = state.recent.samples;
  const recent = { ...state.recent, window: "30s", samples: [
    { ...first!, downloadedBytes: 100, uploadedBytes: 50, evictedBytes: 8 },
    { ...second!, downloadedBytes: 50, uploadedBytes: 25, evictedBytes: 4 },
  ] };
  const markup = html({ state: { ...state, recent } }, <Overview />);
  const transfer = chart(markup, "Transferred per step");
  t.assert.match(transfer, /spark__unrecorded/);
  t.assert.match(transfer, /class="spark__run" d="M150\.0 2\.0 L298\.0 22\.0"/);
  t.assert.match(transfer, /class="spark__run spark__run--secondary" d="M150\.0 22\.0 L298\.0 32\.0"/);
  t.assert.match(visibleText(markup), /Downloaded/);
  t.assert.match(visibleText(markup), /Uploaded/);
  const eviction = chart(markup, "Evicted per step");
  t.assert.match(eviction, /spark__unrecorded/);
  t.assert.doesNotMatch(eviction, /spark__run--secondary/);
});

test("build cache transfers show download and upload in both views", (t) => {
  const row = { ...build, usage: { ...build.usage, cache: { ...build.usage.cache, downloadedBytes: 2048, uploadedBytes: 1024 } } };
  const detail = visibleText(html({}, <BuildView build={row} />));
  t.assert.match(detail, /Downloaded 2 KiB · Uploaded 1 KiB/);
  const markup = html({ state: { ...live, builds: [row] } }, <Builds />);
  t.assert.match(visibleText(markup), /Transferred/);
  t.assert.match(visibleText(markup), /↓ 2 KiB ↑ 1 KiB/);
});

test("every section header renders through the one header component", (t) => {
  for (const [name, rendered] of SCREENS) {
    const markup = rendered();
    t.assert.doesNotMatch(markup, /(?:section__head|wgroup__head)/, name);
    for (const match of markup.matchAll(/<section\b[^>]*aria-labelledby="([^"]+)"[^>]*>/g)) {
      const id = match[1]!;
      t.assert.equal(markup.includes(`<div class="shead"><h2 id="${id}"`), true, `${name}: ${id}`);
    }
    if (name === "forbidden" || name.startsWith("sign")) continue;
    const withoutEmpty = markup.replace(/<div class="empty">[\s\S]*?<\/div>/g, "");
    for (const [heading] of withoutEmpty.matchAll(/<h2\b[^>]*>/g)) {
      if (/class="label"/.test(heading)) continue;
      t.assert.match(heading, /class="shead__title(?: shead__title--mono)?"/, name);
    }
  }
});

test("Tiles spans the final readout for four- and five-column grids", (t) => {
  const cases = [
    { columns: 4, count: 0, span: 1 },
    { columns: 4, count: 1, span: 4 },
    { columns: 4, count: 3, span: 2 },
    { columns: 4, count: 5, span: 4 },
    { columns: 4, count: 6, span: 3 },
    { columns: 4, count: 7, span: 2 },
    { columns: 5, count: 0, span: 1 },
    { columns: 5, count: 1, span: 5 },
    { columns: 5, count: 3, span: 3 },
    { columns: 5, count: 5, span: 1 },
    { columns: 5, count: 6, span: 5 },
    { columns: 5, count: 7, span: 4 },
  ];
  for (const { columns, count, span } of cases) {
    const tiles = Array.from({ length: count }, (_, index) =>
      <Tile key={index} label={`Label ${index}`} value={`Value ${index}`} />);
    const markup = render(<Tiles columns={columns}>{tiles}</Tiles>);
    const context = `${columns} columns, ${count} readouts`;
    t.assert.match(markup, new RegExp(`--tile-columns:\\s*${columns}(?:;|")`), context);
    t.assert.match(markup, new RegExp(`--tile-last-span:\\s*${span}(?:;|")`), context);
    t.assert.equal((markup.match(/class="readout"/g) ?? []).length, count, context);
    t.assert.equal(markup.slice(markup.indexOf(">") + 1, -6), tiles.map((tile) => render(tile)).join(""), context);
  }
});

test("Tiles flattens nested children and ignores null and false with default columns", (t) => {
  const tiles = [0, 1, 2].map((index) =>
    <Tile key={index} label={`Label ${index}`} value={`Value ${index}`} />);
  const markup = render(<Tiles>{[null, [tiles[0], false, [tiles[1], null]], tiles[2], false]}</Tiles>);
  t.assert.match(markup, /--tile-columns:\s*5(?:;|")/);
  t.assert.match(markup, /--tile-last-span:\s*3(?:;|")/);
  t.assert.equal((markup.match(/class="readout"/g) ?? []).length, 3);
  t.assert.equal(markup.slice(markup.indexOf(">") + 1, -6), tiles.map((tile) => render(tile)).join(""));

  const emptyMarkup = render(<Tiles>{[null, [false, []]]}</Tiles>);
  t.assert.match(emptyMarkup, /--tile-columns:\s*5(?:;|")/);
  t.assert.match(emptyMarkup, /--tile-last-span:\s*1(?:;|")/);
  t.assert.equal((emptyMarkup.match(/class="readout"/g) ?? []).length, 0);
  t.assert.equal(emptyMarkup.slice(emptyMarkup.indexOf(">") + 1, -6), "");
});

test("every overview readout reserves a chart slot, including instances", (t) => {
  for (const count of [0, 3, 90]) {
    const markup = html({ state: recentState(count) }, <Overview />);
    t.assert.equal((markup.match(/class="[^"]*\breadout__chart\b[^"]*"/g) ?? []).length, 5, `${count} samples`);
    t.assert.match(
      markup,
      /<div class="readout"><h2 class="label">Instances<\/h2>(?:(?!<div class="readout">)[\s\S])*?<div class="[^"]*\breadout__chart\b[^"]*"[^>]*>\s*<\/div>/,
      `${count} samples: instances chart slot is empty`,
    );
  }
});

test("fleet badges sit on the name's row whatever the name length", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const names = ["a", "build-worker-aarch64-with-a-very-long-name"];
  const state = { ...live, fleets: names.map((name) => ({
    ...fleet, name: `fleets/${name}`, properties: { ...fleet.properties, capacity: "spot" },
  })) };
  const markup = html({ state }, <Overview />);
  for (const name of names) {
    t.assert.match(markup, new RegExp(`<div class="fleet__id"><span class="fleet__ord">[\\s\\S]*?<\\/span><h3 class="fleet__name"><span class="ident" title="${name}">${name}<\\/span><\\/h3><span class="fleet__tags"><span class="tag tag--spot">spot<\\/span>`));
  }
});

test("queued actions show their refusal", (t) => {
  const queued = cold.actions?.queued[0];
  if (!queued || !live.actions) throw new Error("the recordings have a queued action and an action sample");
  const state = { ...live, actions: { ...live.actions, queued: [{ ...queued, candidates: [], refusal: "cpu exceeds every fleet" }] } };
  t.assert.match(html({ state }, <Actions />), /class="[^"]*caution[^"]*"[^>]*>cpu exceeds every fleet/);
});

test("worker rows distinguish instances with the same uuid prefix", (t) => {
  const worker = live.workers[0];
  if (!worker) throw new Error("the live recording has a worker");
  const suffix = "0ee112f6-7a36-7faf-be44-719a1246ef6b";
  const workers = ["i-first", "i-second"].map((instance) => ({
    ...worker, name: `workers/${instance}-${suffix}`, instance: `instances/${instance}`,
  }));
  const text = visibleText(html({ state: { ...live, workers } }, <Workers />));
  t.assert.match(text, /i-first/);
  t.assert.match(text, /i-second/);
  t.assert.doesNotMatch(text, /0ee1/);
});

test("a phase entered after the last lane step shows in the lane", (t) => {
  const worker = live.workers[0];
  if (!worker) throw new Error("the live recording has a worker");
  const at = Math.floor(ms(live.updateTime) / 60000) * 60000 + 5000;
  const start = iso(at - 3000);
  const state = {
    ...live,
    updateTime: iso(at),
    workers: [{ ...worker, launchTime: start, phase: "BUSY" as const, phaseTime: start, phases: [{ phase: "BUSY" as const, start }] }],
  };
  const rendered = html({ state }, <Workers />);
  t.assert.equal(rendered.includes("lane__seg--busy"), true);
});

test("an event after the last step stays inside the activity lane", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const at = Math.floor(ms(live.updateTime) / 60000) * 60000 + 5000;
  const state = {
    ...live,
    updateTime: iso(at),
    events: [{ ...common, fleet: fleet.name, createTime: iso(at - 2000), scaledOut: { from: 1, to: 2 } }],
  };
  const rendered = html({ state }, <Activity />);
  const positions = [...rendered.matchAll(/--x:\s*([0-9.]+)/g)].map((match) => Number(match[1]));
  t.assert.equal(positions.length, 1);
  t.assert.equal(positions.every((x) => x <= 1), true);
});

test("launch failure and withdrawal events show their details", (t) => {
  const failure = render(<EventRow withCause e={{ ...common, launchFailed: { from: 2, to: 4, reason: "spot quota exceeded" } }} />);
  t.assert.match(visibleText(failure), /2 of 4 launched/);
  t.assert.match(failure, /event__cause caution[^>]*>spot quota exceeded/);
  const withdrawn = render(<EventRow withCause e={{ ...common, withdrawn: { from: 4, to: 2 } }} />);
  t.assert.match(visibleText(withdrawn), /4 → 2/);
});

test("terminating hosted old member and replacement occupy two cells at desired one", (t) => {
  const fleet = live.fleets[0];
  const worker = live.workers[0];
  const instance = live.instances[0];
  if (!fleet || !worker || !instance) throw new Error("fixture has fleet, worker and instance");
  const f = { ...fleet, desired: 1, policy: { ...fleet.policy, max: 2 }, instances: { ...fleet.instances, terminating: 1, warm: 0 } };
  const old = { ...worker, fleet: f.name, name: "workers/old", instance: "instances/old", phase: "BUSY" as const, infrastructure: { phase: "REPLACING" as const, reason: "SPOT_REBALANCE" as const } };
  const replacement = { ...worker, fleet: f.name, name: "workers/new", instance: "instances/new", phase: "IDLE" as const, running: 0, infrastructure: { phase: "REPLACEMENT" as const, reason: "SPOT_REBALANCE" as const } };
  const census = { workers: [old, replacement], instances: [{ ...instance, fleet: f.name, name: old.instance, lifecycle: "TERMINATING" as const }] };
  t.assert.deepEqual(cellStates(f, census), ["replacing", "replacement"]);
  t.assert.equal(fleetCounts(f, census).reduce((n, [, count]) => n + count, 0), 2);
  const tape = html({}, <Tape f={f} census={census} />);
  t.assert.match(tape, /1 replacing, 1 replacement/);
  t.assert.match(tape, /Desired 1/);
  t.assert.match(tape, /--d:\s*1/);
  t.assert.match(tape, /c-replacing/);
  t.assert.match(tape, /c-replacement/);
  const legend = render(<Legend />);
  t.assert.match(legend, /c-replacing[^]*replacing/);
  t.assert.match(legend, /c-replacement[^]*replacement/);
});

test("census groups generations, isolates fleets, and preserves unknown states", (t) => {
  const fleet = live.fleets[0];
  const worker = live.workers[0];
  const instance = live.instances[0];
  if (!fleet || !worker || !instance) throw new Error("fixture has fleet, worker and instance");
  const f = { ...fleet, instances: { ...fleet.instances, warm: 0 } };
  const row = { ...worker, fleet: f.name, name: "workers/a", instance: "instances/a", phase: "DRAINING" as const, running: 1 };
  const second = { ...row, name: "workers/b", phase: "IDLE" as const, running: 0 };
  const pending = { ...instance, fleet: f.name, name: "instances/a", lifecycle: "PENDING" as const };
  const count = (workers: typeof live.workers, instances: typeof live.instances = []) => new Map(fleetCounts(f, { workers, instances }));
  t.assert.equal(count([row, second], [pending]).get("busy"), 1);
  t.assert.equal(count([row, { ...second, phase: "DRAINING" }], [pending]).get("draining"), 1);
  const replacing = { ...row, infrastructure: { phase: "REPLACING" as const, reason: "SPOT_REBALANCE" as const } };
  const replacement = { ...second, infrastructure: { phase: "REPLACEMENT" as const, reason: "SPOT_REBALANCE" as const } };
  t.assert.equal(count([replacing, replacement]).get("replacing"), 1);
  t.assert.equal(count([replacing, replacement]).get("replacement"), 0);
  t.assert.equal(count([second]).get("replacement"), 0);
  const future = "FUTURE" as unknown as typeof worker.phase;
  t.assert.equal(count([{ ...row, running: 0 }, { ...second, phase: future }]).get("unknown"), 1);
  t.assert.equal(count([{ ...row, running: 0 }, { ...second, phase: future }]).get("draining"), 0);
  t.assert.equal(count([{ ...row, phase: future, running: 1 }]).get("busy"), 1);
  const unattached = [{ ...row, name: "workers/x", instance: "" }, { ...row, name: "workers/y", instance: "" }];
  t.assert.equal(count(unattached).get("draining"), 2);
  const foreign = { ...row, fleet: "fleets/other" };
  t.assert.equal(count([foreign], [pending]).get("pending"), 1);
  const unknown = { ...instance, fleet: f.name, name: "instances/unknown", lifecycle: "FUTURE" as unknown as typeof instance.lifecycle };
  t.assert.equal(count([], [unknown, unknown]).get("unknown"), 1);
  t.assert.equal(count([], [unknown]).get("unconnected"), 0);
  t.assert.equal(cellStates(f, { workers: [], instances: [unknown] })[0], "unknown");
});

test("worker and unconnected instance show infrastructure without changing workload phase", (t) => {
  const worker = live.workers[0];
  const fleet = live.fleets.find((f) => f.name === worker?.fleet);
  const instance = live.instances[0];
  if (!worker || !fleet || !instance) throw new Error("fixture has worker, fleet and instance");
  const status = { phase: "REPLACING" as const, reason: "SPOT_REBALANCE" as const, otherInstance: "instances/i-new" };
  const w = { ...worker, phase: "BUSY" as const, infrastructure: status };
  const orphan = { ...instance, name: "instances/i-old", fleet: fleet.name, lifecycle: "IN_SERVICE" as const, infrastructure: status };
  const f = { ...fleet, workers: { busy: 1, idle: 0, draining: 0, unconnected: 1 }, instances: { ...fleet.instances, pending: 0, terminating: 0 } };
  const state = { ...live, fleets: [f], workers: [w], instances: [orphan] };
  const text = visibleText(html({ state }, <Workers />));
  t.assert.match(text, /busy/);
  t.assert.match(text, /replacing · spot rebalance · replacement i-new/);
  t.assert.match(text, /i-old · in service/);
  t.assert.match(visibleText(html({ state }, <WorkerDetail w={w} state={state} />)), /replacing · spot rebalance/);
  t.assert.doesNotMatch(text, /1 no worker/);
});

test("AWS status messages and the single Activity filter are visible", (t) => {
  const at = "2026-01-15T10:00:00Z";
  const event = (kind: AwsEventKind): ScalingEvent =>
    ({ ...common, name: "awsEvents/a", aws: { event: { id: "a", at, kind } } });
  const cases: [ScalingEvent, RegExp][] = [
    [event({ type: "rebalanceRecommendation", instance: "instances/i-old" }), /AWS: spot rebalance recommendation for i-old/],
    [event({ type: "spotInterruption", instance: "instances/i-old", action: "TERMINATE" }), /AWS: spot interruption notice for i-old \(2 min warning\)/],
    [event({ type: "spotInterruption", instance: "instances/i-old", action: "STOP" }), /AWS: spot interruption notice for i-old \(stop; 2 min warning\)/],
    [event({ type: "spotInterruption", instance: "instances/i-old", action: "HIBERNATE" }), /AWS: spot hibernation notice for i-old$/],
    [event({ type: "spotInterruption", instance: "instances/i-old", action: "FUTURE" } as unknown as AwsEventKind), /AWS: spot interruption notice for i-old$/],
    [event({ type: "instanceLaunched", instance: "instances/i-new" }), /AWS: i-new launched$/],
    [event({ type: "replacementLaunched", instance: "instances/i-new" }), /AWS: replacement i-new launched/],
    [event({ type: "replacementLaunched", instance: "instances/i-new", replacing: "instances/i-old" }), /AWS: spot rebalance: replacing i-old with i-new/],
    [event({ type: "instanceTerminated", instance: "instances/i-old", reason: "SPOT_RECLAIMED" }), /terminated: spot reclaimed/],
    [event({ type: "instanceTerminated", instance: "instances/i-old", reason: "REBALANCE" }), /terminated: rebalance/],
    [event({ type: "instanceTerminated", instance: "instances/i-old", reason: "OTHER", reasonDetail: "user request" }), /terminated: user request/],
    [event({ type: "capacityUnavailable", reason: "insufficient capacity" }), /capacity unavailable: insufficient capacity/],
  ];
  for (const [e, expected] of cases) t.assert.match(visibleText(render(<EventRow e={e} withCause={false} />)), expected);
  const state = { ...live, statusEvents: [cases[0]![0]] };
  const activity = html({ state }, <Activity />);
  t.assert.equal((activity.match(/>AWS <span class="num">/g) ?? []).length, 1);
  t.assert.match(visibleText(activity), /spot rebalance recommendation/);
  t.assert.match(visibleText(html({ state }, <Overview />)), /spot rebalance recommendation/);
});

test("unknown AWS observation renders a safe generic message", (t) => {
  const unknown = { ...common, aws: { event: { id: "a", at: common.createTime, kind: { type: "futureNotice" } } } } as unknown as ScalingEvent;
  t.assert.match(visibleText(render(<EventRow e={unknown} withCause={false} />)), /AWS: infrastructure event/);
});
