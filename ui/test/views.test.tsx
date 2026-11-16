// Every screen per scenario, as what it says: the visible text of each rendered page, plus the landmark a user relies on.
// Update: `node tools/bundle-test.mjs test/views.test.tsx test-bundle/views.test.mjs &&
// node --test --test-update-snapshots test-bundle/views.test.mjs`, from `ui/`.
import { snapshot, test } from "node:test";
import type { ComponentChildren } from "preact";
import { render } from "preact-render-to-string";
import type { EarlierBuilds } from "../src/client.ts";
import { clock } from "../src/format.ts";
import type { Screen } from "../src/routes.ts";
import { Shell } from "../src/shell.tsx";
import { ClientContext } from "../src/ui.tsx";
import { Access } from "../src/views/access.tsx";
import { Actions } from "../src/views/actions.tsx";
import { Activity } from "../src/views/activity.tsx";
import { Build, BuildView } from "../src/views/build.tsx";
import { autoLoadBuilds, Builds, filterBuilds } from "../src/views/builds.tsx";
import { fleetCounts, fleetTotals, Tape } from "../src/views/fleet.tsx";
import { Forbidden, SignIn, SignInError } from "../src/views/gate.tsx";
import { EventRow, Overview } from "../src/views/overview.tsx";
import { WorkerDetail, Workers } from "../src/views/workers.tsx";
import { ms } from "../src/wire-parse.ts";
import type { ScalingEvent } from "../src/wire.ts";
import { build, clientFor, session, signedOut, type Scenario } from "./scenario.ts";
import { iso, load, snapshotOf } from "./sse.ts";

snapshot.setResolveSnapshotPath(() => new URL("../test/views.snapshot", import.meta.url).pathname);
snapshot.setDefaultSnapshotSerializers([(html: unknown) => html]);
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
  ["access live", () => screen("access", {}, <Access />)],
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
  test(name, (t) => {
    const markup = rendered();
    t.assert.snapshot(visibleText(markup));
    const landmark = LANDMARKS[name];
    if (landmark) t.assert.match(markup, landmark, `${name}: landmark missing`);
  });
}

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
    t.assert.match(markup, /ops@example.org/);
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
  t.assert.doesNotMatch(markup, /invocation-example/);
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
  for (const expected of [/invocation-example/, /bazel 8.4.2/, /Remote work Gang saw; local actions, disk-cache hits and analysis are not included\./, /Remote cache/, /80%/, /Slowest actions/, /parser/, /Joined\s+3/]) t.assert.match(visibleText(markup), expected);
});

test("a missing build says it is not held", (t) => {
  const client = clientFor({});
  client.build.value = { kind: "missing", id: "missing" };
  const markup = render(<ClientContext.Provider value={client}><Build id="missing" /></ClientContext.Provider>);
  t.assert.match(markup, /build not held: only the latest builds are kept in memory/);
});

test("overview includes remote cache hit rate", (t) => {
  const markup = html({}, <Overview />);
  t.assert.match(markup, /Remote cache/);
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
    t.assert.match(visibleText(markup), /invocation-example/);
  } finally {
    route.value = previous;
  }
});

const WORKER = "i-04828e21626b14dd2-0ee112f6-7a36-7faf-be44-719a1246ef6b";
const common = { name: "events/1022", createTime: live.updateTime, fleet: "fleets/amd64-on-demand" };

test("worker events name the worker and its verb", (t) => {
  const row = (e: ScalingEvent) => render(<EventRow withCause e={e} />);
  t.assert.match(row({ ...common, workerRegistered: { worker: `workers/${WORKER}` } }), /i-04828e21626b14dd2 joined/);
  t.assert.match(row({ ...common, workerDeregistered: { worker: `workers/${WORKER}` } }), /i-04828e21626b14dd2 left/);
});

test("a minimum cause is shown without caution styling", (t) => {
  const row = render(<EventRow withCause e={{ ...common, cause: "MINIMUM", scaledOut: { from: 0, to: 1 } }} />);
  t.assert.match(row, /<span class="event__cause">minimum<\/span>/);
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
  if (!spot || !onDemand) throw new Error("the live recording has two fleets");
  t.assert.deepEqual(fleetCounts(spot), [
    ["busy", 9],
    ["idle", 1],
    ["draining", 1],
    ["unconnected", 1],
    ["pending", 2],
    ["terminating", 1],
    ["warm", 4],
  ]);
  t.assert.deepEqual(fleetTotals([spot, onDemand]).slice(0, 3), [
    ["busy", 11],
    ["idle", 2],
    ["draining", 1],
  ]);
  t.assert.match(html({}, <Tape f={spot} />), /9 busy, 1 idle, 1 draining, 1 no worker, 2 pending, 1 terminating, of 24 maximum/);
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
  t.assert.match(markup, /<thead>[\s\S]*Target[\s\S]*Mnemonic[\s\S]*Running[\s\S]*<\/thead>/);
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
        completed: 10, running: 100, queued: 5, passes: 5, skipped: 0, lookups: 0, hits: 0,
      })),
    },
  };
};

const chart = (markup: string, label: string): string =>
  new RegExp(`<svg\\b[^>]*aria-label="${label}"[^>]*>[\\s\\S]*?<\\/svg>`).exec(markup)?.[0] ?? "";

const passStrip = (markup: string): string =>
  /<div class="passbar"[^>]*>[\s\S]*?<\/div>/.exec(markup)?.[0] ?? "";

test("a page opened once renders Done/min and the full charts from the first frame", (t) => {
  const markup = html({ state: recentState(90) }, <Overview />);
  t.assert.match(markup, /Done \/ min<\/h2><p class="readout__big"><span[^>]*>60<\/span><\/p>/);
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
  t.assert.match(markup, /over 20s/);
  t.assert.equal((passStrip(markup).match(/is-unrecorded/g) ?? []).length, 87);
  for (const label of ["Done per minute", "Executing", "Queued"]) {
    t.assert.match(chart(markup, label), /spark__unrecorded/, label);
  }
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

test("fleet badges sit on the name's row whatever the name length", (t) => {
  const fleet = live.fleets[0];
  if (!fleet) throw new Error("the live recording has a fleet");
  const names = ["a", "build-worker-aarch64-with-a-very-long-name"];
  const state = { ...live, fleets: names.map((name) => ({
    ...fleet, name: `fleets/${name}`, properties: { ...fleet.properties, capacity: "spot" },
  })) };
  const markup = html({ state }, <Overview />);
  for (const name of names) {
    t.assert.match(markup, new RegExp(`<div class="fleet__id"><span class="fleet__ord">[\\s\\S]*?<\\/span><h3 class="fleet__name" title="${name}">${name}<\\/h3><span class="fleet__tags"><span class="tag tag--spot">spot<\\/span>`));
  }
});

test("identifiers never wrap", (t) => {
  const row = render(<EventRow withCause e={{ ...common, fleet: "fleets/build-worker-x86-64", scaledOut: { from: 0, to: 1 } }} />);
  t.assert.match(row, /<span class="ident" title="build-worker-x86-64">build-worker-x86-64<\/span>/);
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
