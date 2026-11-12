// Every screen per scenario, as what it says: the visible text of each rendered page, plus the landmark a user relies on.
// Update: `node tools/bundle-test.mjs test/views.test.tsx test-bundle/views.test.mjs &&
// node --test --test-update-snapshots test-bundle/views.test.mjs`, from `ui/`.
import { snapshot, test } from "node:test";
import type { ComponentChildren } from "preact";
import { render } from "preact-render-to-string";
import type { Screen } from "../src/routes.ts";
import { Shell } from "../src/shell.tsx";
import { ClientContext } from "../src/ui.tsx";
import { Access } from "../src/views/access.tsx";
import { Actions } from "../src/views/actions.tsx";
import { Activity } from "../src/views/activity.tsx";
import { fleetCounts, fleetTotals, Tape } from "../src/views/fleet.tsx";
import { Forbidden, SignIn, SignInError } from "../src/views/gate.tsx";
import { EventRow, Overview } from "../src/views/overview.tsx";
import { Workers } from "../src/views/workers.tsx";
import { ms } from "../src/wire-parse.ts";
import type { ScalingEvent } from "../src/wire.ts";
import { clientFor, session, signedOut, type Scenario } from "./scenario.ts";
import { load, snapshotOf } from "./sse.ts";

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

const WORKER = "i-04828e21626b14dd2-0ee112f6-7a36-7faf-be44-719a1246ef6b";
const common = { name: "events/1022", createTime: live.updateTime, fleet: "fleets/amd64-on-demand" };

test("worker events name the worker and its verb", (t) => {
  const row = (e: ScalingEvent) => render(<EventRow withCause e={e} />);
  t.assert.match(row({ ...common, workerRegistered: { worker: `workers/${WORKER}` } }), /0ee1 joined/);
  t.assert.match(row({ ...common, workerDeregistered: { worker: `workers/${WORKER}` } }), /0ee1 left/);
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
