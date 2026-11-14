import assert from "node:assert/strict";
import { test } from "node:test";
import type { ComponentChildren } from "preact";
import { render } from "preact-render-to-string";
import { routeOf } from "../src/routes.ts";
import { Shell } from "../src/shell.tsx";
import { ClientContext } from "../src/ui.tsx";
import { Access } from "../src/views/access.tsx";
import { ApprovalRefused, Device, DeviceApproved, DeviceRequest, LookupRefused, approveDevice, lookupDevice } from "../src/views/device.tsx";
import { Forbidden, SignIn, SignInError } from "../src/views/gate.tsx";
import { Overview } from "../src/views/overview.tsx";
import { Workers } from "../src/views/workers.tsx";
import { clientFor, type Scenario } from "./scenario.ts";

Object.defineProperty(globalThis, "location", { value: new URL("https://gang.example.org/ui/") });

test("the app root shows the screen the route and the link call for", async () => {
  // App initializes its route from location at module evaluation.
  const { App, route } = await import("../src/app.tsx");
  const rows: { name: string; pathname: string; search?: string; scenario: Scenario; absentSession?: boolean; expected: ComponentChildren }[] = [
    { name: "live overview", pathname: "/ui/", scenario: {}, expected: <Shell screen="overview"><Overview /></Shell> },
    { name: "live workers", pathname: "/ui/workers", scenario: {}, expected: <Shell screen="workers"><Workers /></Shell> },
    { name: "signed out", pathname: "/ui/", scenario: { link: { kind: "signed_out" } }, expected: <SignIn /> },
    { name: "forbidden actions", pathname: "/ui/actions", scenario: { link: { kind: "forbidden" } }, expected: <Shell screen="actions"><Forbidden /></Shell> },
    { name: "forbidden access", pathname: "/ui/access", scenario: { link: { kind: "forbidden" } }, expected: <Shell screen="access"><Access /></Shell> },
    { name: "sign-in error takes precedence", pathname: "/ui/error", search: "?reason=denied", scenario: { link: { kind: "signed_out" } }, expected: <SignInError reason="denied" /> },
    { name: "device approval takes precedence", pathname: "/ui/device", search: "?reason=failed", scenario: { link: { kind: "signed_out" } }, absentSession: true, expected: <Device reason="failed" /> },
    { name: "device confirmation takes precedence", pathname: "/ui/device/approved", scenario: { link: { kind: "signed_out" } }, absentSession: true, expected: <DeviceApproved /> },
    { name: "no session", pathname: "/ui/", scenario: {}, absentSession: true, expected: null },
  ];
  for (const row of rows) {
    route.value = routeOf(row.pathname, row.search ?? "");
    const client = clientFor(row.scenario);
    if (row.absentSession) client.session.value = null;
    assert.equal(
      render(<App client={client} />),
      render(<ClientContext.Provider value={client}>{row.expected}</ClientContext.Provider>),
      row.name,
    );
  }
});

test("a fetched device request shows its address and an approve button", async () => {
  const request: typeof fetch = async (input) => {
    assert.equal(input, "/v1/deviceAuthorizations/ABCD%2FEFGH");
    return new Response(JSON.stringify({
      name: "deviceAuthorizations/ABCD-EFGH",
      provider: "example",
      verificationUri: "https://gang.example.org/ui/device",
      clientAddress: "192.0.2.1",
      state: "PENDING",
      createTime: "2026-01-01T00:00:00Z",
      expireTime: "2026-01-01T00:10:00Z",
      pollInterval: "5s",
    }));
  };
  const authorization = await lookupDevice("ABCD/EFGH", request);
  const markup = render(<DeviceRequest authorization={authorization} code="ABCD/EFGH" now={Date.parse("2026-01-01T00:01:00Z")} />);
  assert.match(markup, /192\.0\.2\.1/);
  assert.match(markup, /<dt>Requested<\/dt><dd>1m 00s ago<\/dd>/);
  assert.match(markup, /<dt>Expires in<\/dt><dd>9m 00s<\/dd>/);
  assert.match(markup, /<dt>Sign in with<\/dt><dd>Example<\/dd>/);
  assert.match(markup, /Pending login/);
  assert.match(markup, /Approve only if you just ran/);
  assert.doesNotMatch(markup, /<form\b/);
  assert.match(markup, /<button[^>]*type="button"/);
  assert.match(markup, /Sign in with Example to approve/);
});

test("device approval posts JSON and returns the authorization URL", async () => {
  const request: typeof fetch = async (input, init) => {
    assert.equal(input, "/ui/device");
    assert.equal(init?.method, "POST");
    assert.equal(init?.credentials, "same-origin");
    assert.deepEqual(init?.headers, { "content-type": "application/json", accept: "application/json" });
    assert.equal(init?.body, JSON.stringify({ userCode: "BCDF-GHJK" }));
    return new Response(JSON.stringify({ authorizationUrl: "https://example.com/authorize" }));
  };
  assert.equal(await approveDevice("BCDF-GHJK", request), "https://example.com/authorize");
});

test("device approval reports the refusal status", async () => {
  await assert.rejects(
    approveDevice("BCDF-GHJK", async () => new Response(null, { status: 404 })),
    (error: unknown) => error instanceof ApprovalRefused && error.status === 404,
  );
});

test("device lookup reports failed response statuses", async () => {
  for (const status of [404, 500]) {
    await assert.rejects(
      lookupDevice("BCDF-GHJK", async () => new Response(null, { status })),
      (error: unknown) => error instanceof LookupRefused && error.status === status,
    );
  }
});

test("device entry renders eight empty labelled cells and ignores a URL code", () => {
  const route = routeOf("/ui/device", "?code=BCDF-GHJK");
  assert.equal(route.kind, "device");
  if (route.kind !== "device") throw new Error("expected device route");
  const markup = render(<Device reason={route.reason} />);
  assert.match(markup, /role="group"[^>]*aria-labelledby="device-code-label"[^>]*aria-describedby="device-code-help"/);
  const inputs = markup.match(/<input\b[^>]*>/g) ?? [];
  assert.equal(inputs.length, 8);
  inputs.forEach((input, index) => {
    assert.match(input, new RegExp(`aria-label="Character ${index + 1} of 8"`));
    assert.match(input, /autocomplete="off"/);
    assert.doesNotMatch(input, /value="[^"]+"/);
  });
  assert.doesNotMatch(markup, /BCDF-GHJK/);
});

test("device entry shows known callback errors but ignores unknown reasons", () => {
  const error = render(<Device reason="denied" />);
  assert.match(error, /role="alert"/);
  assert.match(error, /The identity provider refused this sign-in\. Enter the code again to retry\./);
  assert.doesNotMatch(render(<Device reason="unknown" />), /role="alert"/);
  assert.doesNotMatch(render(<Device reason={null} />), /role="alert"/);
});

test("device confirmation explains the outcome", () => {
  const approved = render(<DeviceApproved />);
  assert.match(approved, /APPROVED/);
  assert.match(approved, /Device login approved/);
  assert.match(approved, /You can close this tab: <code>gang login<\/code> finishes by itself in your terminal\./);
});
