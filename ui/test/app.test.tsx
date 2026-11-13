import assert from "node:assert/strict";
import { test } from "node:test";
import type { ComponentChildren } from "preact";
import { render } from "preact-render-to-string";
import { routeOf } from "../src/routes.ts";
import { Shell } from "../src/shell.tsx";
import { ClientContext } from "../src/ui.tsx";
import { Access } from "../src/views/access.tsx";
import { Device, DeviceApproved, DeviceRequest, lookupDevice } from "../src/views/device.tsx";
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
    { name: "device approval takes precedence", pathname: "/ui/device", search: "?error=unknown", scenario: { link: { kind: "signed_out" } }, absentSession: true, expected: <Device error="unknown" /> },
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

test("a fetched device request shows its address and posts the entered code", async () => {
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
  assert.match(markup, /Requested 1m 00s ago/);
  assert.match(markup, /<form method="post" action="\/ui\/device">/);
  assert.match(markup, /name="userCode" value="ABCD\/EFGH"/);
  assert.match(markup, /Sign in with example to approve/);
});

test("device lookup rejects failed responses", async () => {
  await assert.rejects(lookupDevice("ABCD-EFGH", async () => new Response(null, { status: 404 })));
});

test("device error and confirmation pages explain their outcomes", () => {
  const error = render(<Device error="unknown" />);
  assert.match(error, /That code is unknown, expired or already approved\./);
  assert.match(error, /placeholder="XXXX-XXXX"/);
  const approved = render(<DeviceApproved />);
  assert.match(approved, /Device login approved\./);
  assert.match(approved, /You can close this tab; <code>gang login<\/code> finishes by itself\./);
});
