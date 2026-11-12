import assert from "node:assert/strict";
import { test } from "node:test";
import type { ComponentChildren } from "preact";
import { render } from "preact-render-to-string";
import { routeOf } from "../src/routes.ts";
import { Shell } from "../src/shell.tsx";
import { ClientContext } from "../src/ui.tsx";
import { Access } from "../src/views/access.tsx";
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
