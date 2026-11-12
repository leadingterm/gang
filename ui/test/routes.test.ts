import assert from "node:assert/strict";
import { test } from "node:test";
import { routeOf } from "../src/routes.ts";

test("the root and the bare /ui path both route to the overview", () => {
  assert.deepEqual(routeOf("/ui/", ""), { kind: "screen", screen: "overview" });
  assert.deepEqual(routeOf("/ui", ""), { kind: "screen", screen: "overview" });
  assert.deepEqual(routeOf("/ui//", ""), { kind: "screen", screen: "overview" });
});

test("a screen path routes to its screen, with or without a trailing slash", () => {
  assert.deepEqual(routeOf("/ui/workers/", ""), { kind: "screen", screen: "workers" });
  assert.deepEqual(routeOf("/ui/workers", ""), { kind: "screen", screen: "workers" });
  assert.deepEqual(routeOf("/ui/access", ""), { kind: "screen", screen: "access" });
});

test("an unknown path falls back to the overview", () => {
  assert.deepEqual(routeOf("/ui/nope", ""), { kind: "screen", screen: "overview" });
});

test("the error page carries the reason from its query", () => {
  assert.deepEqual(routeOf("/ui/error", "?reason=denied"), { kind: "error", reason: "denied" });
});

test("the error page without a query has no reason", () => {
  assert.deepEqual(routeOf("/ui/error", ""), { kind: "error", reason: null });
});

test("the error page with a trailing slash is still the error page", () => {
  assert.equal(routeOf("/ui/error/", "?reason=denied").kind, "error");
  assert.deepEqual(routeOf("/ui/error/", ""), { kind: "error", reason: null });
});
