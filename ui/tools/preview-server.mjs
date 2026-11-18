// Fixture-backed preview server for the built dashboard.
import { existsSync, readFileSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize } from "node:path";
import { load } from "../test/sse.ts";

const HEARTBEAT_MS = 10000;
const DISCONNECT_AFTER_MS = 1500;

const UI_HEADERS = JSON.parse(readFileSync("headers.json", "utf8"));
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".woff2": "font/woff2",
  ".svg": "image/svg+xml",
  ".json": "application/json",
  ".map": "application/json",
};
const SCENARIOS = ["live", "stale", "lagging", "empty", "isolated", "cold", "disconnected", "forbidden", "signedout", "long-session"];
const STREAMS = { stale: "stale", lagging: "lagging", empty: "empty", isolated: "isolated", cold: "cold" };

const fixture = (name) => readFileSync(join("fixtures", name), "utf8");

const session = JSON.parse(fixture("session.json"));
const device = JSON.parse(fixture("device-authorization.json"));
const DEVICE_PATH = "/v1/deviceAuthorizations/";
const DEVICE_AGE_MS = 90_000;
const normalizeCode = (code) => code.toUpperCase().replace(/[- ]/g, "");
const sessionFor = (scenario) => {
  if (scenario === "signedout") return { name: session.name, permissions: [], login: { ...session.login, provider: "github-actions-oidc.platform-team.example.org" } };
  if (scenario === "forbidden") return { ...session, principal: "principals/github-actions%3Arepository_owner_id%3A000000000%3Arepository_id%3A0000000001%3Aref%3Arefs%2Fheads%2Fnew-hire-onboarding", permissions: [] };
  if (scenario === "long-session") return { ...session, principal: "principals/github-actions-oidc.platform-team.example.org%3Arepository_owner_id%3A000000000%3Arepository_id%3A0000000000%3Aref%3Arefs%2Fheads%2Fbuild%2Fremote-build-aarch64" };
  return session;
};

const cookies = (req) => Object.fromEntries((req.headers.cookie ?? "").split(/;\s*/).filter(Boolean).map((c) => c.split("=")));
const connects = new Map();

const send = (res, status, headers, body = "") => {
  res.writeHead(status, { "cache-control": "no-store", ...headers });
  res.end(body);
};
const json = (res, body) => send(res, 200, { "content-type": "application/json" }, JSON.stringify(body));

const stream = (req, res, scenario, replay) => {
  const n = (connects.get(scenario) ?? 0) + 1;
  connects.set(scenario, n);
  if (scenario === "signedout") return send(res, 401, {});
  if (scenario === "forbidden") return send(res, 403, {});
  if (scenario === "disconnected" && n > 1) return send(res, 503, {});
  res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store", "x-accel-buffering": "no" });
  const frames = load(STREAMS[scenario] ?? "live");
  if (replay === "instant") {
    res.write(frames.map((frame) => frame.text).join(""));
    return;
  }
  const start = frames[0].at;
  const last = frames.at(-1);
  let beats = 0;
  // After the recording, the stream keeps beating at the last seq so the preview stays live.
  const beat = () => {
    beats += 1;
    const tick = { seq: last.event.data.seq, time: new Date(last.at + beats * HEARTBEAT_MS).toISOString() };
    res.write(`event: tick\nid: ${tick.seq}\ndata: ${JSON.stringify(tick)}\n\n`);
    timers.push(setTimeout(beat, HEARTBEAT_MS));
  };
  const timers = frames.map((f) => setTimeout(() => res.write(f.text), f.at - start));
  timers.push(setTimeout(beat, last.at - start + HEARTBEAT_MS));
  if (scenario === "disconnected") timers.push(setTimeout(() => res.destroy(), DISCONNECT_AFTER_MS));
  req.on("close", () => timers.forEach(clearTimeout));
};

const file = (req, res, path) => {
  const accepts = req.headers["accept-encoding"] ?? "";
  const type = TYPES[extname(path)] ?? "application/octet-stream";
  const immutable = path.includes("/assets/");
  const headers = {
    ...UI_HEADERS,
    "content-type": type,
    "cache-control": immutable ? "public, max-age=31536000, immutable" : "no-store",
    ...(existsSync(path + ".br") || existsSync(path + ".gz") ? { vary: "accept-encoding" } : {}),
  };
  for (const [encoding, ext] of [["br", ".br"], ["gzip", ".gz"]]) {
    if (accepts.includes(encoding) && existsSync(path + ext)) {
      res.writeHead(200, { ...headers, "content-encoding": encoding });
      return res.end(readFileSync(path + ext));
    }
  }
  res.writeHead(200, headers);
  res.end(readFileSync(path));
};

const ui = (req, res, url, dist) => {
  const rel = normalize(url.pathname.slice("/ui/".length));
  if (rel.startsWith("..") || url.pathname.includes("\\")) return send(res, 404, UI_HEADERS);
  if (url.pathname === "/ui/login") return send(res, 302, { location: url.searchParams.get("next") ?? "/ui/" });
  if (extname(rel)) {
    const path = join(dist, rel);
    return existsSync(path) && statSync(path).isFile() ? file(req, res, path) : send(res, 404, UI_HEADERS);
  }
  const scenario = url.searchParams.get("scenario");
  if (SCENARIOS.includes(scenario)) {
    connects.delete(scenario);
    res.setHeader("set-cookie", `preview_scenario=${scenario}; Path=/; SameSite=Strict`);
  }
  file(req, res, join(dist, "index.html"));
};

/** Read endpoints answered from a fixture, with the permission each needs and how a query narrows it. */
const BODIES = {
  "/v1/principals": {
    permission: "principals.read",
    file: "principals.json",
    narrow: (page, query) =>
      query.get("filter") === "revokeTime:*" ? { principals: page.principals.filter((p) => p.revokeTime) } : page,
  },
  "/v1/roles": { permission: "principals.read", file: "roles.json" },
  "/v1/events": { permission: "events.read", file: "events.json" },
};

export const previewServer = ({ dist, replay = "recorded" }) => createServer((req, res) => {
  const url = new URL(req.url ?? "/", "http://preview");
  const scenario = SCENARIOS.find((s) => s === cookies(req).preview_scenario) ?? "live";
  if (req.method === "POST" && url.pathname === "/ui/logout") return send(res, 303, { location: "/ui/", "set-cookie": "preview_scenario=signedout; Path=/; SameSite=Strict" });
  if (req.method === "POST" && url.pathname === "/ui/renew") return send(res, 204, {});
  if (req.method === "POST" && url.pathname === "/ui/device") return json(res, { authorizationUrl: "/ui/device/approved" });
  if (req.method === "GET" && url.pathname.startsWith(DEVICE_PATH)) {
    let code;
    try {
      code = decodeURIComponent(url.pathname.slice(DEVICE_PATH.length));
    } catch {
      return send(res, 404, { "content-type": "application/json" }, "{}");
    }
    if (normalizeCode(code) !== normalizeCode(device.name.slice("deviceAuthorizations/".length))) {
      return send(res, 404, { "content-type": "application/json" }, "{}");
    }
    const created = Date.now() - DEVICE_AGE_MS;
    const lifetime = Date.parse(device.expireTime) - Date.parse(device.createTime);
    return json(res, { ...device, createTime: new Date(created).toISOString(), expireTime: new Date(created + lifetime).toISOString() });
  }
  if (url.pathname === "/v1/session") return json(res, sessionFor(scenario));
  if (url.pathname === "/v1/state:watch") return stream(req, res, scenario, replay);
  if (req.method === "GET" && url.pathname === "/v1/builds") {
    if (!sessionFor(scenario).permissions.includes("builds.read")) return send(res, 403, {});
    return json(res, { builds: load("live")[0].event.data.builds ?? [] });
  }
  if (req.method === "GET" && url.pathname.startsWith("/v1/builds/")) {
    if (!sessionFor(scenario).permissions.includes("builds.read")) return send(res, 403, {});
    const build = JSON.parse(fixture("build.json"));
    if (url.pathname !== `/v1/${build.name}`) return send(res, 404, { "content-type": "application/json" }, "{}");
    return json(res, build);
  }
  const body = BODIES[url.pathname];
  if (body) {
    if (!sessionFor(scenario).permissions.includes(body.permission)) return send(res, 403, {});
    const page = JSON.parse(fixture(body.file));
    return json(res, body.narrow ? body.narrow(page, url.searchParams) : page);
  }
  if (url.pathname === "/ui" || url.pathname.startsWith("/ui/")) return ui(req, res, url, dist);
  send(res, 302, { location: "/ui/" });
});
