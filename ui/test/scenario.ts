// Recorded fixture scenarios shared by server-rendered UI tests.
import { readFileSync } from "node:fs";
import { StateClient, type Environment, type Link } from "../src/client.ts";
import { emptyHistory, observe } from "../src/history.ts";
import { ms } from "../src/wire-parse.ts";
import type { ListPrincipalsResponse, ListRolesResponse, Session, State } from "../src/wire.ts";
import { load, snapshotOf } from "./sse.ts";

export const json = <T>(name: string): T => JSON.parse(readFileSync(new URL(`../fixtures/${name}.json`, import.meta.url), "utf8")) as T;
export const session = json<Session>("session");
export const principals = json<ListPrincipalsResponse>("principals");
export const roles = json<ListRolesResponse>("roles");
const { principal: _principal, expireTime: _expireTime, ...signedOut } = session;
export { signedOut };

export const offline: Environment = {
  openStream: () => {
    throw new Error("views render without a stream");
  },
  fetchJson: () => Promise.reject(new Error("views render without fetches")),
  post: () => Promise.reject(new Error("views render without posts")),
  now: () => 0,
  schedule: () => () => {},
  navigate: () => {},
  here: () => "/ui/",
};

export interface Scenario {
  readonly state?: State | null;
  readonly session?: Session;
  readonly link?: Link;
}

/** A client frozen on one frame, at that frame's clock. */
export const clientFor = ({ state = snapshotOf(load("live")), session: who = session, link = { kind: "live", beats: 0 } }: Scenario) => {
  const client = new StateClient(offline);
  client.session.value = who;
  client.link.value = link;
  client.access.value = {
    principals: principals.principals,
    nextPageToken: principals.nextPageToken,
    revoked: principals.principals.filter((p) => p.revokeTime !== undefined),
    roles: roles.roles,
  };
  if (state) {
    const at = ms(state.updateTime);
    client.state.value = state;
    client.history.value = observe(emptyHistory(at), state, at);
    client.now.value = at;
  }
  return client;
};
