// Hand-written helpers over the generated wire types of ./wire.ts.
import type { Duration, EventChange, ScalingEvent, Timestamp } from "./wire.ts";

/** Unix milliseconds: a parsed `Timestamp`, and the unit of the client's clock. */
export type UnixMs = number;

export const ms = (t: Timestamp): UnixMs => Date.parse(t);

export const seconds = (d: Duration): number => Number.parseFloat(d);

/** The ID of a resource name or reference, percent-decoded: `workers/i-0abc-1` is `i-0abc-1`. */
export const idOf = (name: string): string => decodeURIComponent(name.slice(name.indexOf("/") + 1));

/** The identity string `provider:principal` that a `principals/{principal}` name or reference holds. */
export const identityOf = idOf;

/** The provider and the principal of an identity string, split at the first colon. */
export const splitIdentity = (identity: string): readonly [provider: string, principal: string] => {
  const colon = identity.indexOf(":");
  return colon < 0 ? [identity, ""] : [identity.slice(0, colon), identity.slice(colon + 1)];
};

type KeyOf<T> = T extends unknown ? keyof T : never;

/** Every oneof member an event can carry. */
export type EventKind = KeyOf<EventChange>;

const KNOWN_KINDS: { readonly [K in EventKind]: true } = {
  scaledOut: true,
  drainStarted: true,
  drainCancelled: true,
  terminated: true,
  exitedEarly: true,
  workerRegistered: true,
  workerDeregistered: true,
};

/** The oneof members this client knows, in display order. */
export const EVENT_KINDS: readonly EventKind[] = Object.keys(KNOWN_KINDS) as EventKind[];

/** The oneof member an event carries, or `undefined` for one this client does not know. */
export const eventKind = (e: ScalingEvent): EventKind | undefined => EVENT_KINDS.find((kind) => kind in e);
