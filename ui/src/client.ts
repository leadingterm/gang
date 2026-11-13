// The one client of the ADR-27 state API: it opens the SSE stream, folds frames into the store, keeps the
// server clock, classifies failures through `GET /v1/session`, and exposes signals. Frame clocks
// are parsed once here; rows keep their timestamp strings for the views to parse.
import { batch, signal } from "@preact/signals";
import { applyDelta, tickContinues } from "./store.ts";
import { emptyHistory, observe, streamLost, type History } from "./history.ts";
import { idOf, ms, seconds, type UnixMs } from "./wire-parse.ts";
import {
  PATHS,
  type Closed,
  type Delta,
  type Frame,
  type ListEventsResponse,
  type ListPrincipalsResponse,
  type ListRolesResponse,
  type Principal,
  type Role,
  type ScalingEvent,
  type Session,
  type State,
  type Tick,
} from "./wire.ts";

/** The part of `EventSource` the client uses. */
export interface StreamSource {
  addEventListener(type: string, listener: (ev: MessageEvent<string>) => void): void;
  onerror: ((ev: Event) => void) | null;
  close(): void;
}

/** Everything the client touches outside itself, so tests run it deterministically. */
export interface Environment {
  openStream(path: string): StreamSource;
  fetchJson(path: string): Promise<unknown>;
  /** POSTs to `path` with no body and same-origin credentials; resolves to the HTTP status, rejects on a network failure. */
  post(path: string): Promise<number>;
  /** The local clock, in unix milliseconds. */
  now(): UnixMs;
  schedule(run: () => void, delayMs: number): () => void;
  navigate(url: string): void;
  /** The current `/ui` location, for the post-login redirect. */
  here(): string;
}

/** The payload of the SSE event named `E`. */
export type FrameData<E extends Frame["event"]> = Extract<Frame, { event: E }>["data"];

export type Link =
  | { readonly kind: "connecting" }
  | { readonly kind: "live"; readonly beats: number }
  /** The rows are frozen at `frozenAt` (server clock); the stream reopens at `retryAt`. */
  | { readonly kind: "lost"; readonly frozenAt: UnixMs | null; readonly retryAt: UnixMs }
  | { readonly kind: "revoked" }
  | { readonly kind: "forbidden" }
  | { readonly kind: "signed_out" };

export interface Access {
  /** The first pages of every principal, by identity. */
  readonly principals: readonly Principal[];
  /** The page token of the principals still to load. */
  readonly nextPageToken: string | undefined;
  /** Every revoked principal, however many pages. */
  readonly revoked: readonly Principal[];
  readonly roles: readonly Role[];
}

/** Scaling events older than the stream's, loaded on request; oldest first. */
export type Earlier =
  /** Nothing loaded yet: older events may exist. */
  | { readonly kind: "unloaded"; readonly loading: boolean }
  /** `events` are loaded; `next` is the page token of the next older page, absent at the start of the ledger. */
  | { readonly kind: "loaded"; readonly events: readonly ScalingEvent[]; readonly next: string | undefined; readonly loading: boolean };

/** Whether older events than those loaded may exist. */
export const earlierMore = (earlier: Earlier): boolean => earlier.kind === "unloaded" || earlier.next !== undefined;

const BACKOFF_FIRST_MS = 1000;
const BACKOFF_MAX_MS = 30000;
/** The session renews when time left drops below max(1 minute, 20%), but never earlier than half its remaining life. */
const RENEW_LEAD_MIN_MS = 60_000;
const RENEW_LEAD_FRACTION = 0.2;
const PRINCIPALS_PAGE = 500;
const REVOKED_FILTER = "revokeTime:*";
const EARLIER_PAGE = 200;
/** The largest page size the server grants. */
const PAGE_MAX = 1000;
const NO_EARLIER: Earlier = { kind: "unloaded", loading: false };

const seqOf = (e: ScalingEvent): number => Number(idOf(e.name));

const withQuery = (path: string, params: Readonly<Record<string, string>>): string => {
  const query = new URLSearchParams(params).toString();
  return query === "" ? path : `${path}?${query}`;
};

/**
 * Keeps a loaded page contiguous with the streamed events: events the stream stopped holding move
 * onto the page, and a stream whose oldest event the client never held drops the page (a gap).
 */
const continueEarlier = (
  earlier: Earlier,
  prev: readonly ScalingEvent[],
  next: readonly ScalingEvent[],
): Earlier | null => {
  if (prev === next || prev.length === 0) return earlier;
  const first = next[0];
  const kept = first ? prev.findIndex((e) => e.name === first.name) : -1;
  if (kept < 0) return null;
  return kept === 0 || earlier.kind === "unloaded"
    ? earlier
    : { ...earlier, events: [...earlier.events, ...prev.slice(0, kept)] };
};

export const loginUrl = (next: string): string => `${PATHS.login}?next=${encodeURIComponent(next)}`;

export class StateClient {
  readonly state = signal<State | null>(null);
  readonly history = signal<History | null>(null);
  readonly link = signal<Link>({ kind: "connecting" });
  readonly session = signal<Session | null>(null);
  readonly access = signal<Access | null>(null);
  readonly earlier = signal<Earlier>(NO_EARLIER);
  /** The server clock, advanced once a second between frames. */
  readonly now = signal<UnixMs>(0);

  #source: StreamSource | null = null;
  #offset = 0;
  /** Server clock of the last frame. */
  #lastFrame: UnixMs | null = null;
  #failures = 0;
  #cancelRetry: (() => void) | null = null;
  #cancelRenewal: (() => void) | null = null;
  #accessEpoch: number | null = null;
  #earlierGeneration = 0;
  /** A `loadMorePrincipals` request is in flight; a second call would repeat its page token. */
  #morePending = false;
  readonly #env: Environment;

  constructor(env: Environment) {
    this.#env = env;
    this.now.value = env.now();
  }

  /** The server clock now. */
  serverNow(): UnixMs {
    return this.#env.now() + this.#offset;
  }

  /** Advances `now`; the UI calls it once a second. */
  tick(): void {
    this.now.value = this.serverNow();
  }

  async start(): Promise<void> {
    const session = await this.#probe();
    if (!session) return this.#retry(null);
    this.#admit(session);
  }

  stop(): void {
    this.#closeSource();
    this.#cancelRetry?.();
    this.#cancelRenewal?.();
  }

  /**
   * Loads principals, revoked principals and roles for the current `principalsEpoch`. The epoch is
   * recorded only once its answer arrives, so a failed load is retried by the next call and an
   * answer for an epoch that is no longer current is dropped.
   */
  async loadAccess(): Promise<void> {
    const epoch = this.state.value?.principalsEpoch ?? null;
    if (epoch === this.#accessEpoch && this.access.value) return;
    const pageSize = String(PRINCIPALS_PAGE);
    try {
      const [page, revoked, roles] = await Promise.all([
        this.#get<ListPrincipalsResponse>(PATHS.listPrincipals, { pageSize }),
        this.#walk<ListPrincipalsResponse, Principal>(PATHS.listPrincipals, { pageSize, filter: REVOKED_FILTER }, (p) => p.principals),
        this.#walk<ListRolesResponse, Role>(PATHS.listRoles, {}, (r) => r.roles),
      ]);
      if ((this.state.value?.principalsEpoch ?? null) !== epoch) return;
      this.#accessEpoch = epoch;
      this.access.value = { principals: page.principals, nextPageToken: page.nextPageToken, revoked, roles };
    } catch {
      // The previous lists stay; the next epoch or visit retries.
    }
  }

  async loadMorePrincipals(): Promise<void> {
    const current = this.access.value;
    if (this.#morePending || current?.nextPageToken === undefined) return;
    this.#morePending = true;
    try {
      const page = await this.#get<ListPrincipalsResponse>(PATHS.listPrincipals, {
        pageSize: String(PRINCIPALS_PAGE),
        pageToken: current.nextPageToken,
      });
      if (this.access.value !== current) return;
      this.access.value = {
        ...current,
        principals: [...current.principals, ...page.principals],
        nextPageToken: page.nextPageToken,
      };
    } catch {
      // The list stays; the next click retries.
    } finally {
      this.#morePending = false;
    }
  }

  /**
   * Loads the scaling events older than the oldest one the client holds. The first request asks
   * for everything held plus one more page, because the ledger lists newest first; its token then
   * walks back one page at a time.
   */
  async loadEarlier(): Promise<void> {
    const current = this.earlier.value;
    if (current.loading || !earlierMore(current)) return;
    const held = this.state.value?.events ?? [];
    this.earlier.value = { ...current, loading: true };
    const generation = this.#earlierGeneration;
    const params =
      current.kind === "loaded" && current.next !== undefined
        ? { pageSize: String(EARLIER_PAGE), pageToken: current.next }
        : { pageSize: String(Math.min(PAGE_MAX, held.length + EARLIER_PAGE)) };
    try {
      const page = await this.#get<ListEventsResponse>(PATHS.listEvents, params);
      if (generation !== this.#earlierGeneration) return;
      const latest = this.earlier.value;
      const events = latest.kind === "loaded" ? latest.events : [];
      const oldest = events[0] ?? this.state.value?.events[0];
      const older = page.events.filter((e) => oldest === undefined || seqOf(e) < seqOf(oldest));
      this.earlier.value = {
        kind: "loaded",
        events: [...older.reverse(), ...events],
        loading: false,
        next: page.nextPageToken,
      };
    } catch {
      if (generation === this.#earlierGeneration) {
        this.earlier.value = { ...this.earlier.value, loading: false };
      }
    }
  }

  #get<T>(path: string, params: Readonly<Record<string, string>>): Promise<T> {
    return this.#env.fetchJson(withQuery(path, params)) as Promise<T>;
  }

  /** Every item of a collection, following `nextPageToken` to the last page. */
  async #walk<T extends { readonly nextPageToken?: string }, I>(
    path: string,
    params: Readonly<Record<string, string>>,
    items: (page: T) => readonly I[],
  ): Promise<readonly I[]> {
    const rows: I[] = [];
    let pageToken: string | undefined;
    do {
      const page = await this.#get<T>(path, pageToken === undefined ? params : { ...params, pageToken });
      rows.push(...items(page));
      pageToken = page.nextPageToken;
    } while (pageToken !== undefined);
    return rows;
  }

  #admit(session: Session): void {
    this.session.value = session;
    this.#cancelRenewal?.();
    this.#cancelRenewal = null;
    if (session.principal !== undefined && session.login && session.expireTime !== undefined) {
      const left = ms(session.expireTime) - this.serverNow();
      const lead = Math.min(left / 2, Math.max(RENEW_LEAD_MIN_MS, RENEW_LEAD_FRACTION * left));
      const delay = Math.max(0, left - lead);
      this.#cancelRenewal = this.#env.schedule(() => void this.#renew(), delay);
    }
    if (session.principal === undefined) {
      this.link.value = { kind: "signed_out" };
      return;
    }
    if (!session.permissions.includes("state.read")) {
      this.link.value = { kind: "forbidden" };
      return;
    }
    this.#connect();
  }

  #connect(): void {
    this.#closeSource();
    const source = this.#env.openStream(PATHS.watchState);
    this.#source = source;
    const handlers = {
      snapshot: (s) => this.#onSnapshot(s),
      delta: (d) => this.#onDelta(d),
      tick: (t) => this.#onTick(t),
      closed: (c) => this.#onClosed(c),
    } satisfies { [E in Frame["event"]]: (data: FrameData<E>) => void };
    for (const [event, handle] of Object.entries(handlers)) {
      source.addEventListener(event, (ev) => {
        if (this.#source !== source) return;
        let data;
        try {
          data = JSON.parse(ev.data);
        } catch {
          // A frame that is not JSON means the stream is broken: reopen it, as for a foreign base.
          this.#connect();
          return;
        }
        handle(data);
      });
    }
    source.onerror = () => {
      if (this.#source === source) void this.#onFailure();
    };
  }

  /** Every frame carries the server clock; the offset and the freeze time follow the latest. */
  #clockFrom(at: UnixMs): void {
    this.#offset = at - this.#env.now();
    this.#lastFrame = at;
  }

  /** Keeps earlier pages contiguous with the stream, invalidating in-flight pages across gaps. */
  #continueEarlier(prev: readonly ScalingEvent[], next: readonly ScalingEvent[]): void {
    const earlier = continueEarlier(this.earlier.value, prev, next);
    if (earlier === null) {
      this.#earlierGeneration += 1;
      this.earlier.value = NO_EARLIER;
    } else {
      this.earlier.value = earlier;
    }
  }

  #onSnapshot(state: State): void {
    const at = ms(state.updateTime);
    this.#clockFrom(at);
    this.#failures = 0;
    const prev = this.history.value;
    const held = this.state.value?.events ?? [];
    batch(() => {
      this.#continueEarlier(held, state.events);
      this.state.value = state;
      this.history.value = observe(prev ?? emptyHistory(at), state, at);
      this.link.value = { kind: "live", beats: 0 };
      this.now.value = at;
    });
  }

  #onDelta(delta: Delta): void {
    const state = this.state.value;
    const applied = state ? applyDelta(state, delta) : ({ kind: "resync" } as const);
    if (applied.kind === "resync") return this.#connect();
    const at = ms(delta.updateTime);
    this.#clockFrom(at);
    const next = applied.state;
    batch(() => {
      if (state) this.#continueEarlier(state.events, next.events);
      this.state.value = next;
      this.history.value = observe(this.history.value ?? emptyHistory(at), next, at);
      this.now.value = at;
    });
  }

  #onTick(tick: Tick): void {
    const state = this.state.value;
    if (!state || !tickContinues(state, tick)) return this.#connect();
    const at = ms(tick.time);
    this.#clockFrom(at);
    const link = this.link.value;
    batch(() => {
      this.now.value = at;
      if (link.kind === "live") this.link.value = { kind: "live", beats: link.beats + 1 };
    });
  }

  #onClosed(closed: Closed): void {
    this.#closeSource();
    switch (closed.reason) {
      case "REVOKED":
        this.#streamDown({ kind: "revoked" });
        return;
      case "EXPIRED":
        this.#freeze(this.serverNow());
        void this.#renew();
        return;
      case "VIEWER_CAP":
      case "SHUTDOWN":
      default:
        this.#retry(closed.retryDelay === undefined ? null : seconds(closed.retryDelay) * 1000);
    }
  }

  /** `EventSource` hides the HTTP status, so a failed stream is classified by the session. */
  async #onFailure(): Promise<void> {
    this.#closeSource();
    const session = await this.#probe();
    if (!session || (session.principal !== undefined && session.permissions.includes("state.read"))) {
      return this.#retry(null);
    }
    if (session.principal === undefined && session.login && this.state.value) {
      this.#freeze(this.serverNow());
      void this.#renew();
      return;
    }
    this.#admit(session);
  }

  async #renew(): Promise<void> {
    let status: number | null;
    try {
      status = await this.#env.post(PATHS.renew);
    } catch {
      status = null;
    }
    if (status === 204) {
      await this.start();
    } else if (status === 401 || status === 403) {
      this.#freeze(this.serverNow());
      this.#env.navigate(loginUrl(this.#env.here()));
    } else {
      this.#retry(null, () => this.#renew());
    }
  }

  async #probe(): Promise<Session | null> {
    try {
      return (await this.#env.fetchJson(PATHS.getSession)) as Session;
    } catch {
      return null;
    }
  }

  /** Runs `attempt` again after `retryMs`, or after the next backoff step when it is null. */
  #retry(retryMs: number | null, attempt: () => Promise<void> = () => this.start()): void {
    const delay = retryMs ?? Math.min(BACKOFF_MAX_MS, BACKOFF_FIRST_MS * 2 ** this.#failures);
    this.#failures += 1;
    this.#freeze(this.serverNow() + delay);
    this.#cancelRetry?.();
    this.#cancelRetry = this.#env.schedule(() => {
      this.#cancelRetry = null;
      void attempt();
    }, delay);
  }

  #freeze(retryAt: UnixMs): void {
    this.#streamDown({ kind: "lost", frozenAt: this.#lastFrame, retryAt });
  }

  /** The stream ended: nothing is observed after the last frame until a new one arrives. */
  #streamDown(link: Link): void {
    const history = this.history.value;
    batch(() => {
      if (history && this.#lastFrame !== null) this.history.value = streamLost(history, this.#lastFrame);
      this.link.value = link;
    });
  }

  #closeSource(): void {
    this.#source?.close();
    this.#source = null;
  }
}

/** The browser environment. */
export const browserEnvironment = (): Environment => ({
  openStream: (path) => new EventSource(path),
  fetchJson: async (path) => {
    const response = await fetch(path, { credentials: "same-origin", headers: { accept: "application/json" } });
    if (!response.ok) throw new Error(`${path}: ${response.status}`);
    return response.json();
  },
  post: async (path) => (await fetch(path, { method: "POST", credentials: "same-origin" })).status,
  now: () => Date.now(),
  schedule: (run, delayMs) => {
    const id = setTimeout(run, delayMs);
    return () => clearTimeout(id);
  },
  navigate: (url) => location.assign(url),
  here: () => `${location.pathname}${location.search}`,
});
