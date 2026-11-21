// The one client of the ADR-27 state API: it opens the SSE stream, folds frames into the store, keeps the
// server clock, classifies failures through `GET /v1/session`, and exposes signals. Frame clocks
// are parsed once here; rows keep their timestamp strings for the views to parse.
import { batch, signal } from "@preact/signals";
import { applyDelta, tickContinues } from "./store.ts";
import { idOf, ms, seconds, type UnixMs } from "./wire-parse.ts";
import {
  PATHS,
  type Build,
  type Closed,
  type Delta,
  type Frame,
  type ListBuildsResponse,
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
export type AccessState =
  | { readonly kind: "unloaded" }
  | { readonly kind: "loading" }
  | { readonly kind: "error" }
  | {
      readonly kind: "loaded";
      readonly data: Access;
      readonly asOf: UnixMs;
      readonly loading: boolean;
      readonly failing: boolean;
      readonly moreLoading: boolean;
      readonly moreFailed: boolean;
    };


/** Scaling events older than the stream's, loaded on request; oldest first. */
export type Earlier =
  /** Nothing loaded yet: older events may exist. */
  | { readonly kind: "unloaded"; readonly loading: boolean }
  /** `events` are loaded; `next` is the page token of the next older page, absent at the start of the ledger. */
  | { readonly kind: "loaded"; readonly events: readonly ScalingEvent[]; readonly next: string | undefined; readonly loading: boolean };

/** Whether older events than those loaded may exist. */
export const earlierMore = (earlier: Earlier): boolean => earlier.kind === "unloaded" || earlier.next !== undefined;

/** The result of loading a build's full view. */
export type BuildDetail =
  | { readonly kind: "loading"; readonly id: string }
  /** `asOf` is the server time of the last successful load; `failing` says the latest reload failed, so `build` is as of `asOf`. */
  | { readonly kind: "loaded"; readonly id: string; readonly build: Build; readonly asOf: UnixMs; readonly failing: boolean }
  | { readonly kind: "missing"; readonly id: string }
  | { readonly kind: "error"; readonly id: string };

/** Pages of held builds loaded independently of the state stream. */
export type EarlierBuilds =
  | { readonly kind: "unloaded"; readonly loading: boolean }
  /** The first page failed: nothing is known yet, and the next load retries. */
  | { readonly kind: "error"; readonly loading: boolean }
  /** `asOf` is the server time of the first page's last successful load; `failing` says the latest refresh failed, so `builds` are as of `asOf`. */
  | {
      readonly kind: "loaded";
      readonly builds: readonly Build[];
      readonly next: string | undefined;
      readonly loading: boolean;
      readonly asOf: UnixMs;
      readonly failing: boolean;
    };

/** An HTTP response that did not succeed. */
export class HttpError extends Error {
  readonly status: number;
  constructor(status: number) {
    super(`HTTP ${status}`);
    this.status = status;
  }
}

/** How often a session with `builds.read` but no state stream reloads the builds it shows. */
export const BUILDS_REFRESH_MS = 5_000;

const BACKOFF_FIRST_MS = 1000;
const BACKOFF_MAX_MS = 30000;
/** The session renews when time left drops below max(1 minute, 20%), but never earlier than half its remaining life. */
const RENEW_LEAD_MIN_MS = 60_000;
const RENEW_LEAD_FRACTION = 0.2;
const PRINCIPALS_PAGE = 500;
const REVOKED_FILTER = "revokeTime:*";
const EARLIER_PAGE = 200;
/** Retained historical-event cap; exceeding it resets the page and token together. */
const EARLIER_LIMIT = 5000;
/** The largest page size the server grants. */
export const PAGE_MAX = 1000;
const NO_EARLIER: Earlier = { kind: "unloaded", loading: false };

const seqOf = (e: ScalingEvent): number => Number(idOf(e.name));

const withQuery = (path: string, params: Readonly<Record<string, string>>): string => {
  const query = new URLSearchParams(params).toString();
  return query === "" ? path : `${path}?${query}`;
};

/** A refused request the session must classify: no valid credential (401) or no longer permitted (403). */
const refused = (error: unknown): boolean => error instanceof HttpError && (error.status === 401 || error.status === 403);

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
  if (kept > 0 && earlier.kind === "loaded" && earlier.events.length + kept > EARLIER_LIMIT) return null;
  return kept === 0 || earlier.kind === "unloaded"
    ? earlier
    : { ...earlier, events: [...earlier.events, ...prev.slice(0, kept)] };
};

export const loginUrl = (next: string): string => `${PATHS.login}?next=${encodeURIComponent(next)}`;

export class StateClient {
  readonly state = signal<State | null>(null);
  readonly link = signal<Link>({ kind: "connecting" });
  readonly session = signal<Session | null>(null);
  readonly access = signal<AccessState>({ kind: "unloaded" });
  readonly earlier = signal<Earlier>(NO_EARLIER);
  readonly build = signal<BuildDetail | null>(null);
  readonly earlierBuilds = signal<EarlierBuilds>({ kind: "unloaded", loading: false });
  /** The server clock, advanced once a second between frames. */
  readonly now = signal<UnixMs>(0);

  #source: StreamSource | null = null;
  #visible = true;
  #running = false;
  #streamGeneration = 0;
  #offset = 0;
  /** Server clock of the last frame. */
  #lastFrame: UnixMs | null = null;
  #failures = 0;
  #cancelRetry: (() => void) | null = null;
  #cancelRenewal: (() => void) | null = null;
  /** The renewal in flight, which every renewal requested meanwhile joins; `stop` drops it. */
  #renewal: Promise<void> | null = null;
  #cancelBuildsRefresh: (() => void) | null = null;
  /** Bumped by `stop` and every admission; a session probe, failure classification or renewal of an older generation does nothing more. */
  #sessionGeneration = 0;
  /** Bumped whenever the builds refresh stops; a refresh or refusal probe of an older generation does nothing more. */
  #buildsRefreshGeneration = 0;
  #accessGeneration = 0;
  #earlierGeneration = 0;
  #buildGeneration = 0;
  #earlierBuildsGeneration = 0;
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
    this.#running = true;
    await this.#loadSession("stream");
  }

  async #loadSession(owner: "stream" | "renewal"): Promise<void> {
    const generation = this.#sessionGeneration;
    const session = await this.#probe();
    if (generation !== this.#sessionGeneration) return;
    if (!session) {
      if (this.link.value.kind !== "revoked") this.#retry(null, () => this.#loadSession(owner), owner);
      return;
    }
    this.#admit(session);
  }

  /** Suspends the stream while hidden without interrupting the session. */
  setVisible(visible: boolean): void {
    if (this.#visible === visible) return;
    this.#visible = visible;
    if (!visible) {
      this.#closeSource();
      this.#cancelRetry?.();
      this.#cancelRetry = null;
      return;
    }
    if (!this.#running || this.link.value.kind === "revoked") return;
    const session = this.session.value;
    if (session?.principal !== undefined && session.permissions.includes("state.read")) {
      this.link.value = { kind: "connecting" };
      this.#connect();
    } else if (!session && this.link.value.kind !== "signed_out" && this.link.value.kind !== "forbidden") {
      this.link.value = { kind: "connecting" };
      void this.start();
    }
  }

  stop(): void {
    this.#running = false;
    this.#closeSource();
    this.#cancelRetry?.();
    this.#cancelRenewal?.();
    this.#stopBuildsRefresh();
    this.#buildGeneration += 1;
    this.#earlierBuildsGeneration += 1;
    this.#accessGeneration += 1;
    this.#earlierGeneration += 1;
    this.#sessionGeneration += 1;
    this.#renewal = null;
  }

  /** Loads a build's full view, discarding superseded responses; a failed reload keeps the build shown, marked failing. */
  async loadBuild(id: string): Promise<void> {
    const error = await this.#loadBuild(id);
    if (this.link.value.kind === "forbidden" && refused(error)) void this.#onRefused();
  }

  /** Forgets the open build; the Build page calls it when it unmounts, so only a shown build is reloaded. */
  closeBuild(): void {
    this.#buildGeneration += 1;
    this.build.value = null;
  }

  /** As `loadBuild`; resolves to the request's error, if any, for the builds refresh to classify. */
  async #loadBuild(id: string): Promise<unknown> {
    const generation = ++this.#buildGeneration;
    const shown = this.build.value;
    if (shown?.kind !== "loaded" || shown.id !== id) this.build.value = { kind: "loading", id };
    try {
      const build = await this.#get<Build>(PATHS.getBuild.replace("{build}", encodeURIComponent(id)), {});
      if (generation === this.#buildGeneration) this.build.value = { kind: "loaded", id, build, asOf: this.serverNow(), failing: false };
      return undefined;
    } catch (error) {
      if (generation !== this.#buildGeneration) return undefined;
      const latest = this.build.value;
      this.build.value = error instanceof HttpError && error.status === 404
        ? { kind: "missing", id }
        : latest?.kind === "loaded" ? { ...latest, failing: true } : { kind: "error", id };
      return error;
    }
  }

  /** Loads the next page of held builds; it supersedes a refresh in flight, and a refresh never supersedes it. */
  async loadEarlierBuilds(): Promise<void> {
    const error = await this.#loadEarlierBuilds();
    if (this.link.value.kind === "forbidden" && refused(error)) void this.#onRefused();
  }

  /** As `loadEarlierBuilds`; resolves to the current request's error, if any, for its caller to classify. */
  async #loadEarlierBuilds(): Promise<unknown> {
    const current = this.earlierBuilds.value;
    if (current.loading || (current.kind === "loaded" && current.next === undefined)) return undefined;
    const generation = ++this.#earlierBuildsGeneration;
    this.earlierBuilds.value = { ...current, loading: true };
    const params = current.kind === "loaded" && current.next !== undefined
      ? { pageSize: String(EARLIER_PAGE), pageToken: current.next }
      : { pageSize: String(EARLIER_PAGE) };
    try {
      const page = await this.#get<ListBuildsResponse>(PATHS.listBuilds, params);
      if (generation !== this.#earlierBuildsGeneration) return undefined;
      const latest = this.earlierBuilds.value;
      const builds = latest.kind === "loaded" ? latest.builds : [];
      const names = new Set(builds.map((build) => build.name));
      const live = new Map((this.state.value?.builds ?? []).map((build) => [build.name, build]));
      const added = page.builds.filter((build) => !names.has(build.name)).map((build) => live.get(build.name) ?? build);
      this.earlierBuilds.value = latest.kind === "loaded"
        ? { ...latest, builds: [...builds, ...added], next: page.nextPageToken, loading: false }
        : { kind: "loaded", builds: added, next: page.nextPageToken, loading: false, asOf: this.serverNow(), failing: false };
      return undefined;
    } catch (error) {
      if (generation !== this.#earlierBuildsGeneration) return undefined;
      const latest = this.earlierBuilds.value;
      this.earlierBuilds.value = latest.kind === "loaded" ? { ...latest, loading: false } : { kind: "error", loading: false };
      return error;
    }
  }

  /**
   * Every visit revalidates principals, revoked principals and roles, including their sightings.
   * The epoch and generation guard against publishing a superseded response.
   */
  async loadAccess(): Promise<void> {
    const previous = this.access.value;
    const epoch = this.state.value?.principalsEpoch ?? null;
    const generation = ++this.#accessGeneration;
    this.access.value = previous.kind === "loaded"
      ? { ...previous, loading: true, moreLoading: false, moreFailed: false }
      : { kind: "loading" };
    const pageSize = String(PRINCIPALS_PAGE);
    try {
      const [page, revoked, roles] = await Promise.all([
        this.#get<ListPrincipalsResponse>(PATHS.listPrincipals, { pageSize }),
        this.#walk<ListPrincipalsResponse, Principal>(PATHS.listPrincipals, { pageSize, filter: REVOKED_FILTER }, (p) => p.principals),
        this.#walk<ListRolesResponse, Role>(PATHS.listRoles, {}, (r) => r.roles),
      ]);
      if (generation !== this.#accessGeneration) return;
      if ((this.state.value?.principalsEpoch ?? null) !== epoch) {
        this.access.value = previous.kind === "loaded"
          ? { ...previous, loading: false, failing: true, moreLoading: false, moreFailed: false }
          : { kind: "unloaded" };
        return;
      }
      this.access.value = {
        kind: "loaded",
        data: { principals: page.principals, nextPageToken: page.nextPageToken, revoked, roles },
        asOf: this.serverNow(), loading: false, failing: false, moreLoading: false, moreFailed: false,
      };
    } catch (error) {
      if (generation !== this.#accessGeneration) return;
      const moved = (this.state.value?.principalsEpoch ?? null) !== epoch;
      this.access.value = previous.kind === "loaded"
        ? { ...previous, loading: false, failing: true, moreLoading: false, moreFailed: false }
        : { kind: moved ? "unloaded" : "error" };
      if (this.link.value.kind === "forbidden" && refused(error)) void this.#onRefused();
    }
  }

  async loadMorePrincipals(): Promise<void> {
    const current = this.access.value;
    if (current.kind !== "loaded" || current.loading || current.moreLoading || current.data.nextPageToken === undefined) return;
    const generation = this.#accessGeneration;
    const token = current.data.nextPageToken;
    const data = current.data;
    this.access.value = { ...current, moreLoading: true, moreFailed: false };
    try {
      const page = await this.#get<ListPrincipalsResponse>(PATHS.listPrincipals, {
        pageSize: String(PRINCIPALS_PAGE),
        pageToken: token,
      });
      const latest = this.access.value;
      if (generation !== this.#accessGeneration || latest.kind !== "loaded" || latest.data !== data) return;
      this.access.value = {
        ...latest,
        data: { ...data, principals: [...data.principals, ...page.principals], nextPageToken: page.nextPageToken },
        moreLoading: false,
        moreFailed: false,
      };
    } catch (error) {
      const latest = this.access.value;
      if (generation !== this.#accessGeneration || latest.kind !== "loaded" || latest.data !== data) return;
      this.access.value = { ...latest, moreLoading: false, moreFailed: true };
      if (this.link.value.kind === "forbidden" && refused(error)) void this.#onRefused();
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
      if (events.length + older.length > EARLIER_LIMIT) {
        this.#earlierGeneration += 1;
        this.earlier.value = NO_EARLIER;
        return;
      }
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

  #scheduleBuildsRefresh(): void {
    this.#cancelBuildsRefresh?.();
    this.#cancelBuildsRefresh = this.#env.schedule(() => {
      this.#cancelBuildsRefresh = null;
      void this.#refreshBuilds();
    }, BUILDS_REFRESH_MS);
  }

  #stopBuildsRefresh(): void {
    this.#cancelBuildsRefresh?.();
    this.#cancelBuildsRefresh = null;
    this.#buildsRefreshGeneration += 1;
  }

  /** Reloads the loaded held builds and the open build together, then refreshes again or hands a refusal to the session. */
  async #refreshBuilds(): Promise<void> {
    const generation = this.#buildsRefreshGeneration;
    const open = this.build.value;
    const errors = await Promise.all([
      this.#refreshEarlierBuilds(),
      open?.kind === "loaded" ? this.#loadBuild(open.id) : undefined,
    ]);
    if (generation !== this.#buildsRefreshGeneration) return;
    if (errors.some(refused)) void this.#onRefused();
    else this.#scheduleBuildsRefresh();
  }

  /** Replaces the loaded held builds with as many of the newest, at most `PAGE_MAX`; skipped while a page is loading. */
  async #refreshEarlierBuilds(): Promise<unknown> {
    const current = this.earlierBuilds.value;
    if (current.kind !== "loaded" || current.loading) return undefined;
    const generation = ++this.#earlierBuildsGeneration;
    try {
      const page = await this.#get<ListBuildsResponse>(PATHS.listBuilds, {
        pageSize: String(Math.min(PAGE_MAX, Math.max(EARLIER_PAGE, current.builds.length))),
      });
      if (generation === this.#earlierBuildsGeneration) {
        this.earlierBuilds.value = { kind: "loaded", builds: page.builds, next: page.nextPageToken, loading: false, asOf: this.serverNow(), failing: false };
      }
      return undefined;
    } catch (error) {
      if (generation !== this.#earlierBuildsGeneration) return undefined;
      const latest = this.earlierBuilds.value;
      if (latest.kind === "loaded") this.earlierBuilds.value = { ...latest, failing: true };
      return error;
    }
  }

  /** A build or access request was refused (401 or 403): the session decides, as `#onFailure` does for the stream. */
  async #onRefused(): Promise<void> {
    this.#stopBuildsRefresh();
    const generation = this.#buildsRefreshGeneration;
    let session: Session;
    try {
      session = (await this.#env.fetchJson(PATHS.getSession)) as Session;
    } catch (error) {
      if (generation !== this.#buildsRefreshGeneration) return;
      // `GET /v1/session` refuses only a revoked principal (403) or a malformed credential (docs/API.md).
      if (error instanceof HttpError && error.status === 403) {
        this.#stopBuildsRefresh();
        this.#streamDown({ kind: "revoked" });
        return;
      }
      if (this.link.value.kind === "forbidden" && (this.session.value?.permissions.includes("builds.read") ?? false)) this.#scheduleBuildsRefresh();
      return;
    }
    if (generation !== this.#buildsRefreshGeneration) return;
    if (session.principal === undefined && session.login) {
      void this.#renew();
      return;
    }
    this.#admit(session);
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

  /**
   * The one owner of what a session may see. Every admission passes here (`start`, hence renewal and
   * retries; `#onFailure`; `#onRefused`): it stops the previous admission's work, drops what the session
   * may not read, reloads the open build, an errored first page of held builds and errored or failing access,
   * and starts the stream or the builds refresh.
   */
  #admit(session: Session): void {
    this.#sessionGeneration += 1;
    this.#stopBuildsRefresh();
    this.#cancelRetry?.();
    this.#cancelRetry = null;
    this.#cancelRenewal?.();
    this.#cancelRenewal = null;
    this.#renewal = null;
    if (session.principal !== undefined && session.login && session.expireTime !== undefined) {
      const left = ms(session.expireTime) - this.serverNow();
      const lead = Math.min(left / 2, Math.max(RENEW_LEAD_MIN_MS, RENEW_LEAD_FRACTION * left));
      const delay = Math.max(0, left - lead);
      this.#cancelRenewal = this.#env.schedule(() => void this.#renew(), delay);
    }
    const signedIn = session.principal !== undefined;
    const streamed = signedIn && session.permissions.includes("state.read");
    const builds = signedIn && session.permissions.includes("builds.read");
    const access = signedIn && session.permissions.includes("principals.read");
    batch(() => {
      this.session.value = session;
      if (!streamed) {
        this.#closeSource();
        if (this.state.value !== null) this.#resetEarlierBuilds();
        this.state.value = null;
        this.#earlierGeneration += 1;
        this.earlier.value = NO_EARLIER;
      }
      if (!builds) {
        this.closeBuild();
        this.#resetEarlierBuilds();
      }
      if (!access) {
        this.#accessGeneration += 1;
        this.access.value = { kind: "unloaded" };
      }
      if (!signedIn) this.link.value = { kind: "signed_out" };
      else if (!streamed) this.link.value = { kind: "forbidden" };
      else if (this.link.value.kind !== "live" && this.link.value.kind !== "lost") {
        this.#resetEarlierBuilds();
        this.link.value = { kind: "connecting" };
      }
    });
    const open = this.build.value;
    if (open?.kind === "loaded" || open?.kind === "error") void this.#loadBuild(open.id);
    if (this.earlierBuilds.value.kind === "error") void this.#loadEarlierBuilds();
    const shown = this.access.value;
    if (access && (shown.kind === "error" || (shown.kind === "loaded" && shown.failing))) void this.loadAccess();
    if (streamed) this.#connect();
    else if (builds) this.#scheduleBuildsRefresh();
  }

  #connect(): void {
    this.#closeSource();
    if (!this.#visible || !this.#running) return;
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

  #resetEarlierBuilds(): void {
    this.#earlierBuildsGeneration += 1;
    this.earlierBuilds.value = { kind: "unloaded", loading: false };
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
    const held = this.state.value?.events ?? [];
    batch(() => {
      this.#resetEarlierBuilds();
      this.#continueEarlier(held, state.events);
      this.state.value = state;
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
      if (delta.builds?.remove?.length) {
        this.#resetEarlierBuilds();
      } else if (delta.builds?.upsert?.length) {
        const earlier = this.earlierBuilds.value;
        if (earlier.kind === "loaded") {
          const changes = new Map(delta.builds.upsert.map((build) => [build.name, build]));
          this.earlierBuilds.value = { ...earlier, builds: earlier.builds.map((build) => changes.get(build.name) ?? build) };
        }
      }
      if (state) this.#continueEarlier(state.events, next.events);
      this.state.value = next;
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
    const generation = this.#sessionGeneration;
    const streamGeneration = this.#streamGeneration;
    const session = await this.#probe(streamGeneration);
    if (generation !== this.#sessionGeneration || streamGeneration !== this.#streamGeneration || this.link.value.kind === "revoked") return;
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

  /** Renews the session; a renewal requested while one is in flight joins it. */
  #renew(): Promise<void> {
    if (this.#renewal) return this.#renewal;
    const renewal = this.#renewOnce().finally(() => {
      if (this.#renewal === renewal) this.#renewal = null;
    });
    this.#renewal = renewal;
    return renewal;
  }

  async #renewOnce(): Promise<void> {
    const generation = this.#sessionGeneration;
    let status: number | null;
    try {
      status = await this.#env.post(PATHS.renew);
    } catch {
      status = null;
    }
    if (generation !== this.#sessionGeneration) return;
    if (status === 204) {
      await this.#loadSession("renewal");
    } else if (status === 401 || status === 403) {
      this.#freeze(this.serverNow());
      this.#env.navigate(loginUrl(this.#env.here()));
    } else {
      this.#retry(null, () => this.#renew(), "renewal");
    }
  }

  async #probe(streamGeneration?: number): Promise<Session | null> {
    const generation = this.#sessionGeneration;
    try {
      return (await this.#env.fetchJson(PATHS.getSession)) as Session;
    } catch (error) {
      if (generation === this.#sessionGeneration && (streamGeneration === undefined || streamGeneration === this.#streamGeneration) && error instanceof HttpError && error.status === 403) {
        this.stop();
        this.link.value = { kind: "revoked" };
      }
      return null;
    }
  }

  /** Runs `attempt` again after `retryMs`, or after the next backoff step when it is null. */
  #retry(retryMs: number | null, attempt: () => Promise<void> = () => this.start(), owner: "stream" | "renewal" = "stream"): void {
    const delay = retryMs ?? Math.min(BACKOFF_MAX_MS, BACKOFF_FIRST_MS * 2 ** this.#failures);
    this.#failures += 1;
    this.#freeze(this.serverNow() + delay);
    if (owner === "renewal") {
      this.#cancelRenewal?.();
      this.#cancelRenewal = this.#env.schedule(() => {
        this.#cancelRenewal = null;
        void attempt();
      }, delay);
    } else {
      this.#cancelRetry?.();
      this.#cancelRetry = this.#visible ? this.#env.schedule(() => {
        this.#cancelRetry = null;
        void attempt();
      }, delay) : null;
    }
  }

  /** Freezes streamed rows without replacing a build-only or revoked link. */
  #freeze(retryAt: UnixMs): void {
    if (!this.#visible || this.link.value.kind === "forbidden" || this.link.value.kind === "revoked") return;
    this.#streamDown({ kind: "lost", frozenAt: this.#lastFrame, retryAt });
  }

  /** The stream ended; the last state remains frozen until a new frame arrives. */
  #streamDown(link: Link): void {
    this.link.value = link;
  }

  #closeSource(): void {
    this.#streamGeneration += 1;
    this.#source?.close();
    this.#source = null;
  }
}

/** The browser environment. */
export const browserEnvironment = (): Environment => ({
  openStream: (path) => new EventSource(path),
  fetchJson: async (path) => {
    const response = await fetch(path, { credentials: "same-origin", headers: { accept: "application/json" } });
    if (!response.ok) throw new HttpError(response.status);
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
