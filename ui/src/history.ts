// What the browser keeps from the frames it has seen (ADR-25: no server-side history). Each
// observation is pure: it takes the previous history and the new state at the server clock.
import { sum } from "./format.ts";
import { ms, seconds, type UnixMs } from "./wire-parse.ts";
import type { Pass, Phase, State } from "./wire.ts";

/** How far back the client keeps observations: the phase lanes and the executing spark. */
export const WINDOW_MS = 30 * 60 * 1000;
const MINUTE = 60 * 1000;

export interface Segment extends Span {
  readonly phase: Phase;
}

export interface WorkerHistory {
  /** Before this, the phase is not observed. */
  readonly observedFrom: UnixMs;
  /** Oldest first. */
  readonly segments: readonly Segment[];
}

/** A stretch of server time; `to` is `null` while it lasts. */
export interface Span {
  readonly from: UnixMs;
  readonly to: UnixMs | null;
}

export interface Sample {
  readonly at: UnixMs;
  readonly running: number;
  readonly completed: number;
}

export interface History {
  readonly observedFrom: UnixMs;
  readonly workers: ReadonlyMap<string, WorkerHistory>;
  /** Skipped control passes. */
  readonly outages: readonly Span[];
  /** Stretches the stream was down: nothing was observed then. */
  readonly gaps: readonly Span[];
  /** One sample per minute, oldest first, plus the latest frame. */
  readonly samples: readonly Sample[];
}

export const emptyHistory = (at: UnixMs): History => ({
  observedFrom: at,
  workers: new Map(),
  outages: [],
  gaps: [],
  samples: [],
});

const recent = <S extends Span>(spans: readonly S[], now: UnixMs): readonly S[] =>
  spans.filter((s) => s.to === null || s.to > now - WINDOW_MS);

const isOpen = (spans: readonly Span[]): boolean => spans.at(-1)?.to === null;

const closeLast = <S extends Span>(spans: readonly S[], at: UnixMs): readonly S[] =>
  spans.map((s, i) => (i === spans.length - 1 && s.to === null ? { ...s, to: at } : s));

// A worker's current phase began at `since`, so its history is observed from there.
const observeWorkers = (prev: History, state: State, now: UnixMs): ReadonlyMap<string, WorkerHistory> => {
  const next = new Map<string, WorkerHistory>();
  for (const w of state.workers) {
    const h = prev.workers.get(w.name);
    const since = ms(w.phaseTime);
    const current: Segment = { phase: w.phase, from: since, to: null };
    if (!h) {
      next.set(w.name, { observedFrom: since, segments: [current] });
    } else if (h.segments.at(-1)?.phase === w.phase) {
      next.set(w.name, h);
    } else {
      next.set(w.name, { observedFrom: h.observedFrom, segments: [...recent(closeLast(h.segments, since), now), current] });
    }
  }
  return next;
};

const observeOutages = (prev: readonly Span[], state: State, now: UnixMs): readonly Span[] => {
  const kept = recent(prev, now);
  if (state.pass.state === "SKIPPED" && !isOpen(kept)) {
    return [...kept, { from: state.pass.lastOkTime === undefined ? now : ms(state.pass.lastOkTime), to: null }];
  }
  if (state.pass.state === "OK" && isOpen(kept)) return closeLast(kept, now);
  return kept;
};

const observeSamples = (prev: readonly Sample[], state: State, now: UnixMs): readonly Sample[] => {
  const running = sum(state.fleets, (f) => f.demand.running);
  const sample = { at: now, running, completed: state.completed };
  // A decrease of the monotonic counter means the controller restarted: the rate starts over.
  const restarted = prev.some((s) => s.completed > state.completed);
  const kept = (restarted ? [] : prev).filter((s) => s.at >= now - WINDOW_MS);
  const settled = kept.length > 1 && now - (kept.at(-2)?.at ?? now) < MINUTE ? kept.slice(0, -1) : kept;
  return [...settled, sample];
};

/** Folds one frame, seen at the server clock `now`, into the history; a frame ends a stream gap. */
export const observe = (prev: History, state: State, now: UnixMs): History => ({
  observedFrom: prev.observedFrom,
  workers: observeWorkers(prev, state, now),
  outages: observeOutages(prev.outages, state, now),
  gaps: isOpen(prev.gaps) ? closeLast(recent(prev.gaps, now), now) : prev.gaps,
  samples: observeSamples(prev.samples, state, now),
});

/** The stream went down after the frame at `at`: nothing is observed from then until a frame. */
export const streamLost = (prev: History, at: UnixMs): History =>
  isOpen(prev.gaps) ? prev : { ...prev, gaps: [...recent(prev.gaps, at), { from: at, to: null }] };

/** Completed actions over the last minute; `null` until a minute has been observed. */
export const donePerMinute = (samples: readonly Sample[]): number | null => {
  const last = samples.at(-1);
  if (!last) return null;
  const base = samples.findLast((s) => last.at - s.at >= MINUTE);
  return base ? Math.round(((last.completed - base.completed) * MINUTE) / (last.at - base.at)) : null;
};

/** Pass bars on the Overview. */
export const PASS_BARS = 90;

export type Bar = "ok" | "skipped" | "unobserved";

const overlaps = (spans: readonly Span[], from: UnixMs, to: UnixMs, now: UnixMs): boolean =>
  spans.some((s) => s.from < to && (s.to ?? now) > from);

/**
 * One bar per pass interval, newest last. A bar is skipped when a known outage overlaps it, and
 * unobserved before the first frame or while the stream was down.
 */
export const passBars = (pass: Pass, history: History, now: UnixMs): readonly Bar[] =>
  Array.from({ length: PASS_BARS }, (_, i) => {
    const interval = seconds(pass.interval) * 1000;
    const to = now - (PASS_BARS - 1 - i) * interval;
    const from = to - interval;
    if (overlaps(history.outages, from, to, now)) return "skipped";
    return to < history.observedFrom || overlaps(history.gaps, from, to, now) ? "unobserved" : "ok";
  });
