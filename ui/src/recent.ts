// The server's recent window is the only source of charts and rates; nothing depends on how long
// the page has been open.
import { sum } from "./format.ts";
import { ms, seconds } from "./wire-parse.ts";
import type { Recent, Sample, State } from "./wire.ts";

export const stepMs = (r: Recent): number => seconds(r.step) * 1000;
export const windowMs = (r: Recent): number => seconds(r.window) * 1000;
export const steps = (r: Recent): number => Math.max(1, Math.round(windowMs(r) / stepMs(r)));

/** Applies the server's replacement suffix and trims samples outside its window. */
export const applyRecent = (r: Recent, inc: Recent | undefined): Recent => {
  if (!inc) return r;
  const added = inc.samples ?? [];
  const first = added[0];
  const samples = first
    ? [...(r.samples ?? []).filter((s) => ms(s.start) < ms(first.start)), ...added]
    : [...(r.samples ?? [])];
  const last = samples.at(-1);
  const kept = last ? samples.filter((s) => ms(last.start) - ms(s.start) < windowMs(inc)) : samples;
  return { window: inc.window, step: inc.step, ...(kept.length ? { samples: kept } : {}) };
};

/** One slot per step of the window ending at the last sample, oldest first; null where unrecorded. */
export const slots = (r: Recent): readonly (Sample | null)[] => {
  const samples = (r.samples ?? []).slice(-steps(r));
  return [...Array<null>(steps(r) - samples.length).fill(null), ...samples];
};
/** Whether scheduler measurements in this step are available, including legacy samples. */
export const schedulerKnown = (s: Sample): boolean => s.schedulerKnown ?? (s.passes > 0);

/** Current scheduler availability, with a latest-sample fallback for older servers. */
export const schedulerCurrent = (state: State): boolean => {
  const latest = state.recent.samples?.at(-1);
  return state.schedulerKnown ?? (latest !== undefined && schedulerKnown(latest));
};


export interface DoneRate { readonly perMinute: number; readonly spanMs: number }

/** Completions per minute over the last minute of complete steps; null before one complete step. */
export const donePerMinute = (r: Recent): DoneRate | null => {
  const complete = (r.samples ?? []).slice(0, -1).slice(-Math.max(1, Math.floor(60_000 / stepMs(r))));
  if (!complete.length || complete.some((s) => !schedulerKnown(s))) return null;
  const spanMs = complete.length * stepMs(r);
  return { perMinute: sum(complete, (s) => s.completed) * 60_000 / spanMs, spanMs };
};

/** Completions per minute per complete slot; null where unrecorded or in progress. */
export const doneSeries = (r: Recent): readonly (number | null)[] => {
  const all = slots(r);
  return all.map((s, i) => s && schedulerKnown(s) && i < all.length - 1 ? s.completed * 60_000 / stepMs(r) : null);
};

/** Executing actions at the latest pass in each recorded slot. */
export const runningSeries = (r: Recent): readonly (number | null)[] =>
  slots(r).map((s) => s && schedulerKnown(s) ? s.running : null);

/** Queued actions at the latest pass in each recorded slot. */
export const queuedSeries = (r: Recent): readonly (number | null)[] =>
  slots(r).map((s) => s && schedulerKnown(s) ? s.queued : null);

export interface CacheWindow {
  readonly lookups: number;
  readonly hits: number;
  readonly rate: number | null;
  readonly lookupsPerStep: readonly (number | null)[];
  readonly hitsPerStep: readonly (number | null)[];
}

/** Cache lookup and hit counts and their weighted rate across the recent window. */
export const cacheWindow = (r: Recent): CacheWindow => {
  const all = slots(r);
  const lookupsPerStep: (number | null)[] = [];
  const hitsPerStep: (number | null)[] = [];
  let lookups = 0;
  let hits = 0;
  for (const s of all) {
    lookupsPerStep.push(s === null ? null : s.lookups);
    hitsPerStep.push(s === null ? null : s.hits);
    if (s !== null) {
      lookups += s.lookups;
      hits += s.hits;
    }
  }
  return { lookups, hits, rate: lookups > 0 ? hits / lookups : null, lookupsPerStep, hitsPerStep };
};

/** Total recorded traffic, or null when the window has no samples. */
export const transferred = (r: Recent): { uploaded: number; downloaded: number } | null => {
  const samples = slots(r).filter((s): s is Sample => s !== null);
  if (!samples.length) return null;
  return {
    uploaded: sum(samples, (s) => s.uploadedBytes),
    downloaded: sum(samples, (s) => s.downloadedBytes),
  };
};

/** Traffic per recorded step; missing steps remain null. */
export const bytesSeries = (r: Recent, direction: "uploadedBytes" | "downloadedBytes"): readonly (number | null)[] =>
  slots(r).map((s) => s ? s[direction] : null);

/** Evicted bytes per recorded step. */
export const evictedSeries = (r: Recent): readonly (number | null)[] =>
  slots(r).map((s) => s && schedulerKnown(s) ? s.evictedBytes : null);

export type Bar = "ok" | "skipped" | "idle" | "unrecorded";

/** Pass outcome per slot, with skipped passes taking precedence. */
export const passBars = (r: Recent): readonly Bar[] =>
  slots(r).map((s) => !s ? "unrecorded" : s.skipped > 0 ? "skipped" : s.passes > 0 ? "ok" : "idle");

/** How many leading slots are unrecorded. */
export const unrecordedSlots = (r: Recent): number => Math.max(0, steps(r) - (r.samples ?? []).length);
