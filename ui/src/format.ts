// Text formatting for the wire's units (durations, unix milliseconds, bytes), enum names and sums.
import { seconds, type UnixMs } from "./wire-parse.ts";
import type { Bytes, Duration } from "./wire.ts";

const pad = (n: number): string => String(n).padStart(2, "0");

/** A coarse, stable duration: `42s`, `4m 13s`, `2h 05m`, `3d 4h`. */
export const duration = (millis: number): string => {
  const s = Math.max(0, Math.floor(millis / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${pad(s % 60)}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${pad(m % 60)}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
};

/** A wire duration in whole minutes when exact: `10m`, `90s`. */
export const policyDuration = (d: Duration): string => {
  const s = Math.round(seconds(d));
  return s % 60 === 0 ? `${s / 60}m` : `${s}s`;
};

/** `hh:mm:ss` in UTC. */
export const clock = (t: UnixMs): string => new Date(t).toISOString().slice(11, 19);

/** `d Mon yyyy` in UTC. */
export const day = (t: UnixMs): string =>
  new Date(t).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });

const GIB = 1024 ** 3;

/** Whole GiB. */
export const gib = (bytes: Bytes): string => `${Math.round(bytes / GIB)} GiB`;

/** The package and the name of a Bazel label, split at the last colon. */
export const splitTarget = (target: string): readonly [pkg: string, name: string] => {
  const i = target.lastIndexOf(":");
  return i < 0 ? ["", target] : [target.slice(0, i), target.slice(i)];
};

/** A worker id is `<instance>-<uuid>`; the short form is the uuid's first four characters. */
export const workerShort = (id: string, instance: string): string =>
  (id.startsWith(`${instance}-`) ? id.slice(instance.length + 1) : id).slice(0, 4);

/** The instance a worker id names. */
export const workerInstance = (id: string): string => {
  const uuid = 36;
  return id.length > uuid + 1 ? id.slice(0, id.length - uuid - 1) : id;
};

/** An enum name as words: `IDLE_TIMEOUT` reads `idle timeout`. */
export const humanize = (e: string): string => e.toLowerCase().replaceAll("_", " ");

/** The total of `value` over `rows`. */
export const sum = <T,>(rows: readonly T[], value: (row: T) => number): number => rows.reduce((n, row) => n + value(row), 0);
