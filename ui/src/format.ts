// Text formatting for the wire's units (durations, unix milliseconds, bytes), enum names and sums.
import type { UnixMs } from "./wire-parse.ts";
import type { Bytes } from "./wire.ts";

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


/** `hh:mm:ss` in UTC. */
export const clock = (t: UnixMs): string => new Date(t).toISOString().slice(11, 19);

/** `d Mon yyyy` in UTC. */
export const day = (t: UnixMs): string =>
  new Date(t).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });

const GIB = 1024 ** 3;
const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
const BYTE_NUMBER = new Intl.NumberFormat("en-US", { maximumSignificantDigits: 3 });

/** Whole GiB. */
export const gib = (bytes: Bytes): string => `${Math.round(bytes / GIB)} GiB`;

/** IEC byte units with three significant digits. */
export const bytes = (n: number): string => {
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024;
    unit++;
  }
  return `${BYTE_NUMBER.format(value)} ${BYTE_UNITS[unit]}`;
};

/** Splits at the last `:`, the part that tells similar identifiers apart (a Bazel target's name, an ARN's resource, an OIDC subject's ref); no head when there is no `:` or it leads. */
export const splitTail = (text: string): readonly [head: string, tail: string] => {
  const i = text.lastIndexOf(":");
  return i <= 0 ? ["", text] : [text.slice(0, i), text.slice(i)];
};

/** The instance a worker id names, or the full id when it has no instance. */
export const workerInstance = (id: string): string => {
  const match = /^(.*)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.exec(id);
  return match?.[1] || id;
};

/** An enum name as words: `IDLE_TIMEOUT` reads `idle timeout`. */
export const humanize = (e: string): string => e.toLowerCase().replaceAll("_", " ");

/** The total of `value` over `rows`. */
export const sum = <T,>(rows: readonly T[], value: (row: T) => number): number => rows.reduce((n, row) => n + value(row), 0);

/** A ratio as a whole percentage, or a dash when it has no denominator. */
export const percent = (numerator: number, denominator: number): string =>
  denominator > 0 ? `${Math.round(numerator / denominator * 100)}%` : "—";
