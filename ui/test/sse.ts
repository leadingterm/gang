// Reads the recorded fixture streams: SSE frames, each preceded by an `: at <ms>` send-time comment.
import { readFileSync } from "node:fs";
import type { Frame, State, Timestamp } from "../src/wire.ts";

export interface RecordedFrame {
  readonly at: number;
  readonly event: Frame;
  /** The frame exactly as it goes on the wire, blank-line terminated. */
  readonly text: string;
}

const field = (lines: readonly string[], prefix: string): string => {
  const line = lines.find((l) => l.startsWith(prefix));
  if (line === undefined) throw new Error(`frame without "${prefix}"`);
  return line.slice(prefix.length);
};

export const parseRecording = (text: string): RecordedFrame[] =>
  text
    .split("\n\n")
    .filter((block) => block.trim() !== "")
    .map((block) => {
      const lines = block.split("\n");
      const event = { event: field(lines, "event: "), data: JSON.parse(field(lines, "data: ")) } as Frame;
      return { at: Number(field(lines, ": at ")), event, text: `${block}\n\n` };
    });

export const load = (name: string): RecordedFrame[] =>
  parseRecording(readFileSync(new URL(`../fixtures/${name}.sse`, import.meta.url), "utf8"));

export const snapshotOf = (frames: readonly RecordedFrame[]): State => {
  const first = frames[0]?.event;
  if (first?.event !== "snapshot") throw new Error("a recording starts with a snapshot");
  return first.data;
};

/** The wire form of a unix-millisecond time. */
export const iso = (t: number): Timestamp => new Date(t).toISOString();
