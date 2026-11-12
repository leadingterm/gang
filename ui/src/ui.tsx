// Shared view primitives: live durations, tweened numbers, event glyphs, section frames and filters.
import { createContext, type ComponentChildren } from "preact";
import { useContext, useEffect, useRef, useState } from "preact/hooks";
import { useComputed } from "@preact/signals";
import type { StateClient } from "./client.ts";
import { duration, splitTarget, workerInstance, workerShort } from "./format.ts";
import { eventKind, idOf, type EventKind, type UnixMs } from "./wire-parse.ts";
import type { EventChange, Fleet, ScalingEvent } from "./wire.ts";

export const ClientContext = createContext<StateClient | null>(null);

export const useClient = (): StateClient => {
  const client = useContext(ClientContext);
  if (!client) throw new Error("the view renders inside a ClientContext");
  return client;
};

const reducedMotion = (): boolean =>
  typeof matchMedia === "function" && matchMedia("(prefers-reduced-motion: reduce)").matches;

/** Time since `t` on the server clock; only the text node updates each second. */
export const Since = ({ t, cls = "" }: { t: UnixMs; cls?: string }) => {
  const client = useClient();
  const text = useComputed(() => duration(client.now.value - t));
  return <span class={`mono ${cls}`.trim()}>{text}</span>;
};

/** Time left until `t` on the server clock. */
export const Until = ({ t }: { t: UnixMs }) => {
  const client = useClient();
  const text = useComputed(() => duration(t - client.now.value));
  return <span>{text}</span>;
};

const TWEEN_MS = 640;

/** A count that tweens to its new value; reduced motion jumps straight to it. */
export const Num = ({ value, cls = "" }: { value: number; cls?: string }) => {
  const ref = useRef<HTMLSpanElement>(null);
  const shown = useRef(value);
  useEffect(() => {
    const node = ref.current;
    const from = shown.current;
    shown.current = value;
    if (!node || from === value) return;
    if (reducedMotion()) {
      node.textContent = String(value);
      return;
    }
    const t0 = performance.now();
    let frame = 0;
    const step = (t: number): void => {
      const p = Math.min(1, (t - t0) / TWEEN_MS);
      node.textContent = String(Math.round(from + (value - from) * (1 - (1 - p) ** 4)));
      if (p < 1) frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [value]);
  return (
    <span ref={ref} class={`num live ${cls}`.trim()}>
      {shown.current}
    </span>
  );
};

/** Adds `row-enter` for the first frame of a new row, so it settles into place. */
export const useEnter = <T extends HTMLElement>() => {
  const ref = useRef<T>(null);
  useEffect(() => {
    const node = ref.current;
    if (!node || reducedMotion()) return;
    node.classList.add("row-enter");
    const done = (): void => node.classList.remove("row-enter");
    node.addEventListener("animationend", done, { once: true });
  }, []);
  return ref;
};

export const Target = ({ label }: { label: string }) => {
  const [pkg, name] = splitTarget(label);
  return (
    <span class="target">
      <span class="target__pkg">{pkg}</span>
      {name}
    </span>
  );
};

export const Priority = ({ p }: { p: number }) =>
  p > 0 ? (
    <span class="pri">
      {" "}
      <span class="visually-hidden">priority </span>↑{p}
    </span>
  ) : null;

/** The payload of the oneof member `K`. */
type ChangeOf<K extends EventKind> = EventChange extends infer C ? (C extends Record<K, infer P> ? P : never) : never;

/** The short worker id that an event names, from a `workers/{worker}` reference. */
const workerLabel = (worker: string): string => {
  const id = idOf(worker);
  return workerShort(id, workerInstance(id));
};

const workerGlyph = <circle class="g-worker" cx="7" cy="7" r="3" />;

/** How each kind of event reads; a kind missing here fails the typecheck. */
export const EVENT_VIEW = {
  scaledOut: {
    glyph: <path class="g-out" d="M7 2.5 12.5 11.5h-11z" />,
    label: "Scale-out",
    what: (c) => (
      <span class="mono">
        {c.from} → {c.to}
      </span>
    ),
  },
  drainStarted: {
    glyph: <path class="g-drain" d="M7 2v9M3 7.5 7 11.5l4-4" />,
    label: "Drain",
    what: (c) => <span class="mono">{workerInstance(idOf(c.worker))}</span>,
  },
  drainCancelled: {
    glyph: <path class="g-cancel" d="M11.5 11.5v-5H3M6 3.5l-3 3 3 3" />,
    label: "Cancel drain",
    what: (c) => <span class="mono">{workerInstance(idOf(c.worker))}</span>,
  },
  terminated: {
    glyph: <path class="g-term" d="M3 3l8 8M11 3l-8 8" />,
    label: "Terminate",
    what: (c) => <span class="mono">{idOf(c.instance)}</span>,
  },
  exitedEarly: {
    glyph: <path class="g-term" d="M3 3l8 8M11 3l-8 8" />,
    label: "Early exit",
    what: (c) => <span class="mono">{idOf(c.instance)}: {c.failure}</span>,
  },
  workerRegistered: {
    glyph: workerGlyph,
    label: "Join",
    what: (c) => <span class="mono">{workerLabel(c.worker)} joined</span>,
  },
  workerDeregistered: {
    glyph: workerGlyph,
    label: "Leave",
    what: (c) => <span class="mono">{workerLabel(c.worker)} left</span>,
  },
} satisfies { [K in EventKind]: { glyph: preact.JSX.Element; label: string; what: (change: ChangeOf<K>) => preact.JSX.Element } };

/** The glyph of an event's oneof member; an event of an unknown member has none. */
export const Glyph = ({ kind, cls }: { kind: EventKind | undefined; cls: string }) => (
  <svg class={cls} viewBox="0 0 14 14" aria-hidden="true">
    {kind && EVENT_VIEW[kind].glyph}
  </svg>
);

/** What an event changed; an event of an unknown member has no detail. */
export const EventWhat = ({ e }: { e: ScalingEvent }) => {
  const kind = eventKind(e);
  return kind === undefined ? null : EVENT_VIEW[kind].what(Reflect.get(e, kind));
};

const BRAND_ON = [true, true, false, true, true, true, true, false, false];

export const Brand = ({ href }: { href: string }) => (
  <a class="brand" href={href} aria-label="Gang overview">
    <svg viewBox="0 0 20 20" aria-hidden="true">
      {BRAND_ON.map((on, i) => (
        <rect
          key={i}
          class={on ? "brand__cell brand__cell--on" : "brand__cell"}
          x={1 + (i % 3) * 6.5}
          y={1 + Math.floor(i / 3) * 6.5}
          width="5"
          height="5"
          rx="1"
        />
      ))}
    </svg>
    gang
  </a>
);

export const Empty = ({ children }: { children: ComponentChildren }) => (
  <div class="empty">
    <svg class="empty__mark" viewBox="0 0 40 40" aria-hidden="true">
      {Array.from({ length: 9 }, (_, i) => (
        <rect key={i} class="brand__cell" x={2 + (i % 3) * 13} y={2 + Math.floor(i / 3) * 13} width="10" height="10" rx="2" />
      ))}
    </svg>
    {children}
  </div>
);

export const Tags = ({ items }: { items: readonly string[] }) => (
  <span class="chips">
    {items.map((t) => (
      <span key={t} class="tag">
        {t}
      </span>
    ))}
  </span>
);

/** The server clock floored to `stepMs`, so a view that shows it re-renders once per step. */
export const useNowStep = (stepMs: number): UnixMs => {
  const client = useClient();
  return useComputed(() => Math.floor(client.now.value / stepMs) * stepMs).value;
};

/** The document title: `<label> · Gang`. */
export const useTitle = (label: string): void => {
  useEffect(() => {
    document.title = `${label} · Gang`;
  }, [label]);
};

const FRAME = { section: "section", panel: "panel", plain: undefined } as const;

/** A titled block: `aside` sits at the end of the head, after the optional `count`. */
export const Section = ({
  id,
  title,
  variant = "section",
  count,
  aside,
  asideCls,
  children,
}: {
  id: string;
  title: string;
  variant?: keyof typeof FRAME;
  count?: ComponentChildren;
  aside?: ComponentChildren;
  asideCls?: string;
  children?: ComponentChildren;
}) => (
  <section class={FRAME[variant]} aria-labelledby={id}>
    <div class="section__head">
      <h2 id={id}>{title}</h2>
      {count !== undefined && <span class="section__count">{count}</span>}
      {aside !== undefined && <span class={asideCls === undefined ? "section__aside" : `section__aside ${asideCls}`}>{aside}</span>}
    </div>
    {children}
  </section>
);

/** A fleet picker; `count` adds each fleet's size to its option and disables an empty one. */
export const FleetFilter = ({
  id,
  fleets,
  value,
  onChange,
  count,
}: {
  id: string;
  fleets: readonly Fleet[];
  value: string;
  onChange: (fleet: string) => void;
  count?: (f: Fleet) => number;
}) => (
  <div class="field">
    <label class="visually-hidden" for={id}>
      Fleet
    </label>
    <select class="select" id={id} value={value} onChange={(e) => onChange(e.currentTarget.value)}>
      <option value="">All fleets</option>
      {fleets.map((f) => {
        const n = count?.(f);
        return (
          <option key={f.name} value={f.name} disabled={n === 0}>
            {n === undefined ? idOf(f.name) : `${idOf(f.name)} (${n})`}
          </option>
        );
      })}
    </select>
  </div>
);

/** A button that shows or hides its panel; the panel's content exists only while it is open. */
export const Disclosure = ({
  id,
  btnCls,
  panelCls,
  summary,
  children,
}: {
  id: string;
  btnCls: string;
  panelCls: string;
  summary: ComponentChildren;
  children: ComponentChildren;
}) => {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button class={btnCls} type="button" aria-expanded={open} aria-controls={id} onClick={() => setOpen(!open)}>
        {summary}
      </button>
      <div class={panelCls} id={id} hidden={!open}>
        {open && children}
      </div>
    </>
  );
};
