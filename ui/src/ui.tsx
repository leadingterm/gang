// Shared view primitives: live durations, tweened numbers, event glyphs, section frames and filters.
import { createContext, toChildArray, type ComponentChildren } from "preact";
import { useContext, useEffect, useRef, useState } from "preact/hooks";
import { useComputed } from "@preact/signals";
import type { StateClient } from "./client.ts";
import { clock, day, duration, splitTail, workerInstance } from "./format.ts";
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

/** A column: `share` takes the free width for a main identifier and never drops below 10rem, so capped columns cannot starve it; `fit` shows an identifier whole up to its rem cap; neither fits a one-line value. */
export type Column = { readonly head?: ComponentChildren; readonly share?: number; readonly fit?: number; readonly end?: boolean };

/** Rows share column tracks so their cells cannot widen the table; explicit roles keep the table semantics that grid layout drops. */
export const Table = ({ cols, label, cls, children }: { cols: readonly Column[]; label?: string; cls?: string; children: ComponentChildren }) => (
  <table role="table" class={cls ? `table ${cls}` : "table"} aria-label={label} style={{ "--cols": cols.map((c) => c.share !== undefined ? `minmax(min(10rem, 100%), ${c.share}fr)` : c.fit !== undefined ? `fit-content(${c.fit}rem)` : "max-content").join(" ") }}>
    {cols.some((c) => c.head !== undefined) && <thead role="rowgroup"><tr role="row">{cols.map((c, i) => <th key={i} role="columnheader" scope="col" class={c.end ? "r" : undefined}>{c.head}</th>)}</tr></thead>}
    <tbody role="rowgroup">{children}</tbody>
  </table>
);

const iso = (t: UnixMs): string => new Date(t).toISOString();

/** A moment as a UTC day. */
export const Day = ({ t }: { t: UnixMs }) => <time dateTime={iso(t)}>{day(t)}</time>;

/** A moment as a UTC clock time. */
export const Clock = ({ t }: { t: UnixMs }) => <time dateTime={iso(t)}>{clock(t)}</time>;

/** A length of time. */
export const Duration = ({ ms }: { ms: number }) => <time>{duration(ms)}</time>;

/** Time since `t` on the server clock; only the text node updates each second. */
export const Since = ({ t, cls = "" }: { t: UnixMs; cls?: string }) => {
  const client = useClient();
  const text = useComputed(() => duration(client.now.value - t));
  return <time class={`mono ${cls}`.trim()}>{text}</time>;
};

/** Time left until `t` on the server clock. */
export const Until = ({ t }: { t: UnixMs }) => {
  const client = useClient();
  const text = useComputed(() => duration(t - client.now.value));
  return <time>{text}</time>;
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

/** A table row; `enter` settles a newly added row into place. Rows keep their role because the table is laid out as a grid. */
export const Row = ({ cls, enter = false, children }: { cls?: string | undefined; enter?: boolean; children: ComponentChildren }) => {
  const ref = useEnter<HTMLTableRowElement>();
  return <tr role="row" class={cls} ref={enter ? ref : null}>{children}</tr>;
};
/** A table cell. */
export const Cell = ({ cls, children }: { cls?: string | undefined; children?: ComponentChildren }) => <td role="cell" class={cls}>{children}</td>;

/** A single-line identifier with its full value on hover; when narrow it gives way in its head and keeps its tail (`splitTail`). */
export const Ident = ({ text, title = text, cls }: { text: string; title?: string; cls?: string }) => {
  const [head, tail] = splitTail(text);
  const classes = ["ident", head && "ident--split", cls].filter(Boolean).join(" ");
  return head ? (
    <span class={classes} title={title}><span class="ident__head">{head}</span><span class="ident__tail">{tail}</span></span>
  ) : <span class={classes} title={title}>{text}</span>;
};
/** A Bazel label: an identifier whose package gives way first. */
export const Target = ({ label }: { label: string }) => <Ident text={label} cls="target" />;

export const Priority = ({ p }: { p: number }) =>
  p > 0 ? (
    <span class="pri">
      {" "}
      <span class="visually-hidden">priority </span>↑{p}
    </span>
  ) : null;

/** The payload of the oneof member `K`. */
type ChangeOf<K extends EventKind> = EventChange extends infer C ? (C extends Record<K, infer P> ? P : never) : never;

/** The instance named by a `workers/{worker}` reference, or its full id. */
const workerLabel = (worker: string): string => workerInstance(idOf(worker));

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
  launchFailed: {
    glyph: <path class="caution" fill="currentColor" d="M7 1 13 12H1L7 1zm-.7 4v3.5h1.4V5H6.3zm0 4.5V11h1.4V9.5H6.3z" fill-rule="evenodd" />,
    label: "launch failed",
    what: (c) => <span class="mono">{c.from} of {c.to} launched</span>,
  },
  withdrawn: {
    glyph: <path class="g-drain" d="M7 2v9M3 7.5 7 11.5l4-4" />,
    label: "withdrawn",
    what: (c) => <span class="mono">{c.from} → {c.to}</span>,
  },
  drainStarted: {
    glyph: <path class="g-drain" d="M7 2v9M3 7.5 7 11.5l4-4" />,
    label: "Drain",
    what: (c) => <span class="line mono"><Ident text={workerLabel(c.worker)} /></span>,
  },
  drainCancelled: {
    glyph: <path class="g-cancel" d="M11.5 11.5v-5H3M6 3.5l-3 3 3 3" />,
    label: "Cancel drain",
    what: (c) => <span class="line mono"><Ident text={workerLabel(c.worker)} /></span>,
  },
  terminated: {
    glyph: <path class="g-term" d="M3 3l8 8M11 3l-8 8" />,
    label: "Terminate",
    what: (c) => <span class="line mono"><Ident text={idOf(c.instance)} /></span>,
  },
  exitedEarly: {
    glyph: <path class="g-term" d="M3 3l8 8M11 3l-8 8" />,
    label: "Early exit",
    what: (c) => <span class="line mono"><Ident text={idOf(c.instance)} />{" "}<span class="line__prose">{c.failure}</span></span>,
  },
  workerRegistered: {
    glyph: workerGlyph,
    label: "Join",
    what: (c) => <span class="line mono"><Ident text={workerLabel(c.worker)} />{" "}<span>joined</span></span>,
  },
  workerDeregistered: {
    glyph: workerGlyph,
    label: "Leave",
    what: (c) => <span class="line mono"><Ident text={workerLabel(c.worker)} />{" "}<span>left</span></span>,
  },
  aws: {
    glyph: <path class="g-out" d="M7 1 13 12H1z" />,
    label: "AWS",
    what: ({ event }) => {
      const k = event.kind;
      switch (k.type) {
        case "rebalanceRecommendation":
          return <span>AWS: spot rebalance recommendation for {idOf(k.instance)}</span>;
        case "spotInterruption":
          return <span>AWS: spot {k.action === "HIBERNATE" ? "hibernation" : "interruption"} notice for {idOf(k.instance)}{k.action === "STOP" ? " (stop; 2 min warning)" : k.action === "TERMINATE" ? " (2 min warning)" : ""}</span>;
        case "instanceLaunched":
          return <span>AWS: {idOf(k.instance)} launched</span>;
        case "replacementLaunched":
          return <span>{k.replacing ? `AWS: spot rebalance: replacing ${idOf(k.replacing)} with ${idOf(k.instance)}` : `AWS: replacement ${idOf(k.instance)} launched`}</span>;
        case "instanceTerminated":
          return <span>AWS: {idOf(k.instance)} terminated: {k.reason === "OTHER" ? (k.reasonDetail ?? "other") : k.reason.toLowerCase().replaceAll("_", " ")}</span>;
        case "capacityUnavailable":
          return <span>AWS: launch warning: {k.reason}</span>;
        default:
          return <span>AWS: infrastructure event</span>;
      }
    },
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
      <span key={t} class="tag" title={t}>
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

/** The shared, single-line header for a section or group. */
export const SectionHead = ({ id, title, count, aside, mono = false }: {
  id: string;
  title: string;
  count?: ComponentChildren;
  aside?: ComponentChildren;
  mono?: boolean;
}) => (
  <div class="shead">
    <h2 id={id} class={mono ? "shead__title shead__title--mono" : "shead__title"} title={mono ? undefined : title}>{mono ? <Ident text={title} /> : title}</h2>
    {count !== undefined && <span class="shead__count">{count}</span>}
    <span class="shead__aside">{aside}</span>
  </div>
);


/** A page-wide named grid shared by sections and column groups. */
export const PageGrid = ({ children }: { children: ComponentChildren }) => <div class="page-grid">{children}</div>;

/** Equal columns that inherit the page tracks. */
export const Columns = ({ children }: { children: ComponentChildren }) => <div class="layout-columns">{children}</div>;

/** A vertically spaced group of content. */
export const Stack = ({ children, as = "div", gap = "xl" }: {
  children: ComponentChildren;
  as?: "div" | "aside";
  gap?: "2xs" | "xs" | "sm" | "md" | "lg" | "xl" | "2xl";
}) => {
  const style = { "--stack-gap": `var(--space-${gap})` };
  return as === "aside" ? <aside class="stack" style={style}>{children}</aside> : <div class="stack" style={style}>{children}</div>;
};

/** A group of aligned readouts. */
export const Tiles = ({ children, columns = 5 }: { children: ComponentChildren; columns?: number }) => {
  const items = toChildArray(children);
  const lastSpan = items.length ? columns - ((items.length - 1) % columns) : 1;
  return <div class="readouts" style={{ "--tile-columns": columns, "--tile-last-span": lastSpan }}>{items}</div>;
};

/** A readout reserves the same five slots as its siblings. */
export const Tile = ({ label, value, meter, caption, chart, heading = 2, valueStyle = "metric" }: {
  label: string;
  value: ComponentChildren;
  meter?: ComponentChildren;
  caption?: ComponentChildren;
  chart?: ComponentChildren;
  heading?: 2 | 3;
  valueStyle?: "metric" | "text";
}) => (
  <div class="readout">
    {heading === 2 ? <h2 class="label">{label}</h2> : <h3 class="label">{label}</h3>}
    <div class={valueStyle === "text" ? "readout__big readout__big--text" : "readout__big"}>{value}</div>
    <div class="readout__meter" aria-hidden={meter == null ? "true" : undefined}>{meter}</div>
    <div class="readout__sub">{caption}</div>
    <div class="readout__chart" aria-hidden={chart == null ? "true" : undefined}>{chart}</div>
  </div>
);

/** A chart with a shared title, legend and plot geometry. */
export const ChartFrame = ({ children, title, legend, size = "spark", cls }: {
  children: ComponentChildren;
  title?: string;
  legend?: ComponentChildren;
  size?: "spark" | "timeline" | "bar" | "cache";
  cls?: string;
}) => (
  <div class={["chart-frame", `chart-frame--${size}`, title !== undefined && "chart-frame--titled", cls].filter(Boolean).join(" ")}>
    {title !== undefined && <><h3 class="label chart-frame__title">{title}</h3><div class="chart-frame__legend">{legend}</div></>}
    <div class="chart-frame__plot">{children}</div>
  </div>
);

/** A titled block: `aside` sits at the end of the head, after the optional `count`. */
export const Section = ({
  id,
  title,
  variant = "section",
  cls,
  count,
  aside,
  legend,
  hidden,
  mono = false,
  children,
}: {
  id: string;
  title: string;
  variant?: keyof typeof FRAME;
  cls?: string;
  count?: ComponentChildren;
  aside?: ComponentChildren;
  legend?: ComponentChildren;
  hidden?: boolean;
  mono?: boolean;
  children?: ComponentChildren;
}) => (
  <section class={["section-frame", FRAME[variant], cls].filter(Boolean).join(" ")} aria-labelledby={id} hidden={hidden}>
    <SectionHead id={id} title={title} count={count} aside={aside} mono={mono} />
    <div class="section__legend">{legend}</div>
    <div class="section__body">{children}</div>
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
