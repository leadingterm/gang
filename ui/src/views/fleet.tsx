// The fleet tape: one cell per instance up to the fleet's max, in a fixed state order.
import { Fragment } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import { idOf } from "../wire-parse.ts";
import type { Fleet } from "../wire.ts";
import { gib, policyDuration } from "../format.ts";
import { Disclosure } from "../ui.tsx";

export type CellState = "busy" | "idle" | "draining" | "unconnected" | "pending" | "terminating" | "warm" | "empty";

export const STATE_NAME: Record<Counted, string> = {
  busy: "busy",
  idle: "idle",
  draining: "draining",
  unconnected: "no worker",
  pending: "pending",
  terminating: "terminating",
  warm: "warm",
};

/** A state a fleet counts: every cell state but the empty slot. */
export type Counted = Exclude<CellState, "empty">;

export type Counts = readonly (readonly [Counted, number])[];

/** Busy, idle, draining, no worker, pending, terminating, then warm spares: every state a fleet counts. */
export const fleetCounts = (f: Fleet): Counts => [
  ["busy", f.workers.busy],
  ["idle", f.workers.idle],
  ["draining", f.workers.draining],
  ["unconnected", f.workers.unconnected],
  ["pending", f.instances.pending],
  ["terminating", f.instances.terminating],
  ["warm", f.instances.warm],
];

/** The states of `fleetCounts` that fill a tape cell; warm spares sit beside the tape. */
const tapeCounts = (f: Fleet): Counts => fleetCounts(f).filter(([state]) => state !== "warm");

/** The counts of several fleets added up, state by state. */
export const fleetTotals = (fleets: readonly Fleet[]): Counts => {
  const totals = new Map<Counted, number>();
  for (const [state, n] of fleets.flatMap(fleetCounts)) totals.set(state, (totals.get(state) ?? 0) + n);
  return [...totals];
};

/** The tape's cells: its states in order, then empty slots up to max. */
export const cellStates = (f: Fleet): readonly CellState[] => {
  const cells = tapeCounts(f).flatMap(([state, n]) => Array<CellState>(n).fill(state));
  return cells.length < f.policy.max ? [...cells, ...Array<CellState>(f.policy.max - cells.length).fill("empty")] : cells;
};

const CROSSFADE_MS = 700;

/** A tape cell crossfades between states by opacity. */
const Cell = ({ state }: { state: CellState }) => {
  const [leaving, setLeaving] = useState<CellState | null>(null);
  const [entering, setEntering] = useState(false);
  const prev = useRef(state);
  useEffect(() => {
    if (prev.current === state) return;
    setLeaving(prev.current);
    setEntering(true);
    prev.current = state;
    const frame = requestAnimationFrame(() => requestAnimationFrame(() => setEntering(false)));
    const done = setTimeout(() => setLeaving(null), CROSSFADE_MS);
    return () => {
      cancelAnimationFrame(frame);
      clearTimeout(done);
    };
  }, [state]);
  return (
    <span class="cell">
      {leaving && <i key={`l-${leaving}`} class={`c-${leaving} is-leaving`} />}
      <i key={`c-${state}`} class={entering ? `c-${state} is-entering` : `c-${state}`} />
    </span>
  );
};

export const StaticCell = ({ state }: { state: CellState }) => (
  <span class="cell">
    <i class={`c-${state}`} />
  </span>
);

const stripLabel = (f: Fleet): string => {
  const counts = tapeCounts(f).map(([state, n]) => `${n} ${STATE_NAME[state]}`);
  return `${idOf(f.name)}: ${counts.join(", ")}, of ${f.policy.max} maximum. Desired ${f.desired}, minimum ${f.policy.min}.`;
};

export const Tally = ({ items }: { items: Counts }) => (
  <span class="tally">
    {items
      .filter(([, n]) => n > 0)
      .map(([state, n]) => (
        <span key={state} class="tally__item">
          <StaticCell state={state} />
          <b>{n}</b>
          <span class="visually-hidden"> {STATE_NAME[state]}</span>
        </span>
      ))}
  </span>
);

const TapeKey = ({ f }: { f: Fleet }) => {
  const size = f.instanceSize ? [`${f.instanceSize.vcpus} vCPU`, gib(f.instanceSize.memory)] : [];
  const machine = [f.isa, f.sandbox, f.properties["machine_class"], ...size];
  const rows: [string, string][] = [
    ["machine", machine.filter(Boolean).join(" · ")],
    ["size", `desired ${f.desired} · min ${f.policy.min} · max ${f.policy.max}`],
    ["demand", `${f.demand.candidates} eligible · ${f.demand.needed} needed`],
    [
      "policy",
      `${f.policy.targetActions}/instance · idle ${policyDuration(f.policy.idleTimeout)} · grace ${policyDuration(f.policy.bootGrace)}`,
    ],
  ];
  return (
    <>
      <Tally items={fleetCounts(f)} />
      <dl class="kv kv--tight">
        {rows.map(([k, v]) => (
          <Fragment key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </Fragment>
        ))}
      </dl>
    </>
  );
};

/** The tape is a button: a tap or Enter opens the fleet's details, so nothing depends on hover. */
export const Tape = ({ f }: { f: Fleet }) => {
  const cells = cellStates(f);
  return (
    <Disclosure
      id={`tk-${idOf(f.name)}`}
      btnCls="tape"
      panelCls="tape__key"
      summary={
        <>
          <span class="visually-hidden">{stripLabel(f)}</span>
          <span
            class="strip"
            aria-hidden="true"
            style={{ "--max": String(cells.length), "--d": String(Math.max(1, f.desired)), "--m": String(Math.max(1, f.policy.min)) }}
          >
            <span class={f.desired === 0 ? "strip__desired is-zero" : "strip__desired"} />
            {cells.map((state, i) => (
              <Cell key={i} state={state} />
            ))}
            {f.policy.min > 0 && <span class="strip__min" />}
            <span class="strip__rule">{f.policy.max}</span>
          </span>
          {f.instances.warm > 0 && (
            <span class="warmgroup" aria-hidden="true">
              {Array.from({ length: f.instances.warm }, (_, i) => (
                <StaticCell key={i} state="warm" />
              ))}
            </span>
          )}
        </>
      }
    >
      <TapeKey f={f} />
    </Disclosure>
  );
};

export const Legend = () => (
  <div class="legend" aria-label="Legend">
    {Object.entries(STATE_NAME).map(([state, label]) => (
      <span key={state} class="legend__item">
        <StaticCell state={state as CellState} />
        {label}
      </span>
    ))}
    <span class="legend__item">
      <span class="strip__desired is-zero" />
      desired
    </span>
  </div>
);
