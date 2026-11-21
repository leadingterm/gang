// The fleet tape: one cell per instance, padded to the logical max or desired count.
import { Fragment, type ComponentChildren } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import { idOf, seconds } from "../wire-parse.ts";
import type { Fleet, Instance, State, Worker } from "../wire.ts";
import { gib } from "../format.ts";
import { Disclosure, Duration, Ident } from "../ui.tsx";

export type CellState = "busy" | "idle" | "draining" | "stale" | "unconnected" | "pending" | "terminating" | "replacing" | "replacement" | "unknown" | "warm" | "empty";

export const STATE_NAME: Record<Counted, string> = {
  busy: "busy",
  idle: "idle",
  draining: "draining",
  stale: "stale",
  unconnected: "no worker",
  pending: "pending",
  terminating: "terminating",
  replacing: "replacing",
  replacement: "replacement",
  unknown: "unknown",
  warm: "warm",
};

/** A state a fleet counts: every cell state but the empty slot. */
export type Counted = Exclude<CellState, "empty">;

export type Counts = readonly (readonly [Counted, number])[];

export type FleetCensus = Pick<State, "workers" | "instances">;

/** Unhosted physical instances in this fleet, once per resource name. */
export const unhostedInstances = (f: Fleet, census: FleetCensus): Instance[] => {
  const hosted = new Set(census.workers.filter((w) => w.fleet === f.name && w.instance).map((w) => w.instance));
  const seen = new Set<string>();
  return census.instances.filter((instance) => {
    if (instance.fleet !== f.name || (instance.name && (hosted.has(instance.name) || seen.has(instance.name)))) return false;
    if (instance.name) seen.add(instance.name);
    return true;
  });
};

/** Count physical identities, not overlapping worker generations or provider lifecycles. */
export const fleetCounts = (f: Fleet, census: FleetCensus): Counts => {
  const counts: Record<Counted, number> = {
    busy: 0, idle: 0, draining: 0, stale: 0, unconnected: 0, pending: 0, terminating: 0,
    replacing: 0, replacement: 0, unknown: 0, warm: f.instances.warm,
  };
  const groups = new Map<string, Worker[]>();
  for (const worker of census.workers) {
    if (worker.fleet !== f.name) continue;
    const key = worker.instance ? `instance:${worker.instance}` : `worker:${worker.name}`;
    const group = groups.get(key);
    if (group) group.push(worker);
    else groups.set(key, [worker]);
  }
  for (const group of groups.values()) {
    const phases = group.map((w) => w.infrastructure?.phase);
    const state: Counted = phases.includes("REPLACING") ? "replacing"
      : phases.includes("REPLACEMENT") ? "replacement"
      : group.some((w) => w.stale) ? "stale"
      : group.every((w) => w.phase === "DRAINING") ? "draining"
      : group.some((w) => w.running > 0 || w.phase === "BUSY") ? "busy"
      : group.some((w) => w.phase === "IDLE") ? "idle" : "unknown";
    counts[state]++;
  }
  for (const instance of unhostedInstances(f, census)) {
    const state: Counted = instance.infrastructure?.phase === "REPLACING" ? "replacing"
      : instance.infrastructure?.phase === "REPLACEMENT" ? "replacement"
      : instance.lifecycle === "PENDING" ? "pending"
      : instance.lifecycle === "IN_SERVICE" ? "unconnected"
      : instance.lifecycle === "TERMINATING" ? "terminating" : "unknown";
    counts[state]++;
  }
  return Object.entries(counts) as [Counted, number][];
};

/** Warm spares sit beside the tape. */
const tapeCounts = (f: Fleet, census: FleetCensus): Counts => fleetCounts(f, census).filter(([state]) => state !== "warm");

/** The counts of several fleets added up, state by state. */
export const fleetTotals = (fleets: readonly Fleet[], census: FleetCensus): Counts => {
  const totals = new Map<Counted, number>();
  for (const f of fleets) for (const [state, n] of fleetCounts(f, census)) totals.set(state, (totals.get(state) ?? 0) + n);
  return [...totals];
};

/** The tape's cells: its states in order, then empty slots up to the logical max or desired count. */
export const cellStates = (f: Fleet, census: FleetCensus): readonly CellState[] => {
  const cells = tapeCounts(f, census).flatMap(([state, n]) => Array<CellState>(n).fill(state));
  const target = Math.max(f.policy.max, f.desired);
  return cells.length < target ? [...cells, ...Array<CellState>(target - cells.length).fill("empty")] : cells;
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

const stripLabel = (f: Fleet, census: FleetCensus): string => {
  const counts = tapeCounts(f, census).map(([state, n]) => `${n} ${STATE_NAME[state]}`);
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

export const TapeKey = ({ f, census }: { f: Fleet; census: FleetCensus }) => {
  const size = f.instanceSize ? [`${f.instanceSize.vcpus} vCPU`, gib(f.instanceSize.memory)] : [];
  const machine = [f.isa, f.sandbox, f.properties["machine_class"], ...size];
  const rows: [string, ComponentChildren][] = [
    ["machine", <Ident text={machine.filter(Boolean).join(" · ")} />],
    ["size", <><span class="clause">desired {f.desired}</span>{" · "}<span class="clause">min {f.policy.min}</span>{" · "}<span class="clause">max {f.policy.max}</span></>],
    ["demand", <><span class="clause">{f.demand.candidates} eligible</span>{" · "}<span class="clause">{f.demand.needed} needed</span></>],
    [
      "policy",
      <><span class="clause">idle <Duration ms={seconds(f.policy.idleTimeout) * 1000} /></span>{" · "}<span class="clause">grace <Duration ms={seconds(f.policy.bootGrace) * 1000} /></span></>,
    ],
  ];
  if (f.demand.readiness !== undefined) rows.push(["measured readiness", <Duration ms={seconds(f.demand.readiness) * 1000} />]);
  if (f.demand.expectedWait !== undefined) rows.push(["expected wait", <Duration ms={seconds(f.demand.expectedWait) * 1000} />]);
  if (f.demand.load !== undefined) rows.push(["load", `${(f.demand.load / 10).toFixed(1)} worker-equivalents`]);
  if (f.demand.backlog !== undefined) rows.push(["backlog", `${seconds(f.demand.backlog)} worker-equivalent-seconds`]);
  if (f.demand.utilization !== undefined) rows.push(["utilization", `${f.demand.utilization / 10}%`]);
  return (
    <>
      <Tally items={fleetCounts(f, census)} />
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
export const Tape = ({ f, census }: { f: Fleet; census: FleetCensus }) => {
  const cells = cellStates(f, census);
  return (
    <Disclosure
      id={`tk-${idOf(f.name)}`}
      btnCls="tape"
      panelCls="tape__key"
      summary={
        <>
          <span class="visually-hidden">{stripLabel(f, census)}</span>
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
      <TapeKey f={f} census={census} />
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
