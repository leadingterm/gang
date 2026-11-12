// Workers: one row per worker grouped by fleet, with its phase, slots and the phase lane the
// browser has observed; instances without a worker sit under their fleet as counts.
import { useState } from "preact/hooks";
import { idOf, ms, type UnixMs } from "../wire-parse.ts";
import type { Fleet, Instance, Phase, State, Worker } from "../wire.ts";
import { WINDOW_MS, type WorkerHistory } from "../history.ts";
import { clock, duration, sum, workerShort } from "../format.ts";
import { Disclosure, Empty, FleetFilter, Since, Target, useClient, useNowStep } from "../ui.tsx";
import { AsOf } from "./actions.tsx";
import { fleetCounts, fleetTotals, STATE_NAME, StaticCell, Tally, type Counted } from "./fleet.tsx";

/** The instances without a worker are listed under their fleet. */
const GHOSTED: Record<Counted, boolean> = { busy: false, idle: false, draining: false, unconnected: true, pending: true, terminating: true, warm: false };

/** The toolbar tallies workers by phase, and the instances that have none yet. */
const TALLIED: Record<Counted, boolean> = { busy: true, idle: true, draining: true, unconnected: true, pending: true, terminating: false, warm: false };

/** Lanes move at this resolution, so rows do not re-render every second. */
const LANE_STEP_MS = 10 * 1000;

type LaneSegment = { readonly phase: Phase | "off"; readonly from: UnixMs; readonly to: UnixMs };

export const laneSegments = (w: Worker, h: WorkerHistory | undefined, now: UnixMs): readonly LaneSegment[] => {
  const start = now - WINDOW_MS;
  const launched = ms(w.launchTime);
  const off = launched > start ? [{ phase: "off" as const, from: start, to: launched }] : [];
  const seen = (h?.segments ?? []).map((s) => ({ phase: s.phase, from: s.from, to: s.to ?? now }));
  return [...off, ...seen].filter((s) => s.to > start);
};

const Lane = ({ w, h, now }: { w: Worker; h: WorkerHistory | undefined; now: UnixMs }) => {
  if (!h) return <div class="lane" aria-hidden="true" />;
  const start = now - WINDOW_MS;
  return (
    <div class="lane" role="img" aria-label={`Phase history: ${h.segments.map((s) => s.phase.toLowerCase()).join(", ")}`}>
      {laneSegments(w, h, now).map((s) => (
        <i
          key={`${s.phase}-${s.from}`}
          class={`lane__seg lane__seg--${s.phase.toLowerCase()}`}
          style={{
            "--x": String(Math.max(0, (s.from - start) / WINDOW_MS).toFixed(4)),
            "--w": String(Math.max(0, (Math.min(now, s.to) - Math.max(start, s.from)) / WINDOW_MS).toFixed(4)),
          }}
        />
      ))}
    </div>
  );
};

const Pips = ({ w }: { w: Worker }) => (
  <span class="slots">
    <span class={w.phase === "DRAINING" ? "slots__pips is-draining" : "slots__pips"} aria-hidden="true">
      {Array.from({ length: w.slots ?? 0 }, (_, i) => (
        <i key={i} class={i < w.running ? "on" : undefined} />
      ))}
    </span>
    <span class="visually-hidden">
      {w.running} of {w.slots ?? "unknown"} slots
    </span>
  </span>
);

const WorkerDetail = ({ w, h, state }: { w: Worker; h: WorkerHistory | undefined; state: State }) => {
  const client = useClient();
  const acts = state.actions?.running.filter((a) => a.worker === w.name) ?? [];
  const segments = (h?.segments ?? []).toReversed();
  const launched = ms(w.launchTime);
  return (
    <>
      <dl class="kv kv--tight">
        <dt>worker</dt>
        <dd class="mono">{idOf(w.name)}</dd>
        <dt>up</dt>
        <dd class="mono">
          <Since t={launched} /> · since {clock(launched)}
        </dd>
        <dt>slots</dt>
        <dd class="mono">
          {w.running}/{w.slots ?? "?"}
        </dd>
      </dl>
      {segments.length > 0 && (
        <ol class="phases" aria-label="Phase history">
          {segments.map((s) => (
            <li key={s.from} class="phases__row">
              <span class={`phase phase--${s.phase.toLowerCase()}`}>
                <span class="phase__name">{s.phase.toLowerCase()}</span>
              </span>
              <span class="mono">
                {clock(s.from)} → {s.to === null ? "now" : clock(s.to)}
              </span>
              <span class="mono quiet">{duration((s.to ?? client.now.value) - s.from)}</span>
            </li>
          ))}
        </ol>
      )}
      {acts.length > 0 && state.actions && (
        <>
          <table class="table" aria-label="Running on this worker">
            <tbody>
              {acts.map((a) => (
                <tr key={a.name}>
                  <td class="a-main">
                    <Target label={a.target} />
                  </td>
                  <td class="a-kind">{a.mnemonic}</td>
                  <td class="a-age r">
                    <Since t={ms(a.startTime)} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <AsOf actions={state.actions} />
        </>
      )}
    </>
  );
};

const WorkerRow = ({ w, h, state, now }: { w: Worker; h: WorkerHistory | undefined; state: State; now: UnixMs }) => {
  const id = idOf(w.name);
  const instance = idOf(w.instance);
  const phase = w.phase.toLowerCase();
  return (
    <div class="wrow">
      <Disclosure
        id={`wd-${id}`}
        btnCls="wrow__btn"
        panelCls="wdetail"
        summary={
          <>
            <span class="wrow__id">
              <svg class="wrow__chev" viewBox="0 0 10 10" aria-hidden="true">
                <path d="M3 1.5 6.5 5 3 8.5" />
              </svg>
              <span class="wrow__inst">{instance}</span>
              <span class="wrow__uuid">{workerShort(id, instance)}</span>
            </span>
            <span class={`phase phase--${phase}`}>
              <span class="phase__name">{phase}</span>
              <span class="phase__since">
                <Since t={ms(w.phaseTime)} />
              </span>
              {w.paused && <span class="paused">paused</span>}
            </span>
            <Pips w={w} />
            <Lane w={w} h={h} now={now} />
          </>
        }
      >
        <WorkerDetail w={w} h={h} state={state} />
      </Disclosure>
    </div>
  );
};

const Ghost = ({ state, n, overdue }: { state: Counted; n: number; overdue: number }) => (
  <div class="wghost">
    <span class="tally__item">
      <StaticCell state={state} />
      <b>{n}</b> {STATE_NAME[state]}
      {overdue > 0 && <span class="caution"> · {overdue} overdue</span>}
    </span>
    <span />
    <span />
    <span class="wghost__lane" aria-hidden="true" />
  </div>
);

const ghosts = (f: Fleet, instances: readonly Instance[]) => {
  const overdue = instances.filter((i) => i.fleet === f.name && i.pastBootGrace).length;
  return fleetCounts(f)
    .filter(([state, n]) => n > 0 && GHOSTED[state])
    .map(([state, n]) => ({ state, n, overdue: state === "unconnected" ? overdue : 0 }));
};

export const Workers = () => {
  const client = useClient();
  const state = client.state.value;
  const history = client.history.value;
  const [fleet, setFleet] = useState("");
  const now = useNowStep(LANE_STEP_MS);
  if (!state) return null;
  const ws = state.workers;
  const pending = sum(state.fleets, (f) => f.instances.pending);
  if (ws.length === 0) {
    return (
      <Empty>
        <h2>{pending ? `No workers yet · ${pending} booting` : "No workers"}</h2>
      </Empty>
    );
  }
  return (
    <>
      <div class="toolbar">
        <FleetFilter id="wfleet" fleets={state.fleets} value={fleet} onChange={setFleet} count={(f) => ws.filter((w) => w.fleet === f.name).length} />
        <Tally items={fleetTotals(state.fleets).filter(([s]) => TALLIED[s])} />
      </div>
      <div class="wgrid">
        <div class="wgrid__axis" aria-hidden="true">
          <span />
          <span />
          <span />
          <span class="axis">
            <span>−30m</span>
            <span>−20m</span>
            <span>−10m</span>
            <span>now</span>
          </span>
        </div>
        {state.fleets.map((f) => {
          const rows = ws.filter((w) => w.fleet === f.name);
          const gs = ghosts(f, state.instances);
          if (rows.length === 0 && gs.length === 0) return null;
          return (
            <section key={f.name} class="wgroup" aria-label={idOf(f.name)} hidden={fleet !== "" && fleet !== f.name}>
              <div class="wgroup__head">
                <h2>{idOf(f.name)}</h2>
                <span class="section__count">{rows.length}</span>
              </div>
              {rows.map((w) => (
                <WorkerRow key={w.name} w={w} h={history?.workers.get(w.name)} state={state} now={now} />
              ))}
              {gs.map((g) => (
                <Ghost key={g.state} {...g} />
              ))}
            </section>
          );
        })}
      </div>
    </>
  );
};
