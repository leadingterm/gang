// Workers: one row per worker grouped by fleet, with server-recorded phase lanes;
// instances without a worker sit under their fleet as counts.
import { useState } from "preact/hooks";
import { idOf, ms, type UnixMs } from "../wire-parse.ts";
import type { Fleet, Instance, Phase, State, Worker } from "../wire.ts";
import { windowMs } from "../recent.ts";
import { clock, duration, sum, workerInstance } from "../format.ts";
import { Disclosure, Empty, FleetFilter, Ident, SectionHead, Since, Target, useClient, useNowStep } from "../ui.tsx";
import { AsOf } from "./actions.tsx";
import { fleetCounts, fleetTotals, STATE_NAME, StaticCell, Tally, type Counted } from "./fleet.tsx";

/** The instances without a worker are listed under their fleet. */
const GHOSTED: Record<Counted, boolean> = { busy: false, idle: false, draining: false, unconnected: true, pending: true, terminating: true, warm: false };

/** The toolbar tallies workers by phase, and the instances that have none yet. */
const TALLIED: Record<Counted, boolean> = { busy: true, idle: true, draining: true, unconnected: true, pending: true, terminating: false, warm: false };

/** Lanes move at this resolution, so rows do not re-render every second. */
const LANE_STEP_MS = 10 * 1000;

type LaneSegment = { readonly phase: Phase | "off" | "unrecorded"; readonly from: UnixMs; readonly to: UnixMs };

export const laneSegments = (w: Worker, now: UnixMs, window: number): readonly LaneSegment[] => {
  const start = now - window;
  const launched = ms(w.launchTime);
  const phases = w.phases?.length ? w.phases : [{ phase: w.phase, start: w.phaseTime }];
  const first = phases[0];
  const segments: LaneSegment[] = [
    { phase: "off", from: start, to: launched },
    { phase: "unrecorded", from: Math.max(start, launched), to: first ? ms(first.start) : now },
    ...phases.map((span, i) => {
      const next = phases[i + 1];
      return {
        phase: span.phase,
        from: Math.max(launched, ms(span.start)),
        to: next ? ms(next.start) : now,
      };
    }),
  ];
  return segments
    .map((s) => ({ ...s, from: Math.max(start, s.from), to: Math.min(now, s.to) }))
    .filter((s) => s.to > s.from);
};

const Lane = ({ w, now, window }: { w: Worker; now: UnixMs; window: number }) => {
  const start = now - window;
  const segments = laneSegments(w, now, window);
  return (
    <div class="lane" role="img" aria-label={`Phase history: ${segments.map((s) => s.phase.toLowerCase()).join(", ")}`}>
      {segments.map((s) => (
        <i
          key={`${s.phase}-${s.from}`}
          class={`lane__seg lane__seg--${s.phase.toLowerCase()}`}
          style={{
            "--x": String(((s.from - start) / window).toFixed(4)),
            "--w": String(((s.to - s.from) / window).toFixed(4)),
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

export const WorkerDetail = ({ w, state }: { w: Worker; state: State }) => {
  const client = useClient();
  const acts = state.actions?.running.filter((a) => a.worker === w.name) ?? [];
  const phases = w.phases?.length ? w.phases : [{ phase: w.phase, start: w.phaseTime }];
  const segments = phases.map((s, i) => {
    const next = phases[i + 1];
    return {
      phase: s.phase,
      from: ms(s.start),
      to: next ? ms(next.start) : null,
    };
  }).toReversed();
  const launched = ms(w.launchTime);
  return (
    <>
      <dl class="kv kv--tight">
        <dt>worker</dt>
        <dd><Ident text={idOf(w.name)} /></dd>
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
            <thead>
              <tr>
                <th scope="col">Target</th>
                <th scope="col">Mnemonic</th>
                <th scope="col" class="r">Running</th>
              </tr>
            </thead>
            <tbody>
              {acts.map((a) => (
                <tr key={a.name}>
                  <td class="a-main">
                    {a.target ? <Target label={a.target} /> : <span class="mono quiet">{a.digest.slice(0, 12)}</span>}
                  </td>
                  <td class="a-kind">{a.mnemonic || "—"}</td>
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

const WorkerRow = ({ w, state, now }: { w: Worker; state: State; now: UnixMs }) => {
  const id = idOf(w.name);
  const instance = workerInstance(id);
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
              <span class="wrow__inst"><Ident text={instance} /></span>
            </span>
            <span class={`phase phase--${phase}`}>
              <span class="phase__name">{phase}</span>
              <span class="phase__since">
                <Since t={ms(w.phaseTime)} />
              </span>
              {w.paused && <span class="paused">paused</span>}
            </span>
            <Pips w={w} />
            <Lane w={w} now={now} window={windowMs(state.recent)} />
          </>
        }
      >
        <WorkerDetail w={w} state={state} />
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
  const [fleet, setFleet] = useState("");
  const stepped = useNowStep(LANE_STEP_MS);
  if (!state) return null;
  const now = Math.max(stepped, ms(state.updateTime));
  const window = windowMs(state.recent);
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
            <span>−{duration(window)}</span>
            <span>−{duration(2 * window / 3)}</span>
            <span>−{duration(window / 3)}</span>
            <span>now</span>
          </span>
        </div>
        {state.fleets.map((f) => {
          const rows = ws.filter((w) => w.fleet === f.name);
          const gs = ghosts(f, state.instances);
          if (rows.length === 0 && gs.length === 0) return null;
          return (
            <section key={f.name} class="wgroup" aria-labelledby={`wg-${idOf(f.name)}`} hidden={fleet !== "" && fleet !== f.name}>
              <SectionHead id={`wg-${idOf(f.name)}`} title={idOf(f.name)} count={rows.length} mono />
              {rows.map((w) => (
                <WorkerRow key={w.name} w={w} state={state} now={now} />
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
