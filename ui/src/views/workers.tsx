// Workers: one row per worker grouped by fleet, with server-recorded phase lanes;
// instances without a worker sit under their fleet as counts.
import { useState } from "preact/hooks";
import { idOf, ms, type UnixMs } from "../wire-parse.ts";
import type { Fleet, InfrastructureStatus, Instance, Phase, Recent, State, Worker, WorkerCache } from "../wire.ts";
import { schedulerCurrent, schedulerKnown, stepMs, windowMs } from "../recent.ts";
import { bytes, sum, workerInstance } from "../format.ts";
import { Cell, Clock, Disclosure, Duration, Empty, FleetFilter, Ident, Row, Section, Since, Table, Target, useClient, useNowStep } from "../ui.tsx";
import { AsOf, ReservationAmounts } from "./actions.tsx";
import { fleetCounts, fleetTotals, STATE_NAME, StaticCell, Tally, unhostedInstances, type Counted, type FleetCensus } from "./fleet.tsx";

/** The instances without a worker are listed under their fleet. */
const GHOSTED: Record<Counted, boolean> = { busy: false, idle: false, draining: false, stale: false, unconnected: true, pending: true, terminating: true, replacing: false, replacement: false, unknown: true, warm: false };

/** The toolbar tallies workers by phase, and the instances that have none yet. */
const TALLIED: Record<Counted, boolean> = { busy: true, idle: true, draining: true, stale: true, unconnected: true, pending: true, terminating: false, replacing: true, replacement: true, unknown: true, warm: false };

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

const Lane = ({ w, now, recent }: { w: Worker; now: UnixMs; recent: Recent }) => {
  const window = windowMs(recent);
  const start = now - window;
  const segments = laneSegments(w, now, window);
  const unknown = (recent.samples ?? []).filter((s) => !schedulerKnown(s)).map((s) => ({
    from: Math.max(start, ms(s.start), ms(w.launchTime)),
    to: Math.min(now, ms(s.start) + stepMs(recent)),
  })).filter((s) => s.to > s.from);
  return (
    <div class="lane" role="img" aria-label={`Phase history: ${segments.map((s) => s.phase.toLowerCase()).join(", ")}${unknown.length ? ", scheduler unknown" : ""}`}>
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
      {unknown.map((s) => (
        <i key={s.from} class="lane__seg lane__seg--unrecorded" title="scheduler unknown" style={{
          "--x": String(((s.from - start) / window).toFixed(4)),
          "--w": String(((s.to - s.from) / window).toFixed(4)),
        }} />
      ))}
    </div>
  );
};

const Infrastructure = ({ status }: { status: InfrastructureStatus }) => (
  <span>
    {status.phase.toLowerCase()} · {status.reason.toLowerCase().replaceAll("_", " ")}
    {status.otherInstance && <> · {status.phase === "REPLACING" ? "replacement" : "replacing"} {idOf(status.otherInstance)}</>}
  </span>
);

const ledger = (w: Worker): string =>
  `CPU admission ${w.reserved ? `${w.reserved.cpuMillicores} millicores` : "—"} / ${w.capacity ? `${w.capacity.cpuMillicores} millicores` : "—"} · memory ${w.reserved ? bytes(w.reserved.memory) : "—"} / ${w.capacity ? bytes(w.capacity.memory) : "—"} reserved / capacity`;

/** Store counters are cumulative for the lifetime of the worker's store. */
const CacheMetrics = ({ cache }: { cache: WorkerCache | undefined }) => (
  <dl class="kv kv--tight">
    <dt>local reads</dt>
    <dd class="mono">{cache ? `${cache.local.requests} requests · ${bytes(cache.local.bytes)}` : "unknown"}</dd>
    <dt>slow-store fetches</dt>
    <dd class="mono">{cache ? `${cache.remote.requests} requests · ${bytes(cache.remote.bytes)}` : "unknown"}</dd>
    <dt>evictions</dt>
    <dd class="mono">{cache ? `${cache.evictions} · ${bytes(cache.evicted)}` : "unknown"}</dd>
    <dt>used / budget</dt>
    <dd class="mono">{cache ? `${bytes(cache.used)} / ${bytes(cache.budget)}` : "unknown"}</dd>
  </dl>
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
          <span class="clause"><Since t={launched} /></span>{" · "}<span class="clause">since <Clock t={launched} /></span>
        </dd>
        <dt>running</dt>
        <dd class="mono">
          {schedulerCurrent(state) ? w.running : "unknown"}
        </dd>
        <dt>reservation</dt>
        <dd class="mono">{schedulerCurrent(state) ? ledger(w) : "unknown"}</dd>
        {w.stale && <><dt>release</dt><dd>stale or unknown worker version</dd></>}
        {w.infrastructure && <><dt>infrastructure</dt><dd><Infrastructure status={w.infrastructure} /></dd></>}
      </dl>
      <h3>Worker cache</h3>
      <p class="quiet">Cumulative store-lifetime counters.</p>
      <CacheMetrics cache={schedulerCurrent(state) ? w.cache : undefined} />
      {segments.length > 0 && (
        <ol class="phases" aria-label="Phase history">
          {segments.map((s) => (
            <li key={s.from} class="phases__row">
              <span class={`phase phase--${s.phase.toLowerCase()}`}>
                <span class="phase__name">{s.phase.toLowerCase()}</span>
              </span>
              <span class="mono">
                <Clock t={s.from} /> → {s.to === null ? "now" : <Clock t={s.to} />}
              </span>
              <span class="mono quiet"><Duration ms={(s.to ?? client.now.value) - s.from} /></span>
            </li>
          ))}
        </ol>
      )}
      {acts.length > 0 && state.actions && (
        <>
          <Table label="Running on this worker" cols={[{ head: "Target", share: 1 }, { head: "Mnemonic", fit: 10 }, { head: "Running", end: true }]}>
            {acts.map((a) => (
              <Row key={a.name}>
                <Cell cls="a-main">
                  {a.target ? <Target label={a.target} /> : <Ident text={a.digest.slice(0, 12)} title={a.digest} cls="quiet" />}
                  <ReservationAmounts reservation={a.reservation} learned={a.learnedReservation} />
                </Cell>
                <Cell cls="a-kind">{a.mnemonic ? <Ident text={a.mnemonic} /> : "—"}</Cell>
                <Cell cls="a-age r">
                  <Since t={ms(a.startTime)} />
                </Cell>
              </Row>
            ))}
          </Table>
          <AsOf actions={state.actions} />
        </>
      )}
    </>
  );
};

const WorkerRow = ({ w, state, now }: { w: Worker; state: State; now: UnixMs }) => {
  const id = idOf(w.name);
  const instance = workerInstance(id);
  const known = schedulerCurrent(state);
  const phase = known ? w.phase.toLowerCase() : "unknown";
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
              <span class="wrow__inst">
                <Ident text={instance} />
                {known ? <span class="dim" role="group" title="Reserved / admission capacity" aria-label={ledger(w)}>
                  <span style={{ display: "block" }}>CPU {w.reserved?.cpuMillicores ?? "—"} / {w.capacity?.cpuMillicores ?? "—"} mCPU</span>
                  <span style={{ display: "block" }}>Memory {w.reserved ? bytes(w.reserved.memory) : "—"} / {w.capacity ? bytes(w.capacity.memory) : "—"}</span>
                </span> : <span class="dim">Reservation unknown</span>}
              </span>
            </span>
            <span class={`phase phase--${phase}`}>
              <span class="phase__name">{phase}</span>
              {known && <span class="phase__since">
                <Since t={ms(w.phaseTime)} />
              </span>}
              {w.infrastructure && <> · <Infrastructure status={w.infrastructure} /></>}
              {w.stale && <span class="paused">stale</span>}
              {w.paused && <span class="paused">paused</span>}
            </span>
            <span>{known ? `${w.running} running` : "unknown"}</span>
            <Lane w={w} now={now} recent={state.recent} />
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

const ghosts = (f: Fleet, census: FleetCensus) => {
  const instances = unhostedInstances(f, census).filter((i) => !i.infrastructure);
  const overdue = instances.filter((i) => i.lifecycle === "IN_SERVICE" && i.pastBootGrace).length;
  return fleetCounts(f, { workers: [], instances })
    .filter(([state, n]) => n > 0 && GHOSTED[state])
    .map(([state, n]) => ({ state, n, overdue: state === "unconnected" ? overdue : 0 }));
};

const InfrastructureInstance = ({ instance }: { instance: Instance }) => (
  <div class="wghost">
    <span><Ident text={idOf(instance.name)} /> · {instance.lifecycle.toLowerCase().replaceAll("_", " ")}</span>
    <span>{instance.infrastructure && <Infrastructure status={instance.infrastructure} />}</span>
    <span />
    <span class="wghost__lane" aria-hidden="true" />
  </div>
);

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
  if (ws.length === 0 && state.instances.length === 0) {
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
        {schedulerCurrent(state) ? <Tally items={fleetTotals(state.fleets, state).filter(([s]) => TALLIED[s])} /> : <span>Scheduler unknown</span>}
      </div>
      <div class="wgrid">
        <div class="wgrid__axis" aria-hidden="true">
          <span />
          <span />
          <span />
          <span class="axis">
            <span>−<Duration ms={window} /></span>
            <span>−<Duration ms={2 * window / 3} /></span>
            <span>−<Duration ms={window / 3} /></span>
            <span>now</span>
          </span>
        </div>
        {state.fleets.map((f) => {
          const rows = ws.filter((w) => w.fleet === f.name);
          const gs = ghosts(f, state);
          const marked = unhostedInstances(f, state).filter((i) => i.infrastructure);
          if (rows.length === 0 && gs.length === 0 && marked.length === 0) return null;
          return (
            <Section key={f.name} id={`wg-${idOf(f.name)}`} title={idOf(f.name)} count={rows.length} mono variant="plain" cls="wgroup" hidden={fleet !== "" && fleet !== f.name}>
              <div class="section__legend">
                <h3>Worker cache totals</h3>
                <p class="quiet">Cumulative store-lifetime counters for connected workers reporting cache metrics.</p>
                <p>{schedulerCurrent(state) && f.workerCache ? `${f.workerCache.reporting} / ${f.workerCache.workers} workers reporting` : "Reporting coverage unknown"}</p>
                <CacheMetrics cache={schedulerCurrent(state) && f.workerCache?.reporting ? f.workerCache.totals : undefined} />
              </div>
              {rows.map((w) => (
                <WorkerRow key={w.name} w={w} state={state} now={now} />
              ))}
              {gs.map((g) => (
                <Ghost key={g.state} {...g} />
              ))}
              {marked.map((instance) => <InfrastructureInstance key={instance.name} instance={instance} />)}
            </Section>
          );
        })}
      </div>
    </>
  );
};
