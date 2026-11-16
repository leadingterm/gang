// Overview: four readouts, the fleets in preference order, the control pass and recent scaling.
import { eventKind, idOf, ms } from "../wire-parse.ts";
import type { Cause, Fleet, Recent, ScalingEvent, State } from "../wire.ts";
import { donePerMinute, doneSeries, passBars, queuedSeries, runningSeries, unrecordedSlots, windowMs } from "../recent.ts";
import { clock, duration, humanize, sum } from "../format.ts";
import { Empty, EVENT_VIEW, EventWhat, Glyph, Ident, Num, Section, Since, useClient, useEnter } from "../ui.tsx";
import { Legend, Tape } from "./fleet.tsx";

/** Scaling events the Overview shows, however old. */
const RECENT_EVENTS = 6;

const SPARK_W = 300;
const SPARK_H = 44;

const Spark = ({ values, label, recent }: { values: readonly (number | null)[]; label: string; recent: Recent }) => {
  const missing = unrecordedSlots(recent);
  const hatch = `spark-hatch-${label.replaceAll(" ", "-")}`;
  const peak = Math.max(1, ...values.map((value) => value ?? 0));
  const x = (i: number): number => values.length > 1 ? (i / (values.length - 1)) * SPARK_W : 0;
  const y = (value: number): number => SPARK_H - (value / peak) * (SPARK_H - 2) - 1;
  let connected = false;
  const path = values.map((value, i) => {
    if (value === null) {
      connected = false;
      return "";
    }
    const point = `${connected ? "L" : "M"}${x(i).toFixed(1)} ${y(value).toFixed(1)}`;
    connected = true;
    return point;
  }).join(" ");
  return (
    <svg class="spark" viewBox={`0 0 ${SPARK_W} ${SPARK_H}`} preserveAspectRatio="none" role="img" aria-label={label}>
      <defs>
        <pattern id={hatch} width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <rect width="6" height="6" fill="var(--color-rule)" />
          <path d="M0 0V6" stroke="var(--color-rule-2)" stroke-width="3" />
        </pattern>
      </defs>
      {missing > 0 && (
        <rect class="spark__unrecorded" x="0" y="0" width={values.length ? missing / values.length * SPARK_W : 0} height={SPARK_H} fill={`url(#${hatch})`}>
          <title>not recorded</title>
        </rect>
      )}
      <path class="spark__run" d={path} />
      {values.map((value, i) => value !== null && values[i - 1] == null && values[i + 1] == null && (
        <circle key={i} class="spark__point" cx={x(i)} cy={y(value)} r="2" />
      ))}
    </svg>
  );
};

const Meter = ({ f, g, label }: { f?: number; g: number; label?: string }) => (
  <div class="meter" {...(label ? { role: "img", "aria-label": label } : { "aria-hidden": "true" })}>
    {f !== undefined && <i class="meter__fill" style={{ "--f": String(f) }} />}
    <i class="meter__fill meter__fill--b" style={{ "--g": String(g) }} />
  </div>
);

const Readouts = ({ state }: { state: State }) => {
  const running = sum(state.fleets, (f) => f.demand.running);
  const slots = sum(state.workers, (w) => w.slots ?? 0);
  const inService = sum(state.fleets, (f) => f.instances.inService);
  const max = sum(state.fleets, (f) => f.policy.max);
  const spot = sum(
    state.fleets.filter((f) => f.properties["capacity"] === "spot"),
    (f) => f.instances.inService,
  );
  const done = donePerMinute(state.recent);
  const { queue } = state;
  return (
    <section class="readouts" aria-label="Farm readouts">
      <div class="readout">
        <h2 class="label">Queue</h2>
        <p class="readout__big">
          <Num value={queue.total} />
        </p>
        <p class="readout__sub">
          {queue.oldestQueueTime !== undefined ? (
            <span>
              oldest <Since t={ms(queue.oldestQueueTime)} />
            </span>
          ) : (
            <span>&nbsp;</span>
          )}
          {queue.unplaceable > 0 && <span class="caution">{queue.unplaceable} unplaceable</span>}
        </p>
        <Spark values={queuedSeries(state.recent)} label="Queued" recent={state.recent} />
      </div>
      <div class="readout">
        <h2 class="label">Executing</h2>
        <p class="readout__big">
          <Num value={running} />
          <span class="readout__of">/{slots}</span>
        </p>
        <Meter g={slots ? running / slots : 0} />
        <Spark values={runningSeries(state.recent)} label="Executing" recent={state.recent} />
      </div>
      <div class="readout">
        <h2 class="label">Instances</h2>
        <p class="readout__big">
          <Num value={inService} />
          <span class="readout__of">/{max}</span>
        </p>
        <Meter f={max ? inService / max : 0} g={max ? spot / max : 0} label={`${spot} spot, ${inService - spot} on-demand`} />
        <p class="readout__sub">
          <span class="fern">{spot} spot</span>
        </p>
      </div>
      <div class="readout">
        <h2 class="label">Done / min</h2>
        <p class="readout__big">{done === null ? <span class="num live">–</span> : <Num value={done.perMinute} />}</p>
        {done === null ? <p class="readout__sub">not recorded yet</p> : done.spanMs < 60 * 1000 && <p class="readout__sub">over {Math.round(done.spanMs / 1000)}s</p>}
        <Spark values={doneSeries(state.recent)} label="Done per minute" recent={state.recent} />
      </div>
    </section>
  );
};

const FleetRow = ({ f, ordinal, events }: { f: Fleet; ordinal: number; events: readonly ScalingEvent[] }) => {
  const capacity = f.properties["capacity"];
  const idle = f.instances.inService === 0 && f.instances.pending === 0;
  const cold =
    f.instances.pending > 0 && f.instances.inService === 0
      ? events.findLast((e) => e.fleet === f.name && "scaledOut" in e)
      : undefined;
  return (
    <li class={idle ? "fleet is-idle" : "fleet"}>
      <div class="fleet__id">
        <span class="fleet__ord">
          <span class="visually-hidden">preference </span>
          {ordinal}
        </span>
        <h3 class="fleet__name" title={idOf(f.name)}>{idOf(f.name)}</h3>
        <span class="fleet__tags">
          {capacity && <span class={capacity === "spot" ? "tag tag--spot" : "tag"}>{capacity}</span>}
          {f.launchFailure && <span class="tag tag--caution">can't launch</span>}
          {cold && (
            <span class="tag tag--note">
              starting <Since t={ms(cold.createTime)} />
            </span>
          )}
        </span>
        {f.launchFailure && <p class="fleet__failure caution">{f.instances.inService} of {f.desired} · {f.launchFailure.reason}</p>}
      </div>
      <div class="fleet__gauge">
        <Tape f={f} />
      </div>
      <dl class="fleet__nums">
        <div>
          <dt>running</dt>
          <dd>
            <Num value={f.demand.running} />
          </dd>
        </div>
        <div>
          <dt>queued</dt>
          <dd>
            <Num value={f.demand.queued} />
          </dd>
        </div>
      </dl>
    </li>
  );
};

const PassBar = ({ state }: { state: State }) => {
  const bars = passBars(state.recent);
  const skipped = bars.filter((b) => b === "skipped").length;
  return (
    <>
      <div class="passbar" role="img" aria-label={`${skipped} of the last ${bars.length} steps had a skipped pass`}>
        {bars.map((b, i) => (
          <i key={i} class={b === "ok" ? undefined : `is-${b}`} />
        ))}
      </div>
      <div class="passbar__axis" aria-hidden="true">
        <span>−{duration(windowMs(state.recent))}</span>
        <span>now</span>
      </div>
    </>
  );
};

/** Causes whose name alone does not read well. */
const CAUSE_TEXT: Partial<Record<Cause, string>> = {
  ORPHAN: "no worker within boot grace",
  MAX_DRAIN_WAIT: "max drain wait reached",
};

/** Causes that point at something gone wrong. */
const CAUTION: ReadonlySet<Cause> = new Set<Cause>(["ORPHAN", "MAX_DRAIN_WAIT"]);

export const EventRow = ({ e, withCause }: { e: ScalingEvent; withCause: boolean }) => {
  const ref = useEnter<HTMLLIElement>();
  const cause = "launchFailed" in e ? e.launchFailed.reason : e.cause === undefined ? undefined : (CAUSE_TEXT[e.cause] ?? humanize(e.cause));
  const kind = eventKind(e);
  return (
    <li ref={ref} class={withCause ? "event" : "event event--brief"}>
      <span class="event__time">{clock(ms(e.createTime))}</span>
      <Glyph kind={kind} cls="event__glyph" />
      <span class="visually-hidden">{kind ? EVENT_VIEW[kind].label : "Event"}</span>
      <span class="event__fleet"><Ident text={idOf(e.fleet)} /></span>
      <span class={kind === "workerRegistered" || kind === "workerDeregistered" || kind === "drainStarted" || kind === "drainCancelled" || kind === "terminated" ? "event__what event__what--instance" : "event__what"}>
        <EventWhat e={e} />
      </span>
      {withCause && <span class={"launchFailed" in e || (e.cause !== undefined && CAUTION.has(e.cause)) ? "event__cause caution" : "event__cause"}>{cause ?? ""}</span>}
    </li>
  );
};

export const Overview = () => {
  const client = useClient();
  const state = client.state.value;
  if (!state) return null;
  const recent = state.events.slice(-RECENT_EVENTS).reverse();
  const idle = state.workers.length === 0 && state.queue.total === 0;
  return (
    <>
      <Readouts state={state} />
      {idle && (
        <Empty>
          <h2>Idle · no workers, nothing queued</h2>
        </Empty>
      )}
      <Section id="fleets-h" title="Fleets">
        <div class="legend-row"><Legend /></div>
        <ol class="fleets">
          {state.fleets.map((f, i) => (
            <FleetRow key={f.name} f={f} ordinal={i + 1} events={state.events} />
          ))}
        </ol>
      </Section>
      <div class="split">
        <Section id="pass-h" title="Control pass">
          <PassBar state={state} />
        </Section>
        <Section
          id="recent-h"
          title="Scaling"
          aside={
            <a class="more" href="/ui/activity">
              All
            </a>
          }
        >
          <ol class="events">
            {recent.map((e) => (
              <EventRow key={e.name} e={e} withCause={false} />
            ))}
          </ol>
        </Section>
      </div>
    </>
  );
};
