// Overview: five readouts, the fleets in preference order, the control pass and recent scaling.
import type { ComponentChildren } from "preact";
import { eventKind, idOf, ms } from "../wire-parse.ts";
import type { Cause, Fleet, Recent, ScalingEvent, State } from "../wire.ts";
import { cacheWindow, donePerMinute, doneSeries, passBars, queuedSeries, runningSeries, unrecordedSlots, windowMs } from "../recent.ts";
import { humanize, sum } from "../format.ts";
import { Clock, Duration, Empty, EVENT_VIEW, EventWhat, Glyph, Ident, Num, Section, Since, useClient, useEnter } from "../ui.tsx";
import { Legend, Tape } from "./fleet.tsx";

/** Scaling events the Overview shows, however old. */
const RECENT_EVENTS = 6;

const SPARK_W = 300;
const SPARK_H = 44;
const SPARK_POINT_RADIUS = 2;

const Spark = ({ values, areaValues, label, recent }: {
  values: readonly (number | null)[];
  areaValues?: readonly (number | null)[];
  label: string;
  recent: Recent;
}) => {
  const missing = unrecordedSlots(recent);
  const hatch = `spark-hatch-${label.replaceAll(" ", "-")}`;
  let peak = 1;
  for (const value of values) if (value !== null) peak = Math.max(peak, value);
  if (areaValues) for (const value of areaValues) if (value !== null) peak = Math.max(peak, value);
  const x = (i: number): number => values.length > 1 ? SPARK_POINT_RADIUS + (i / (values.length - 1)) * (SPARK_W - 2 * SPARK_POINT_RADIUS) : SPARK_POINT_RADIUS;
  const y = (value: number): number => SPARK_H - SPARK_POINT_RADIUS - (value / peak) * (SPARK_H - 2 * SPARK_POINT_RADIUS);
  const trace = (series: readonly (number | null)[], fillToBaseline = false): string => {
    const points: string[] = [];
    let firstX: string | undefined;
    let lastX = "";
    const close = () => {
      if (fillToBaseline && firstX !== undefined) {
        const baseline = y(0).toFixed(1);
        points.push(`L${lastX} ${baseline} L${firstX} ${baseline} Z`);
      }
      firstX = undefined;
    };
    series.forEach((value, i) => {
      if (value === null) {
        close();
        return;
      }
      const pointX = x(i).toFixed(1);
      points.push(`${firstX === undefined ? "M" : "L"}${pointX} ${y(value).toFixed(1)}`);
      firstX ??= pointX;
      lastX = pointX;
    });
    close();
    return points.join(" ");
  };
  const path = trace(values);
  return (
    <svg class="spark" viewBox={`0 0 ${SPARK_W} ${SPARK_H}`} preserveAspectRatio="none" role="img" aria-label={label}>
      <title>{label}</title>
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
      {areaValues && <path class="spark__area" d={trace(areaValues, true)} />}
      <path class="spark__run" d={path} />
      {values.map((value, i) => value !== null && values[i - 1] == null && values[i + 1] == null && (
        <circle key={i} class="spark__point" cx={x(i)} cy={y(value)} r={SPARK_POINT_RADIUS} />
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

const Readout = ({ label, value, meter, caption, chart }: {
  label: string;
  value: ComponentChildren;
  meter?: ComponentChildren;
  caption?: ComponentChildren;
  chart?: ComponentChildren;
}) => (
  <div class="readout">
    <h2 class="label">{label}</h2>
    <p class="readout__big">{value}</p>
    <div class="readout__meter" aria-hidden={meter == null ? "true" : undefined}>{meter}</div>
    <p class="readout__sub">{caption}</p>
    <div class="readout__chart" aria-hidden={chart == null ? "true" : undefined}>{chart}</div>
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
  const cache = cacheWindow(state.recent);
  const { queue } = state;
  return (
    <section class="readouts" aria-label="Farm readouts">
      <Readout
        label="Queue"
        value={<Num value={queue.total} />}
        caption={
          <>
            {queue.oldestQueueTime !== undefined && (
              <span>oldest <Since t={ms(queue.oldestQueueTime)} /></span>
            )}
            {queue.unplaceable > 0 && <span class="caution">{queue.unplaceable} unplaceable</span>}
          </>
        }
        chart={<Spark values={queuedSeries(state.recent)} label="Queued" recent={state.recent} />}
      />
      <Readout
        label="Executing"
        value={<><Num value={running} /><span class="readout__of">/{slots}</span></>}
        meter={<Meter g={slots ? running / slots : 0} />}
        chart={<Spark values={runningSeries(state.recent)} label="Executing" recent={state.recent} />}
      />
      <Readout
        label="Instances"
        value={<><Num value={inService} /><span class="readout__of">/{max}</span></>}
        meter={<Meter f={max ? inService / max : 0} g={max ? spot / max : 0} label={`${spot} spot, ${inService - spot} on-demand`} />}
        caption={<span class="fern">{spot} spot</span>}
      />
      <Readout
        label="Done / min"
        value={done === null ? <span class="num live">–</span> : <Num value={done.perMinute} />}
        caption={done === null ? "not recorded yet" : done.spanMs < 60 * 1000 && <>over <Duration ms={Math.round(done.spanMs / 1000) * 1000} /></>}
        chart={<Spark values={doneSeries(state.recent)} label="Done per minute" recent={state.recent} />}
      />
      <Readout
        label="Action cache"
        value={cache.rate === null ? <span class="cache-rate__empty">no lookups</span> : <span class="cache-rate">{Math.round(cache.rate * 100)}%{" "}<span class="cache-rate__of">of {cache.lookups} lookups</span></span>}
        caption={<span>hit rate · <Duration ms={windowMs(state.recent)} /></span>}
        chart={<Spark values={cache.hitsPerStep} areaValues={cache.lookupsPerStep} label="Action-cache lookups as area and hits as line per step" recent={state.recent} />}
      />
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
        <h3 class="fleet__name"><Ident text={idOf(f.name)} /></h3>
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
        <span>−<Duration ms={windowMs(state.recent)} /></span>
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
      <span class="event__time"><Clock t={ms(e.createTime)} /></span>
      <Glyph kind={kind} cls="event__glyph" />
      <span class="visually-hidden">{kind ? EVENT_VIEW[kind].label : "Event"}</span>
      <span class="event__fleet"><Ident text={idOf(e.fleet)} /></span>
      <span class="event__what">
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
      <div class="cols">
        <Section id="pass-h" title="Control pass">
          <PassBar state={state} />
        </Section>
        <Section
          id="recent-h"
          title="Scaling"
          cls="cols__wide"
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
