// Overview: five readouts, the fleets in preference order, the control pass and recent scaling.
import { useId } from "preact/hooks";
import { eventKind, idOf, ms, scalingLog } from "../wire-parse.ts";
import type { Cause, Fleet, FleetInstanceSample, Recent, ScalingEvent, State } from "../wire.ts";
import { bytesSeries, cacheWindow, donePerMinute, doneSeries, evictedSeries, passBars, queuedSeries, runningSeries, schedulerCurrent, schedulerKnown, slots as recentSlots, transferred, unrecordedSlots, windowMs } from "../recent.ts";
import { bytes, humanize, percent, sum } from "../format.ts";
import { Cell, ChartFrame, Clock, Columns, Duration, Empty, EVENT_VIEW, EventWhat, Glyph, Ident, Num, Row, Section, Since, Table, Tile, Tiles, useClient, useEnter } from "../ui.tsx";
import { Legend, Tape, type FleetCensus } from "./fleet.tsx";

/** Scaling events the Overview shows, however old. */
const RECENT_EVENTS = 6;

const SPARK_W = 300;
const SPARK_H = 44;
const SPARK_POINT_RADIUS = 2;

type SparkSeries = { name: string; values: readonly (number | null)[]; reference?: boolean };

const Spark = ({ values, areaValues, secondary, series, unknown, label, recent, scheduler = false }: {
  values: readonly (number | null)[];
  areaValues?: readonly (number | null)[];
  secondary?: readonly (number | null)[];
  series?: readonly SparkSeries[];
  unknown?: readonly boolean[];
  label: string;
  recent: Recent;
  scheduler?: boolean;
}) => {
  const missing = unrecordedSlots(recent);
  const hatch = `spark-hatch-${useId()}`;
  let peak = 1;
  for (const value of values) if (value !== null) peak = Math.max(peak, value);
  if (areaValues) for (const value of areaValues) if (value !== null) peak = Math.max(peak, value);
  if (secondary) for (const value of secondary) if (value !== null) peak = Math.max(peak, value);
  if (series) for (const line of series) for (const value of line.values) if (value !== null) peak = Math.max(peak, value);
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
      {!unknown && missing > 0 && (
        <rect class="spark__unrecorded" x="0" y="0" width={values.length ? missing / values.length * SPARK_W : 0} height={SPARK_H} fill={`url(#${hatch})`}>
          <title>not recorded</title>
        </rect>
      )}
      {scheduler && recentSlots(recent).map((sample, i) => sample && !schedulerKnown(sample) && (
        <rect key={i} class="spark__unknown" x={i === 0 ? 0 : (x(i - 1) + x(i)) / 2} y="0"
          width={(i === values.length - 1 ? SPARK_W : (x(i) + x(i + 1)) / 2) - (i === 0 ? 0 : (x(i - 1) + x(i)) / 2)}
          height={SPARK_H} fill={`url(#${hatch})`}>
          <title>scheduler unknown</title>
        </rect>
      ))}
      {unknown?.map((missingSlot, i) => missingSlot && (
        <rect key={i} class="spark__unknown" x={i === 0 ? 0 : (x(i - 1) + x(i)) / 2} y="0"
          width={(i === values.length - 1 ? SPARK_W : (x(i) + x(i + 1)) / 2) - (i === 0 ? 0 : (x(i - 1) + x(i)) / 2)}
          height={SPARK_H} fill={`url(#${hatch})`}>
          <title>not recorded</title>
        </rect>
      ))}
      {areaValues && <path class="spark__area" d={trace(areaValues, true)} />}
      {!series && <path class="spark__run" d={path} />}
      {secondary && <path class="spark__run spark__run--secondary" d={trace(secondary)} />}
      {!series && values.map((value, i) => value !== null && values[i - 1] == null && values[i + 1] == null && (
        <circle key={i} class="spark__point" cx={x(i)} cy={y(value)} r={SPARK_POINT_RADIUS} />
      ))}
      {secondary?.map((value, i) => value !== null && secondary[i - 1] == null && secondary[i + 1] == null && (
        <circle key={i} class="spark__point spark__point--secondary" cx={x(i)} cy={y(value)} r={SPARK_POINT_RADIUS} />
      ))}
      {series?.map((line) => (
        <g key={line.name} class={`spark__series spark__series--${line.name}${line.reference ? " spark__series--reference" : ""}`}>
          <path data-series={line.name} class={`spark__run${line.reference ? " spark__run--reference" : ""}`} d={trace(line.values)} />
          {line.values.map((value, i) => value !== null && line.values[i - 1] == null && line.values[i + 1] == null && (
            <circle key={i} class="spark__point" cx={x(i)} cy={y(value)} r={SPARK_POINT_RADIUS} />
          ))}
        </g>
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
  const inService = sum(state.fleets, (f) => f.instances.inService);
  const max = sum(state.fleets, (f) => f.policy.max);
  const spot = sum(
    state.fleets.filter((f) => f.properties["capacity"] === "spot"),
    (f) => f.instances.inService,
  );
  const known = schedulerCurrent(state);
  const done = donePerMinute(state.recent);
  const samples = state.recent.samples ?? [];
  const awaitingFirstStep = state.schedulerKnown === undefined && samples.every((s) => s.schedulerKnown === false) ||
    samples.length < 2 && state.schedulerKnown !== false &&
      (samples.length === 0 || schedulerKnown(samples[0]!));
  const cache = cacheWindow(state.recent);
  const { queue } = state;
  const traffic = transferred(state.recent);
  const cas = state.cache.stores?.find((store) => store.store === "CAS");
  return (
    <section aria-label="Farm readouts">
      <Tiles>
      <Tile
        label="Queue"
        value={known ? <Num value={queue.total} /> : "unknown"}
        caption={
          <>
            {known && queue.oldestQueueTime !== undefined && (
              <span>oldest <Since t={ms(queue.oldestQueueTime)} /></span>
            )}
            {known && queue.unplaceable > 0 && <span class="caution">{queue.unplaceable} unplaceable</span>}
          </>
        }
        chart={<ChartFrame size="spark"><Spark values={queuedSeries(state.recent)} label="Queued" recent={state.recent} scheduler /></ChartFrame>}
      />
      <Tile
        label="Executing"
        value={known ? <Num value={running} /> : "unknown"}
        chart={<ChartFrame size="spark"><Spark values={runningSeries(state.recent)} label="Executing" recent={state.recent} scheduler /></ChartFrame>}
      />
      <Tile
        label="Instances"
        value={<><Num value={inService} />{max > 0 && <span class="readout__of">/{max}</span>}</>}
        meter={max > 0 ? <Meter f={inService / max} g={spot / max} label={`${spot} spot, ${inService - spot} on-demand`} /> : undefined}
        caption={<span class="fern">{spot} spot</span>}
      />
      <Tile
        label="Done / min"
        value={done === null ? <span class="num live">–</span> : <Num value={done.perMinute} />}
        caption={done === null ? awaitingFirstStep ? "not recorded yet" : "not available" : done.spanMs < 60 * 1000 && <>over <Duration ms={Math.round(done.spanMs / 1000) * 1000} /></>}
        chart={<ChartFrame size="spark"><Spark values={doneSeries(state.recent)} label="Done per minute" recent={state.recent} scheduler /></ChartFrame>}
      />
      <Tile
        label="Action cache"
        value={cache.rate === null ? <span class="cache-rate__empty">no lookups</span> : <span class="cache-rate">{Math.round(cache.rate * 100)}%{" "}<span class="cache-rate__of">of {cache.lookups} lookups</span></span>}
        meter={known && cas && <Meter g={cas.budgetBytes > 0 ? cas.usedBytes / cas.budgetBytes : 0} label={`CAS ${percent(cas.usedBytes, cas.budgetBytes)} of budget`} />}
        caption={<><span>hit rate · <Duration ms={windowMs(state.recent)} /></span><span>{traffic ? `↓ ${bytes(traffic.downloaded)} · ↑ ${bytes(traffic.uploaded)}` : "—"}</span></>}
        chart={<ChartFrame size="spark"><Spark values={cache.hitsPerStep} areaValues={cache.lookupsPerStep} label="Action-cache lookups as area and hits as line per step" recent={state.recent} /></ChartFrame>}
      />
      </Tiles>
    </section>
  );
};

const Cache = ({ state }: { state: State }) => {
  const stores = state.cache.stores ?? [];
  const bucket = state.cache.bucket;
  const samples = state.recent.samples ?? [];
  const known = schedulerCurrent(state);
  const evicted = samples.every(schedulerKnown) ? sum(samples, (s) => s.evictedBytes) : null;
  return (
    <Section id="cache-h" title="Cache">
      {stores.length > 0 && (
        <Table cls="cache-table" cols={[{ head: "Store", share: 1 }, { head: "Used" }, { head: "Budget" }, { head: "Usage" }, { head: "Evicted (window, all stores)" }]}>
          {stores.map((store, index) => (
            <Row key={store.store}>
              <Cell><span class="build-label">Store </span>{store.store}</Cell><Cell><span class="build-label">Used </span>{known ? bytes(store.usedBytes) : "unknown"}</Cell><Cell><span class="build-label">Budget </span>{known ? bytes(store.budgetBytes) : "unknown"}</Cell>
              <Cell><span class="build-label">Usage </span>{known ? <Meter g={store.budgetBytes > 0 ? store.usedBytes / store.budgetBytes : 0} label={`${store.store} ${percent(store.usedBytes, store.budgetBytes)} of budget`} /> : "unknown"}</Cell>
              <Cell><span class="build-label">Evicted (window, all stores) </span>{index === 0 ? evicted === null ? "unknown" : bytes(evicted) : "—"}</Cell>
            </Row>
          ))}
        </Table>
      )}
      {bucket && (
        <Table cls="cache-table" cols={[{ head: "Bytes" }, { head: "Objects" }, { head: "Measurement", share: 1 }]}>
          <Row><Cell><span class="build-label">Bytes </span>{bytes(bucket.bytes)}</Cell><Cell><span class="build-label">Objects </span><Num value={bucket.objects} /></Cell><Cell cls="a-span"><span class="build-label">Measurement </span>as of <Since t={ms(bucket.measureTime)} /> · expiry by bucket lifecycle</Cell></Row>
        </Table>
      )}
      {!stores.length && !bucket && <p>Bucket size not measured yet; S3 reports once a day.</p>}
      <Columns>
        <ChartFrame size="spark" title="Transferred per step" legend={<p class="cache-sparks__legend"><span>↓ Downloaded</span><span>↑ Uploaded</span></p>}>
          <Spark values={bytesSeries(state.recent, "downloadedBytes")} secondary={bytesSeries(state.recent, "uploadedBytes")} label="Transferred per step" recent={state.recent} />
        </ChartFrame>
        <ChartFrame size="spark" title="Evicted per step">
          <Spark values={evictedSeries(state.recent)} label="Evicted per step" recent={state.recent} scheduler />
        </ChartFrame>
      </Columns>
    </Section>
  );
};

const FleetRow = ({ f, ordinal, events, census, known, recent }: { f: Fleet; ordinal: number; events: readonly ScalingEvent[]; census: FleetCensus; known: boolean; recent: Recent }) => {
  const capacity = f.properties["capacity"];
  const idle = f.instances.inService === 0 && f.instances.pending === 0;
  const cold =
    f.instances.pending > 0 && f.instances.inService === 0
      ? events.findLast((e) => e.fleet === f.name && "scaledOut" in e)
      : undefined;
  const samples: readonly (FleetInstanceSample | undefined)[] = recentSlots(recent).map((sample) => sample?.fleets?.find((fleet) => fleet.name === f.name));
  const fields = [
    { name: "pending", label: "Pending", value: (sample: FleetInstanceSample) => sample.instances.pending },
    { name: "inService", label: "In service", value: (sample: FleetInstanceSample) => sample.instances.inService },
    { name: "terminating", label: "Terminating", value: (sample: FleetInstanceSample) => sample.instances.terminating },
    { name: "warm", label: "Warm", value: (sample: FleetInstanceSample) => sample.instances.warm },
    { name: "desired", label: "Desired", value: (sample: FleetInstanceSample) => sample.desired, reference: true },
    { name: "max", label: "Max", value: (sample: FleetInstanceSample) => sample.max, reference: true },
  ];
  const series = fields.map((field) => ({ ...field, values: samples.map((sample) => sample ? field.value(sample) : null) }));
  const latest = samples.at(-1);
  const historyLabel = `Instance history for ${f.name}; latest sample: ${latest ? fields.map((field) => `${field.label} ${field.value(latest)}`).join(", ") : "not recorded"}`;
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
        {known ? <Tape f={f} census={census} /> : <span>Scheduler unknown</span>}
      </div>
      <dl class="fleet__nums">
        <div>
          <dt>running</dt>
          <dd>
            {known ? <Num value={f.demand.running} /> : "unknown"}
          </dd>
        </div>
        <div>
          <dt>queued</dt>
          <dd>
            {known ? <Num value={f.demand.queued} /> : "unknown"}
          </dd>
        </div>
      </dl>
      <div class="fleet__history">
        <ChartFrame size="spark" title="Instance history" legend={<div class="legend" aria-label="Instance history legend">
          {fields.map((field) => <span key={field.name} class="legend__item"><i class={`fleet__history-swatch spark__series--${field.name}${field.reference ? " spark__series--reference" : ""}`} aria-hidden="true" />{field.label}</span>)}
        </div>}>
          <Spark values={series[0]!.values} series={series} unknown={samples.map((sample) => !sample)} label={historyLabel} recent={recent} />
        </ChartFrame>
        {samples.every((sample) => !sample) && <p class="dim">not recorded</p>}
      </div>
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
  UTILIZATION: "measured fleet load stayed below the waterline for the dwell",
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
  const recent = scalingLog(state.events, state.statusEvents ?? []).slice(-RECENT_EVENTS).reverse();
  const known = schedulerCurrent(state);
  const idle = known && state.workers.length === 0 && state.queue.total === 0;
  return (
    <>
      <Readouts state={state} />
      {idle && (
        <Empty>
          <h2>Idle · no workers, nothing queued</h2>
        </Empty>
      )}
      <Section id="fleets-h" title="Fleets" legend={<Legend />}>
        <ol class="fleets">
          {state.fleets.map((f, i) => (
            <FleetRow key={f.name} f={f} ordinal={i + 1} events={state.events} census={state} known={known} recent={state.recent} />
          ))}
        </ol>
      </Section>
      <Cache state={state} />
      <Columns>
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
      </Columns>
    </>
  );
};
