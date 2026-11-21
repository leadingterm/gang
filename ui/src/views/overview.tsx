// Overview: five readouts, the fleets in preference order, the control pass and recent scaling.
import { eventKind, idOf, ms, scalingLog } from "../wire-parse.ts";
import type { Cause, Fleet, Recent, ScalingEvent, State } from "../wire.ts";
import { bytesSeries, cacheWindow, donePerMinute, doneSeries, evictedSeries, passBars, queuedSeries, runningSeries, transferred, unrecordedSlots, windowMs } from "../recent.ts";
import { bytes, humanize, percent, sum } from "../format.ts";
import { Cell, ChartFrame, Clock, Columns, Duration, Empty, EVENT_VIEW, EventWhat, Glyph, Ident, Num, Row, Section, Since, Table, Tile, Tiles, useClient, useEnter } from "../ui.tsx";
import { Legend, Tape, type FleetCensus } from "./fleet.tsx";

/** Scaling events the Overview shows, however old. */
const RECENT_EVENTS = 6;

const SPARK_W = 300;
const SPARK_H = 44;
const SPARK_POINT_RADIUS = 2;

const Spark = ({ values, areaValues, secondary, label, recent }: {
  values: readonly (number | null)[];
  areaValues?: readonly (number | null)[];
  secondary?: readonly (number | null)[];
  label: string;
  recent: Recent;
}) => {
  const missing = unrecordedSlots(recent);
  const hatch = `spark-hatch-${label.replaceAll(" ", "-")}`;
  let peak = 1;
  for (const value of values) if (value !== null) peak = Math.max(peak, value);
  if (areaValues) for (const value of areaValues) if (value !== null) peak = Math.max(peak, value);
  if (secondary) for (const value of secondary) if (value !== null) peak = Math.max(peak, value);
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
      {secondary && <path class="spark__run spark__run--secondary" d={trace(secondary)} />}
      {values.map((value, i) => value !== null && values[i - 1] == null && values[i + 1] == null && (
        <circle key={i} class="spark__point" cx={x(i)} cy={y(value)} r={SPARK_POINT_RADIUS} />
      ))}
      {secondary?.map((value, i) => value !== null && secondary[i - 1] == null && secondary[i + 1] == null && (
        <circle key={i} class="spark__point spark__point--secondary" cx={x(i)} cy={y(value)} r={SPARK_POINT_RADIUS} />
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
  const slots = state.workers.length > 0 && state.workers.every((w) => w.slots !== undefined)
    ? sum(state.workers, (w) => w.slots ?? 0)
    : undefined;
  const inService = sum(state.fleets, (f) => f.instances.inService);
  const max = sum(state.fleets, (f) => f.policy.max);
  const spot = sum(
    state.fleets.filter((f) => f.properties["capacity"] === "spot"),
    (f) => f.instances.inService,
  );
  const done = donePerMinute(state.recent);
  const cache = cacheWindow(state.recent);
  const { queue } = state;
  const traffic = transferred(state.recent);
  const cas = state.cache.stores?.find((store) => store.store === "CAS");
  return (
    <section aria-label="Farm readouts">
      <Tiles>
      <Tile
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
        chart={<ChartFrame size="spark"><Spark values={queuedSeries(state.recent)} label="Queued" recent={state.recent} /></ChartFrame>}
      />
      <Tile
        label="Executing"
        value={<><Num value={running} />{slots !== undefined && <span class="readout__of">/{slots}</span>}</>}
        meter={slots !== undefined ? <Meter g={slots ? running / slots : 0} /> : undefined}
        chart={<ChartFrame size="spark"><Spark values={runningSeries(state.recent)} label="Executing" recent={state.recent} /></ChartFrame>}
      />
      <Tile
        label="Instances"
        value={<><Num value={inService} /><span class="readout__of">/{max}</span></>}
        meter={<Meter f={max ? inService / max : 0} g={max ? spot / max : 0} label={`${spot} spot, ${inService - spot} on-demand`} />}
        caption={<span class="fern">{spot} spot</span>}
      />
      <Tile
        label="Done / min"
        value={done === null ? <span class="num live">–</span> : <Num value={done.perMinute} />}
        caption={done === null ? "not recorded yet" : done.spanMs < 60 * 1000 && <>over <Duration ms={Math.round(done.spanMs / 1000) * 1000} /></>}
        chart={<ChartFrame size="spark"><Spark values={doneSeries(state.recent)} label="Done per minute" recent={state.recent} /></ChartFrame>}
      />
      <Tile
        label="Action cache"
        value={cache.rate === null ? <span class="cache-rate__empty">no lookups</span> : <span class="cache-rate">{Math.round(cache.rate * 100)}%{" "}<span class="cache-rate__of">of {cache.lookups} lookups</span></span>}
        meter={cas && <Meter g={cas.budgetBytes > 0 ? cas.usedBytes / cas.budgetBytes : 0} label={`CAS ${percent(cas.usedBytes, cas.budgetBytes)} of budget`} />}
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
  const evicted = sum(samples, (s) => s.evictedBytes);
  return (
    <Section id="cache-h" title="Cache">
      {stores.length > 0 && (
        <Table cls="cache-table" cols={[{ head: "Store", share: 1 }, { head: "Used" }, { head: "Budget" }, { head: "Usage" }, { head: "Evicted (window, all stores)" }]}>
          {stores.map((store, index) => (
            <Row key={store.store}>
              <Cell><span class="build-label">Store </span>{store.store}</Cell><Cell><span class="build-label">Used </span>{bytes(store.usedBytes)}</Cell><Cell><span class="build-label">Budget </span>{bytes(store.budgetBytes)}</Cell>
              <Cell><span class="build-label">Usage </span><Meter g={store.budgetBytes > 0 ? store.usedBytes / store.budgetBytes : 0} label={`${store.store} ${percent(store.usedBytes, store.budgetBytes)} of budget`} /></Cell>
              <Cell><span class="build-label">Evicted (window, all stores) </span>{index === 0 ? bytes(evicted) : "—"}</Cell>
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
          <Spark values={evictedSeries(state.recent)} label="Evicted per step" recent={state.recent} />
        </ChartFrame>
      </Columns>
    </Section>
  );
};

const FleetRow = ({ f, ordinal, events, census }: { f: Fleet; ordinal: number; events: readonly ScalingEvent[]; census: FleetCensus }) => {
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
        <Tape f={f} census={census} />
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
  const idle = state.workers.length === 0 && state.queue.total === 0;
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
            <FleetRow key={f.name} f={f} ordinal={i + 1} events={state.events} census={state} />
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
