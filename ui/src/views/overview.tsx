// Overview: four readouts, the fleets in preference order, the control pass and recent scaling.
import { eventKind, idOf, ms, seconds, type UnixMs } from "../wire-parse.ts";
import type { Cause, Fleet, Pass, ScalingEvent, State } from "../wire.ts";
import { donePerMinute, PASS_BARS, passBars, type History, type Sample } from "../history.ts";
import { clock, duration, humanize, sum } from "../format.ts";
import { Empty, EVENT_VIEW, EventWhat, Glyph, Num, Section, Since, useClient, useEnter } from "../ui.tsx";
import { Legend, Tape } from "./fleet.tsx";

/** Scaling events the Overview shows, however old. */
const RECENT_EVENTS = 6;

const SPARK_W = 300;
const SPARK_H = 44;

const sparkPath = (samples: readonly Sample[], from: UnixMs, to: UnixMs): string => {
  const peak = Math.max(1, ...samples.map((s) => s.running));
  const span = Math.max(1, to - from);
  return samples
    .map((s, i) => {
      const x = ((s.at - from) / span) * SPARK_W;
      const y = SPARK_H - (s.running / peak) * (SPARK_H - 2) - 1;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
};

const Spark = ({ samples }: { samples: readonly Sample[] }) => {
  const first = samples[0];
  const last = samples.at(-1);
  if (!first || !last || samples.length < 2) return <svg class="spark" viewBox={`0 0 ${SPARK_W} ${SPARK_H}`} aria-hidden="true" />;
  const run = sparkPath(samples, first.at, last.at);
  return (
    <svg class="spark" viewBox={`0 0 ${SPARK_W} ${SPARK_H}`} preserveAspectRatio="none" role="img" aria-label="Executing actions">
      <path class="spark__area" d={`${run} L${SPARK_W} ${SPARK_H} L0 ${SPARK_H} Z`} />
      <path class="spark__run" d={run} />
    </svg>
  );
};

const Meter = ({ f, g, label }: { f?: number; g: number; label?: string }) => (
  <div class="meter" {...(label ? { role: "img", "aria-label": label } : { "aria-hidden": "true" })}>
    {f !== undefined && <i class="meter__fill" style={{ "--f": String(f) }} />}
    <i class="meter__fill meter__fill--b" style={{ "--g": String(g) }} />
  </div>
);

const Readouts = ({ state, history }: { state: State; history: History | null }) => {
  const running = sum(state.fleets, (f) => f.demand.running);
  const slots = sum(state.workers, (w) => w.slots ?? 0);
  const inService = sum(state.fleets, (f) => f.instances.inService);
  const max = sum(state.fleets, (f) => f.policy.max);
  const spot = sum(
    state.fleets.filter((f) => f.properties["capacity"] === "spot"),
    (f) => f.instances.inService,
  );
  const done = donePerMinute(history?.samples ?? []);
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
      </div>
      <div class="readout">
        <h2 class="label">Executing</h2>
        <p class="readout__big">
          <Num value={running} />
          <span class="readout__of">/{slots}</span>
        </p>
        <Meter g={slots ? running / slots : 0} />
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
        <p class="readout__big">{done === null ? <span class="num live">–</span> : <Num value={done} />}</p>
        <Spark samples={history?.samples ?? []} />
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
        <h3 class="fleet__name">{idOf(f.name)}</h3>
        {capacity && <span class={capacity === "spot" ? "tag tag--spot" : "tag"}>{capacity}</span>}
        {cold && (
          <span class="tag tag--note">
            starting <Since t={ms(cold.createTime)} />
          </span>
        )}
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

const PassBar = ({ pass, history }: { pass: Pass; history: History }) => {
  const bars = passBars(pass, history, useClient().now.value);
  const skipped = bars.filter((b) => b === "skipped").length;
  return (
    <>
      <div class="passbar" role="img" aria-label={`${skipped} of the last ${bars.length} passes skipped`}>
        {bars.map((b, i) => (
          <i key={i} class={b === "ok" ? undefined : `is-${b}`} />
        ))}
      </div>
      <div class="passbar__axis" aria-hidden="true">
        <span>−{duration(PASS_BARS * seconds(pass.interval) * 1000)}</span>
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
  const cause = e.cause === undefined ? undefined : (CAUSE_TEXT[e.cause] ?? humanize(e.cause));
  const kind = eventKind(e);
  return (
    <li ref={ref} class={withCause ? "event" : "event event--brief"}>
      <span class="event__time">{clock(ms(e.createTime))}</span>
      <Glyph kind={kind} cls="event__glyph" />
      <span class="visually-hidden">{kind ? EVENT_VIEW[kind].label : "Event"}</span>
      <span class="event__fleet">{idOf(e.fleet)}</span>
      <span class="event__what">
        <EventWhat e={e} />
      </span>
      {withCause && <span class={e.cause !== undefined && CAUTION.has(e.cause) ? "event__cause caution" : "event__cause"}>{cause ?? ""}</span>}
    </li>
  );
};

export const Overview = () => {
  const client = useClient();
  const state = client.state.value;
  const history = client.history.value;
  if (!state) return null;
  const recent = state.events.slice(-RECENT_EVENTS).reverse();
  const idle = state.workers.length === 0 && state.queue.total === 0;
  return (
    <>
      <Readouts state={state} history={history} />
      {idle && (
        <Empty>
          <h2>Idle · no workers, nothing queued</h2>
        </Empty>
      )}
      <Section id="fleets-h" title="Fleets" aside={<Legend />} asideCls="section__aside--wide">
        <ol class="fleets">
          {state.fleets.map((f, i) => (
            <FleetRow key={f.name} f={f} ordinal={i + 1} events={state.events} />
          ))}
        </ol>
      </Section>
      <div class="split">
        <Section id="pass-h" title="Control pass">
          {history && <PassBar pass={state.pass} history={history} />}
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
