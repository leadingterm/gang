// Activity: scaling events as one lane per fleet over the last hours, and a log of every event the
// client holds, newest first, with older pages loaded on request.
import { Fragment } from "preact";
import { useState } from "preact/hooks";
import { earlierMore } from "../client.ts";
import { EVENT_KINDS, eventKind, idOf, ms, type EventKind, type UnixMs } from "../wire-parse.ts";
import type { ScalingEvent } from "../wire.ts";
import { clock, day } from "../format.ts";
import { Empty, EVENT_VIEW, FleetFilter, Glyph, Section, useClient, useNowStep } from "../ui.tsx";
import { EventRow } from "./overview.tsx";

/** The lanes' span. */
const LANES_MS = 6 * 3600 * 1000;
const HOURS = LANES_MS / 3600000;
/** Marks move at this resolution. */
const STEP_MS = 60 * 1000;

/** Newest hour first; an hour of another UTC day carries its date. */
const byHour = (events: readonly ScalingEvent[], now: UnixMs): [string, string, ScalingEvent[]][] => {
  const hours = Map.groupBy(events.toReversed(), (e) => new Date(ms(e.createTime)).toISOString().slice(0, 13));
  const today = new Date(now).toISOString().slice(0, 10);
  return [...hours].map(([hour, entries]) => {
    const first = entries[0];
    const at = first ? ms(first.createTime) : now;
    const label = hour.startsWith(today) ? `${clock(at).slice(0, 2)}:00` : `${day(at)} ${clock(at).slice(0, 2)}:00`;
    return [hour, label, entries];
  });
};

/** Older pages come from `GET /v1/events`, which needs `events.read`. */
const LoadEarlier = () => {
  const client = useClient();
  const earlier = client.earlier.value;
  const { loading } = earlier;
  const more = earlierMore(earlier);
  if (!client.session.value?.permissions.includes("events.read")) return null;
  return (
    <div class="listfoot">
      <button
        class="btn"
        type="button"
        aria-busy={loading}
        disabled={!more}
        onClick={() => void client.loadEarlier()}
      >
        {more ? "Load earlier" : "Nothing earlier"}
      </button>
    </div>
  );
};

export const Activity = () => {
  const client = useClient();
  const state = client.state.value;
  const earlier = client.earlier.value;
  const [hidden, setHidden] = useState<ReadonlySet<EventKind>>(new Set());
  const [fleet, setFleet] = useState("");
  const now = useNowStep(STEP_MS);
  if (!state) return null;
  const start = now - LANES_MS;
  const all = [...(earlier.kind === "loaded" ? earlier.events : []), ...state.events];
  if (all.length === 0) {
    return (
      <>
        <Empty>
          <h2>No scaling events</h2>
        </Empty>
        <LoadEarlier />
      </>
    );
  }
  const isHidden = (e: ScalingEvent): boolean => {
    const kind = eventKind(e);
    return kind !== undefined && hidden.has(kind);
  };
  const recent = all.filter((e) => ms(e.createTime) >= start);
  const shown = all.filter((e) => !isHidden(e) && (fleet === "" || e.fleet === fleet));
  const toggle = (kind: EventKind): void => {
    const next = new Set(hidden);
    if (!next.delete(kind)) next.add(kind);
    setHidden(next);
  };
  return (
    <>
      <div class="toolbar">
        <div class="seg" role="group" aria-label="Event kinds">
          {EVENT_KINDS.map((kind) => (
            <button key={kind} class="seg__opt" type="button" aria-pressed={!hidden.has(kind)} onClick={() => toggle(kind)}>
              <Glyph kind={kind} cls="event__glyph" />
              {EVENT_VIEW[kind].label} <span class="num">{all.filter((e) => eventKind(e) === kind).length}</span>
            </button>
          ))}
        </div>
        <FleetFilter id="afleet" fleets={state.fleets} value={fleet} onChange={setFleet} />
      </div>
      <section aria-label={`Events by fleet over the last ${HOURS} hours`}>
        <div class="lanes">
          {state.fleets.map((f) => (
            <button
              key={f.name}
              class="lanes__row"
              type="button"
              aria-pressed={fleet === f.name}
              onClick={() => setFleet(fleet === f.name ? "" : f.name)}
            >
              <span class="lanes__name">
                {idOf(f.name)}
                <span class="visually-hidden">: show only this fleet's events</span>
              </span>
              <span class="lanes__track" aria-hidden="true">
                {recent
                  .filter((e) => e.fleet === f.name)
                  .map((e) => (
                    <span
                      key={e.name}
                      class="mark"
                      hidden={isHidden(e)}
                      style={{ "--x": ((ms(e.createTime) - start) / LANES_MS).toFixed(4) }}
                    >
                      <Glyph kind={eventKind(e)} cls="" />
                    </span>
                  ))}
              </span>
            </button>
          ))}
          <div class="lanes__axis axis">
            {Array.from({ length: HOURS + 1 }, (_, i) => (
              <span key={i}>{i === HOURS ? "now" : `−${HOURS - i}h`}</span>
            ))}
          </div>
        </div>
      </section>
      <Section id="log-h" title="Log" count={shown.length}>
        {byHour(shown, now).map(([hour, label, entries]) => (
          <Fragment key={hour}>
            <h3 class="hour">{label}</h3>
            <ol class="events">
              {entries.map((e) => (
                <EventRow key={e.name} e={e} withCause />
              ))}
            </ol>
          </Fragment>
        ))}
        <LoadEarlier />
      </Section>
    </>
  );
};
