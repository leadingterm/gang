// Actions: what runs where, and what waits, filtered by target, mnemonic and fleet.
import { useComputed } from "@preact/signals";
import { useState } from "preact/hooks";
import { idOf, ms } from "../wire-parse.ts";
import type { Actions as ActionsSample, QueuedAction, RunningAction } from "../wire.ts";
import { clock, workerInstance } from "../format.ts";
import { actionsStale } from "../store.ts";
import { Empty, FleetFilter, Num, Priority, Section, Since, Tags, Target, useClient, useEnter } from "../ui.tsx";

/** Queued longer than this, the wait reads as caution. */
const AGED_MS = 3 * 60 * 1000;

/** The sample time, shown only while the sample is stale. */
export const AsOf = ({ actions }: { actions: ActionsSample }) => {
  const client = useClient();
  const stale = useComputed(() => actionsStale(actions, client.now.value)).value;
  return <span class={stale ? "asof is-on" : "asof"}>AS OF {clock(ms(actions.sampleTime))}</span>;
};

const matches = (text: string, query: string): boolean => query === "" || text.toLowerCase().includes(query);

const RunningRow = ({ a }: { a: RunningAction }) => {
  const ref = useEnter<HTMLTableRowElement>();
  return (
    <tr ref={ref}>
      <td class="a-main">
        <Target label={a.target} />
        <Priority p={a.priority} />
      </td>
      <td class="a-kind">{a.mnemonic}</td>
      <td class="a-m1 mono dim">{idOf(a.fleet)}</td>
      <td class="a-m2 mono dim">{workerInstance(idOf(a.worker))}</td>
      <td class="a-age r">
        <Since t={ms(a.startTime)} />
      </td>
    </tr>
  );
};

const QueuedRow = ({ q }: { q: QueuedAction }) => {
  const client = useClient();
  const aged = useComputed(() => client.now.value - ms(q.queueTime) > AGED_MS).value;
  const ref = useEnter<HTMLTableRowElement>();
  return (
    <tr ref={ref}>
      <td class="a-age">
        <Since t={ms(q.queueTime)} cls={aged ? "caution" : ""} />
      </td>
      <td class="a-main">
        <Target label={q.target} />
        <Priority p={q.priority} />
      </td>
      <td class="a-kind">{q.mnemonic}</td>
      <td class="a-m1 a-span">{q.candidates.length ? <Tags items={q.candidates.map(idOf)} /> : <span class="warning">no fleet</span>}</td>
    </tr>
  );
};

export const Actions = () => {
  const client = useClient();
  const state = client.state.value;
  const [query, setQuery] = useState("");
  const [fleet, setFleet] = useState("");
  if (!state) return null;
  const actions = state.actions;
  // Whether work exists is the live state's call; the sample may lag it or not exist yet.
  const busy = state.queue.total > 0 || state.fleets.some((f) => f.demand.running > 0);
  const listed = actions !== undefined && (actions.running.length > 0 || actions.queued.length > 0);
  if (!busy && !listed) {
    return (
      <Empty>
        <h2>Nothing running or queued</h2>
      </Empty>
    );
  }
  if (!actions) {
    return (
      <Empty>
        <h2>No action sample yet</h2>
      </Empty>
    );
  }
  const q = query.trim().toLowerCase();
  const running = actions.running.filter(
    (a) => matches(`${a.target} ${a.mnemonic}`, q) && (fleet === "" || a.fleet === fleet),
  );
  const queued = actions.queued.filter(
    (a) => matches(`${a.target} ${a.mnemonic}`, q) && (fleet === "" || a.candidates.includes(fleet)),
  );
  return (
    <>
      <div class="toolbar" role="search">
        <div class="field field--grow">
          <label class="visually-hidden" for="q">
            Filter by target or mnemonic
          </label>
          <input
            class="input"
            id="q"
            type="search"
            placeholder="//target or mnemonic"
            autocomplete="off"
            spellcheck={false}
            value={query}
            onInput={(e) => setQuery(e.currentTarget.value)}
          />
        </div>
        <FleetFilter id="fleet" fleets={state.fleets} value={fleet} onChange={setFleet} />
        <AsOf actions={actions} />
      </div>
      {actions.running.length > 0 && (
        <Section
          id="run-h"
          title="Running"
          count={<Num value={actions.running.length} />}
          aside={actions.truncated.running > 0 ? `+${actions.truncated.running}` : undefined}
          asideCls="trunc"
        >
          <table class="table">
            <thead>
              <tr>
                <th>Target</th>
                <th>Mnemonic</th>
                <th>Fleet</th>
                <th>Worker</th>
                <th class="r">Time</th>
              </tr>
            </thead>
            <tbody>
              {running.map((a) => (
                <RunningRow key={a.name} a={a} />
              ))}
            </tbody>
          </table>
        </Section>
      )}
      {actions.queued.length > 0 && (
        <Section
          id="q-h"
          title="Queued"
          count={<Num value={state.queue.total} />}
          aside={actions.truncated.queued > 0 ? `+${actions.truncated.queued}` : undefined}
          asideCls="trunc"
        >
          <table class="table">
            <thead>
              <tr>
                <th>Wait</th>
                <th>Target</th>
                <th>Mnemonic</th>
                <th>Fleets</th>
              </tr>
            </thead>
            <tbody>
              {queued.map((a) => (
                <QueuedRow key={a.name} q={a} />
              ))}
            </tbody>
          </table>
        </Section>
      )}
    </>
  );
};
