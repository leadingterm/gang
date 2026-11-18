// Actions: what runs where, and what waits, filtered by target, mnemonic and fleet.
import { useComputed } from "@preact/signals";
import { useState } from "preact/hooks";
import { idOf, ms } from "../wire-parse.ts";
import type { Actions as ActionsSample, QueuedAction, RunningAction } from "../wire.ts";
import { workerInstance } from "../format.ts";
import { actionsStale } from "../store.ts";
import { Cell, Clock, Empty, FleetFilter, Ident, Num, Priority, Row, Section, Since, Table, Target, useClient } from "../ui.tsx";

/** Queued longer than this, the wait reads as caution. */
const AGED_MS = 3 * 60 * 1000;

/** The sample time, shown only while the sample is stale. */
export const AsOf = ({ actions }: { actions: ActionsSample }) => {
  const client = useClient();
  const stale = useComputed(() => actionsStale(actions, client.now.value)).value;
  return <span class={stale ? "asof is-on" : "asof"}>AS OF <Clock t={ms(actions.sampleTime)} /></span>;
};

const matches = (text: string, query: string): boolean => query === "" || text.toLowerCase().includes(query);

const RunningRow = ({ a }: { a: RunningAction }) => {
  return (
    <Row enter>
      <Cell cls="a-main">
        <div class="line"><Target label={a.target} />{" "}<Priority p={a.priority} /></div>
        {a.build && <a class="more line" href={`/ui/builds/${encodeURIComponent(idOf(a.build))}`}><span>Build</span>{" "}<Ident text={idOf(a.build)} /></a>}
      </Cell>
      <Cell cls="a-kind"><Ident text={a.mnemonic} /></Cell>
      <Cell cls="a-m1 mono dim"><Ident text={idOf(a.fleet)} /></Cell>
      <Cell cls="a-m2 mono dim"><Ident text={workerInstance(idOf(a.worker))} /></Cell>
      <Cell cls="a-age r">
        <Since t={ms(a.startTime)} />
      </Cell>
    </Row>
  );
};

const QueuedRow = ({ q }: { q: QueuedAction }) => {
  const client = useClient();
  const aged = useComputed(() => client.now.value - ms(q.queueTime) > AGED_MS).value;
  return (
    <Row enter>
      <Cell cls="a-age">
        <Since t={ms(q.queueTime)} cls={aged ? "caution" : ""} />
      </Cell>
      <Cell cls="a-main">
        <div class="line"><Target label={q.target} />{" "}<Priority p={q.priority} /></div>
        {q.build && <a class="more line" href={`/ui/builds/${encodeURIComponent(idOf(q.build))}`}><span>Build</span>{" "}<Ident text={idOf(q.build)} /></a>}
        {q.refusal && <div class="action__refusal caution">{q.refusal}</div>}
      </Cell>
      <Cell cls="a-kind"><Ident text={q.mnemonic} /></Cell>
      <Cell cls="a-m1 a-span">
        {q.candidates.length ? (
          <span class="chips">
            {q.candidates.map((candidate) => <span key={candidate} class="tag"><Ident text={idOf(candidate)} /></span>)}
          </span>
        ) : <span class="warning">no fleet</span>}
      </Cell>
    </Row>
  );
};

export const Actions = () => {
  const client = useClient();
  const state = client.state.value;
  const [query, setQuery] = useState("");
  const [fleet, setFleet] = useState("");
  const [build, setBuild] = useState("");
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
  const builds = [...new Set([...actions.running, ...actions.queued].flatMap((a) => a.build ? [a.build] : []).concat(build ? [build] : []))].sort();
  const running = actions.running.filter(
    (a) => matches(`${a.target} ${a.mnemonic}`, q) && (fleet === "" || a.fleet === fleet) && (build === "" || a.build === build),
  );
  const queued = actions.queued.filter(
    (a) => matches(`${a.target} ${a.mnemonic}`, q) && (fleet === "" || a.candidates.includes(fleet)) && (build === "" || a.build === build),
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
        <label class="field">
          <span class="visually-hidden">Filter by build</span>
          <select class="select" value={build} onChange={(e) => setBuild(e.currentTarget.value)}>
            <option value="">All builds</option>
            {builds.map((name) => <option key={name} value={name}>{idOf(name)}</option>)}
          </select>
        </label>
        <AsOf actions={actions} />
      </div>
      {actions.running.length > 0 && (
        <Section
          id="run-h"
          title="Running"
          count={<Num value={actions.running.length} />}
          aside={actions.truncated.running > 0 ? <span class="trunc">+{actions.truncated.running}</span> : undefined}
        >
          <Table cols={[
            { head: "Target", share: 1 },
            { head: "Mnemonic", fit: 10 },
            { head: "Fleet", fit: 20 },
            { head: "Worker", fit: 12 },
            { head: "Time", end: true },
          ]}>
            {running.map((a) => (
              <RunningRow key={a.name} a={a} />
            ))}
          </Table>
        </Section>
      )}
      {actions.queued.length > 0 && (
        <Section
          id="q-h"
          title="Queued"
          count={<Num value={state.queue.total} />}
          aside={actions.truncated.queued > 0 ? <span class="trunc">+{actions.truncated.queued}</span> : undefined}
        >
          <Table cols={[
            { head: "Wait" },
            { head: "Target", share: 1 },
            { head: "Mnemonic", fit: 10 },
            { head: "Fleets", fit: 24 },
          ]}>
            {queued.map((a) => (
              <QueuedRow key={a.name} q={a} />
            ))}
          </Table>
        </Section>
      )}
    </>
  );
};
