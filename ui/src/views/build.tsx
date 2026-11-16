// One build's full, in-memory view, refreshed by its streamed row or the build-only refresh.
import { useEffect } from "preact/hooks";
import { clock, duration, percent } from "../format.ts";
import { identityOf, ms, seconds } from "../wire-parse.ts";
import type { Build as BuildData, ScalingEvent, State } from "../wire.ts";
import { Empty, Num, Section, Target, useClient } from "../ui.tsx";
import { actionSeconds } from "./builds.tsx";
import { Forbidden } from "./gate.tsx";

const WIDTH = 600;
const HEIGHT = 120;
const BAR_HEIGHT = 20;
const CACHE_HEIGHT = 24;

type Phase = readonly [label: string, seconds: number];

const TimeBar = ({ build }: { build: BuildData }) => {
  const time = build.usage.actionTime;
  const total = actionSeconds(time);
  const parts: readonly Phase[] = [
    ["Queued", seconds(time.queued)],
    ["Input fetch", seconds(time.inputFetch)],
    ["Executing", seconds(time.executing)],
    ["Output upload", seconds(time.outputUpload)],
  ];
  let offset = 0;
  return (
    <Section id="build-time" title="Where the time went">
      <svg class="build-time" viewBox={`0 0 ${WIDTH} ${BAR_HEIGHT}`} preserveAspectRatio="none" role="img" aria-label="Action time by phase">
        {parts.map(([label, value], i) => {
          const x = offset;
          const width = total ? value / total * WIDTH : 0;
          offset += width;
          return (
            <rect key={label} class={`build-time__part build-time__part--${i}`} x={x} y="0" width={width} height={BAR_HEIGHT}>
              <title>{label}: {duration(value * 1000)}</title>
            </rect>
          );
        })}
      </svg>
      <p>{parts.map(([label, value]) => `${label} ${duration(value * 1000)}`).join(" · ")}</p>
      <p class="dim">Remote work Gang saw; local actions, disk-cache hits and analysis are not included.</p>
    </Section>
  );
};

const BuildTimeline = ({ build, events }: { build: BuildData; events: ScalingEvent[] }) => {
  const timeline = build.timeline;
  const buckets = timeline?.buckets ?? [];
  const first = buckets[0];
  const last = buckets.at(-1);
  if (!timeline || !first || !last) return <p class="dim">No timeline recorded.</p>;
  const step = seconds(timeline.step);
  const peak = Math.max(1, ...buckets.map((b) => Math.max(seconds(b.executing), seconds(b.queued)) / step));
  const countPeak = Math.max(1, ...buckets.map((b) => b.hits + b.misses));
  const x = (i: number): number => (i + 0.5) * WIDTH / buckets.length;
  const point = (value: number, i: number): string => `${x(i)},${HEIGHT - value / step / peak * HEIGHT}`;
  const start = ms(timeline.startTime);
  const span = buckets.length * step * 1000;
  const marks = events.filter((e) => ms(e.createTime) >= ms(build.startTime) && ms(e.createTime) <= ms(build.lastActiveTime));
  return (
    <>
      <svg class="build-chart" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none" role="img" aria-label="Mean executing and queued actions per timeline bucket">
        <polyline class="build-chart__executing" points={[point(seconds(first.executing), -0.5), ...buckets.map((b, i) => point(seconds(b.executing), i)), point(seconds(last.executing), buckets.length - 0.5)].join(" ")} />
        <polyline class="build-chart__queued" points={[point(seconds(first.queued), -0.5), ...buckets.map((b, i) => point(seconds(b.queued), i)), point(seconds(last.queued), buckets.length - 0.5)].join(" ")} />
        {marks.map((e) => (
          <line key={e.name} class="build-chart__event" x1={(ms(e.createTime) - start) / span * WIDTH} x2={(ms(e.createTime) - start) / span * WIDTH} y1="0" y2={HEIGHT}>
            <title>{e.name}</title>
          </line>
        ))}
      </svg>
      <svg class="build-cache-strip" viewBox={`0 0 ${WIDTH} ${CACHE_HEIGHT}`} preserveAspectRatio="none" role="img" aria-label="Cache hits and misses per timeline bucket">
        {buckets.map((b, i) => {
          const hitHeight = b.hits / countPeak * CACHE_HEIGHT;
          const missHeight = b.misses / countPeak * CACHE_HEIGHT;
          const left = i * WIDTH / buckets.length;
          const width = WIDTH / buckets.length;
          return (
            <g key={i}>
              <rect class="build-chart__hit" x={left} y={CACHE_HEIGHT - hitHeight} width={width} height={hitHeight}>
                <title>{b.hits} hits</title>
              </rect>
              <rect class="build-chart__miss" x={left} y={CACHE_HEIGHT - hitHeight - missHeight} width={width} height={missHeight}>
                <title>{b.misses} misses</title>
              </rect>
            </g>
          );
        })}
      </svg>
      <div class="passbar__axis"><span>{clock(start)} UTC</span><span>{clock(start + span)} UTC</span></div>
      <div class="legend" aria-label="Timeline legend">
        <span class="legend__item"><i class="build-swatch build-swatch--executing" /> executing</span>
        <span class="legend__item"><i class="build-swatch build-swatch--queued" /> queued</span>
        <span class="legend__item"><i class="build-swatch build-swatch--hit" /> hits</span>
        <span class="legend__item"><i class="build-swatch build-swatch--miss" /> misses</span>
        <span class="legend__item"><i class="build-swatch build-swatch--event" /> scaling event</span>
      </div>
    </>
  );
};

const CacheCard = ({ build }: { build: BuildData }) => {
  const cache = build.usage.cache;
  const mnemonics = (build.mnemonics ?? []).filter((m) => m.usage.cache.misses > 0);
  const other = build.otherMnemonics?.cache.misses ?? 0;
  const targets = build.missedTargets ?? [];
  return (
    <Section id="build-cache" title="Remote cache">
      <p>Hit rate <strong>{percent(cache.hits, cache.hits + cache.misses + cache.errors)}</strong> · {cache.hits} hits · {cache.misses} misses · {cache.errors} errors</p>
      <h3 class="label">Misses by mnemonic</h3>
      {mnemonics.length === 0 && other === 0 ? <p class="dim">No mnemonic misses recorded.</p> : (
        <table class="table build-counts">
          <tbody>
            {mnemonics.map((m) => (
              <tr key={m.mnemonic}><td>{m.mnemonic}</td><td class="r">{m.usage.cache.misses}</td></tr>
            ))}
            {other > 0 && <tr><td>Other</td><td class="r">{other}</td></tr>}
          </tbody>
        </table>
      )}
      <h3 class="label">Most-missed targets</h3>
      {targets.length === 0 ? <p class="dim">No missed targets recorded.</p> : (
        <table class="table build-counts">
          <tbody>
            {targets.map((t) => (
              <tr key={t.target}><td><Target label={t.target} /></td><td class="r">{t.overestimate > 0 && "≈"}{t.misses}</td></tr>
            ))}
          </tbody>
        </table>
      )}
    </Section>
  );
};

const ExecutionsCard = ({ build }: { build: BuildData }) => {
  const execution = build.usage.executions;
  return (
    <Section id="build-executions" title="Executions">
      <div class="readouts">
        {([
          ["Execute calls", execution.executeCalls],
          ["Completed", execution.completed],
          ["Failed", execution.failed],
          ["Joined", Math.max(0, execution.executeCalls - execution.completed - execution.failed)],
          ["Rejected", execution.rejected],
        ] as const).map(([label, value]) => (
          <div class="readout" key={label}>
            <h3 class="label">{label}</h3>
            <p class="readout__big"><Num value={value} /></p>
          </div>
        ))}
      </div>
    </Section>
  );
};

const SlowestTable = ({ build }: { build: BuildData }) => (
  <Section id="build-slowest" title="Slowest actions">
    {(build.slowest ?? []).length === 0 ? <p class="dim">No completed actions recorded.</p> : (
      <table class="table">
        <thead><tr><th>Target</th><th>Mnemonic</th><th>Worker</th><th>Span</th><th>Exit</th></tr></thead>
        <tbody>
          {(build.slowest ?? []).map((a, i) => (
            <tr key={i}>
              <td class="a-main"><Target label={a.target || a.digest} /></td>
              <td class="a-kind">{a.mnemonic || "—"}</td>
              <td class="a-m1">{a.worker || "—"}</td>
              <td class="a-age">{duration(seconds(a.span) * 1000)}</td>
              <td class="a-m2">{a.exitCode}</td>
            </tr>
          ))}
        </tbody>
      </table>
    )}
  </Section>
);

const LiveActions = ({ build, state }: { build: BuildData; state: State | null }) => {
  const running = state?.actions?.running.filter((a) => a.build === build.name) ?? [];
  const queued = state?.actions?.queued.filter((a) => a.build === build.name) ?? [];
  return (
    <Section id="build-actions" title="Running and queued">
      <h3 class="label">Running</h3>
      {running.length ? <ul>{running.map((a) => <li key={a.name}><Target label={a.target || a.digest} /> · {a.mnemonic}</li>)}</ul> : <p class="dim">None sampled</p>}
      <h3 class="label">Queued</h3>
      {queued.length ? <ul>{queued.map((a) => <li key={a.name}><Target label={a.target || a.digest} /> · {a.mnemonic}</li>)}</ul> : <p class="dim">None sampled</p>}
    </Section>
  );
};

export const BuildView = ({ build }: { build: BuildData }) => {
  const state = useClient().state.value;
  return (
    <>
      <Section id="build-header" title={identityOf(build.principal)}>
        <div class="readouts build-header">
          <div class="readout">
            <h3 class="label">Invocation</h3>
            <p class="mono">{build.invocationId} <button class="btn btn--quiet" type="button" onClick={() => void navigator.clipboard.writeText(build.invocationId)}>Copy</button></p>
          </div>
          <div class="readout"><h3 class="label">Tool</h3><p>{build.tool || "Unknown tool"}</p></div>
          <div class="readout"><h3 class="label">Span</h3><p class="mono">{duration(ms(build.lastActiveTime) - ms(build.startTime))}</p></div>
          <div class="readout"><h3 class="label">State</h3><p><span class={build.state === "IDLE" ? "tag" : "tag tag--spot"}>{build.state}</span></p></div>
        </div>
        {build.state === "IDLE" && <p class="dim">No observation for a while; Gang cannot know if it finished.</p>}
        {build.unrecorded > 0 && <p class="caution">{build.unrecorded} observations not recorded</p>}
      </Section>
      <TimeBar build={build} />
      <Section id="build-timeline" title="Timeline">
        <BuildTimeline build={build} events={state?.events ?? []} />
      </Section>
      <CacheCard build={build} />
      <ExecutionsCard build={build} />
      <SlowestTable build={build} />
      {build.state === "ACTIVE" && <LiveActions build={build} state={state} />}
    </>
  );
};

export const Build = ({ id }: { id: string }) => {
  const client = useClient();
  const permitted = client.session.value?.permissions.includes("builds.read") ?? false;
  const row = client.state.value?.builds?.find((b) => b.name === `builds/${id}`);
  useEffect(() => {
    if (permitted) void client.loadBuild(id);
  }, [client, id, permitted, row]);
  useEffect(() => () => client.closeBuild(), [client]);
  if (!permitted) return <Forbidden permission="builds.read" />;
  const detail = client.build.value;
  if (!detail || detail.id !== id || detail.kind === "loading") return <Empty><h2>Loading build</h2></Empty>;
  if (detail.kind === "missing") return <Empty><h2>build not held: only the latest builds are kept in memory</h2></Empty>;
  if (detail.kind === "error") return <Empty><h2>Could not load build</h2></Empty>;
  return (
    <>
      {detail.failing && <p class="caution" role="status">Could not refresh this build; showing it as of {clock(detail.asOf)}</p>}
      <BuildView build={detail.build} />
    </>
  );
};
