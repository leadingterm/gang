// Builds observed in the recent window, and older builds still held by the server.
import { useEffect, useState } from "preact/hooks";
import { bytes, percent } from "../format.ts";
import { identityOf, ms, seconds, splitIdentity } from "../wire-parse.ts";
import type { ActionTime, Build } from "../wire.ts";
import { Cell, Clock, Duration, Empty, Ident, Num, Row, Section, Since, Table, useClient } from "../ui.tsx";
import { Forbidden } from "./gate.tsx";
import type { Link } from "../client.ts";

export const actionSeconds = (time: ActionTime): number =>
  seconds(time.queued) + seconds(time.inputFetch) + seconds(time.executing) + seconds(time.outputUpload);

/** The client-side build filters, applied to both live and held rows. */
export const filterBuilds = (builds: readonly Build[], mine: boolean, who: string | undefined, principal: string, invocation: string): Build[] => {
  const principalQuery = principal.trim().toLowerCase();
  const invocationQuery = invocation.trim().toLowerCase();
  return builds.filter((b) =>
    (!mine || b.principal === who) &&
    identityOf(b.principal).toLowerCase().includes(principalQuery) &&
    b.invocationId.toLowerCase().includes(invocationQuery),
  ).sort((a, b) => ms(b.lastActiveTime) - ms(a.lastActiveTime) || a.name.localeCompare(b.name));
};

/** A build-only session has no state stream, so load held builds on entry. */
export const autoLoadBuilds = (permitted: boolean, link: Link, loaded: boolean): boolean =>
  permitted && link.kind === "forbidden" && !loaded;

const BuildRow = ({ build }: { build: Build }) => {
  const cache = build.usage.cache;
  const time = build.usage.actionTime;
  const identity = identityOf(build.principal);
  const short = splitIdentity(identity)[1] || identity;
  return (
    <Row>
      <Cell cls="a-main"><a class="line" href={`/ui/builds/${encodeURIComponent(build.name.slice("builds/".length))}`}><Ident text={short} title={identity} /></a></Cell>
      <Cell cls="a-kind"><span class="build-label">Started </span><Since t={ms(build.startTime)} /></Cell>
      <Cell cls="a-age"><span class="build-label">Span </span><Duration ms={ms(build.lastActiveTime) - ms(build.startTime)} /></Cell>
      <Cell cls="a-m1"><span class="build-label">State </span><span class={build.state === "IDLE" ? "tag" : "tag tag--spot"}>{build.state}</span></Cell>
      <Cell cls="a-m2"><span class="build-label">Remote cache </span>{percent(cache.hits, cache.hits + cache.misses + cache.errors)}</Cell>
      <Cell cls="a-span"><span class="build-label">Transferred </span>↓ {bytes(cache.downloadedBytes)} ↑ {bytes(cache.uploadedBytes)}</Cell>
      <Cell><span class="build-label">Executed </span>{build.usage.executions.completed + build.usage.executions.failed}</Cell>
      <Cell><span class="build-label">Queued share </span>{percent(seconds(time.queued), actionSeconds(time))}</Cell>
    </Row>
  );
};

const BuildTable = ({ builds }: { builds: readonly Build[] }) => (
  <Table cls="builds-table" cols={[
    { head: "Principal", share: 1 },
    { head: "Started" },
    { head: "Span" },
    { head: "State" },
    { head: "Remote cache" },
    { head: "Transferred" },
    { head: "Executed" },
    { head: "Queued share" },
  ]}>
    {builds.map((build) => <BuildRow key={build.name} build={build} />)}
  </Table>
);

export const Builds = () => {
  const client = useClient();
  const [mine, setMine] = useState(false);
  const [principal, setPrincipal] = useState("");
  const [invocation, setInvocation] = useState("");
  const permitted = client.session.value?.permissions.includes("builds.read") ?? false;
  const state = client.state.value;
  const earlier = client.earlierBuilds.value;
  const link = client.link.value;
  useEffect(() => {
    if (autoLoadBuilds(permitted, link, earlier.kind !== "unloaded" || earlier.loading)) void client.loadEarlierBuilds();
  }, [client, permitted, link.kind, earlier.kind]);
  if (!permitted) return <Forbidden permission="builds.read" />;
  if (!state && link.kind !== "forbidden") return null;
  const live = state?.builds ?? [];
  const liveNames = new Set(live.map((build) => build.name));
  const held = earlier.kind === "loaded" ? earlier.builds.filter((build) => !liveNames.has(build.name)) : [];
  const who = client.session.value?.principal;
  const builds = filterBuilds(live, mine, who, principal, invocation);
  const older = filterBuilds(held, mine, who, principal, invocation);
  const more = earlier.kind !== "loaded" || earlier.next !== undefined;
  const empty = state || earlier.kind === "loaded"
    ? <Empty><div><h2>No builds</h2><p>Builds appear when Bazel sends remote calls.</p></div></Empty>
    : earlier.kind === "error" && !earlier.loading
      ? <Empty><h2>Could not load builds</h2></Empty>
      : <Empty><h2>Loading builds</h2></Empty>;
  return (
    <>
      {earlier.kind === "loaded" && earlier.failing && <p class="caution" role="status">Could not refresh builds; showing them as of <Clock t={earlier.asOf} /></p>}
      <div class="toolbar" role="search">
        <label><input type="checkbox" checked={mine} onChange={(e) => setMine(e.currentTarget.checked)} /> Mine</label>
        <label class="field field--grow">
          <span class="visually-hidden">Principal</span>
          <input class="input" type="search" placeholder="Principal" value={principal} onInput={(e) => setPrincipal(e.currentTarget.value)} />
        </label>
        <label class="field field--grow">
          <span class="visually-hidden">Invocation id</span>
          <input class="input" type="search" placeholder="Invocation id" value={invocation} onInput={(e) => setInvocation(e.currentTarget.value)} />
        </label>
      </div>
      {builds.length === 0 && older.length === 0 ? (
        empty
      ) : (
        <>
          {builds.length > 0 && <Section id="builds-h" title="Builds" count={<Num value={builds.length} />}><BuildTable builds={builds} /></Section>}
          {older.length > 0 && <Section id="older-builds-h" title={state ? "Earlier builds" : "Builds"} count={<Num value={older.length} />}><BuildTable builds={older} /></Section>}
        </>
      )}
      <div class="listfoot">
        <button class="btn" type="button" aria-busy={earlier.loading} disabled={!more} onClick={() => void client.loadEarlierBuilds()}>
          {more ? "Load earlier" : "Nothing earlier"}
        </button>
      </div>
    </>
  );
};
