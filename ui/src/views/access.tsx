// Access: the principals the server has seen, the caller's own session, and the configured roles.
import { useEffect, useState } from "preact/hooks";
import { identityOf, idOf, ms, splitIdentity } from "../wire-parse.ts";
import type { Principal, Session } from "../wire.ts";
import { Cell, Clock, Day, Ident, Row, Section, Since, Table, Tags, Until, useClient } from "../ui.tsx";

const DAY_MS = 24 * 3600 * 1000;

const PrincipalRow = ({ p, now }: { p: Principal; now: number }) => {
  const lastSeen = ms(p.lastSeenTime);
  return (
    <Row cls={p.revokeTime !== undefined ? "is-revoked" : undefined}>
      <Cell cls="a-kind mono dim"><Ident text={p.provider} /></Cell>
      <Cell cls="a-main">
        <span class="principal-id">{p.principal}</span>
      </Cell>
      <Cell cls="a-m1 dim"><Day t={ms(p.createTime)} /></Cell>
      <Cell cls="a-m2">{now - lastSeen < DAY_MS ? <Since t={lastSeen} /> : <Day t={lastSeen} />}</Cell>
      <Cell cls="a-age r">
        {p.revokeTime !== undefined && <span class="status-revoked">revoked <Day t={ms(p.revokeTime)} /></span>}
      </Cell>
    </Row>
  );
};

const You = ({ session }: { session: Session }) => {
  const who = session.principal === undefined ? undefined : splitIdentity(identityOf(session.principal));
  return (
    <Section
      id="se-h"
      title="You"
      variant="panel"
      aside={session.expireTime === undefined ? undefined : <span class="mono quiet"><Until t={ms(session.expireTime)} /></span>}
    >
      {who && (
        <p class="you line">
          <Ident text={who[1]} />{" "}<Ident text={who[0]} cls="quiet" />
        </p>
      )}
      <p>
        <Tags items={session.permissions} />
      </p>
    </Section>
  );
};

type Show = "all" | "revoked";

export const Access = () => {
  const client = useClient();
  const session = client.session.value;
  const resource = client.access.value;
  const access = resource.kind === "loaded" ? resource.data : null;
  const epoch = client.state.value?.principalsEpoch;
  const allowed = session?.permissions.includes("principals.read") ?? false;
  const [show, setShow] = useState<Show>("all");
  const [query, setQuery] = useState("");
  useEffect(() => {
    if (allowed) void client.loadAccess();
  }, [allowed, epoch]);
  if (!session) return null;
  const principals = access?.principals ?? [];
  const revoked = access?.revoked ?? [];
  const revokedNames = new Set(revoked.map((p) => p.name));
  const q = query.trim().toLowerCase();
  const rows = (show === "revoked" ? revoked : principals).filter(
    (p) => q === "" || `${p.provider} ${p.principal}`.toLowerCase().includes(q),
  );
  const now = client.now.peek();
  return (
    <div class="cols">
      {allowed && resource.kind !== "loaded" && (
        <div class="cols__wide">
          {(resource.kind === "unloaded" || resource.kind === "loading") && (
            <p role="status">Loading access observations…</p>
          )}
          {resource.kind === "error" && (
            <div class="caution" role="alert">
              Could not load access observations. <button class="btn" type="button" onClick={() => void client.loadAccess()}>Retry</button>
            </div>
          )}
        </div>
      )}
      {allowed && resource.kind === "loaded" && (
        <Section id="pr-h" title="Principals" variant="plain" count={principals.length} cls="cols__wide">
          {!resource.failing && <p>As of <Clock t={resource.asOf} /></p>}
          {resource.loading && <p role="status">Refreshing access observations…</p>}
          {resource.failing && (
            <div class="caution" role="alert">
              Could not refresh access observations; showing observations as of <Clock t={resource.asOf} />.{" "}
              <button class="btn" type="button" onClick={() => void client.loadAccess()}>Retry</button>
            </div>
          )}
          <div class="toolbar">
            <div class="seg" role="radiogroup" aria-label="Show">
              <button class="seg__opt" type="button" role="radio" aria-checked={show === "all"} onClick={() => setShow("all")}>
                All
              </button>
              <button
                class="seg__opt"
                type="button"
                role="radio"
                aria-checked={show === "revoked"}
                onClick={() => setShow("revoked")}
              >
                Revoked <span class="num">{revoked.length}</span>
              </button>
            </div>
            <div class="field field--grow">
              <label class="visually-hidden" for="pq">
                Filter principals
              </label>
              <input
                class="input"
                id="pq"
                type="search"
                placeholder="repo:example/monorepo"
                autocomplete="off"
                spellcheck={false}
                value={query}
                onInput={(e) => setQuery(e.currentTarget.value)}
              />
            </div>
          </div>
          <Table cols={[
            { head: "Provider", fit: 12 },
            { head: "Principal", share: 1 },
            { head: "First seen" },
            { head: "Last seen" },
            { head: <span class="visually-hidden">Revoked</span>, end: true },
          ]}>
            {rows.map((p) => (
              <PrincipalRow key={p.name} p={p} now={now} />
            ))}
          </Table>
          {resource.moreFailed && <div class="caution" role="alert">Could not load more principals.</div>}
          {access?.nextPageToken !== undefined && (
            <div class="listfoot">
              <button class="btn" type="button" disabled={resource.loading || resource.moreLoading} onClick={() => void client.loadMorePrincipals()}>
                {resource.moreFailed ? "Retry more" : "More"}
              </button>
            </div>
          )}
        </Section>
      )}
      <aside>
        <You session={session} />
        {allowed && access && (
          <Section id="ro-h" title="Roles" variant="panel">
            {access.roles.map((r) => (
              <div key={r.name} class="role">
                <h3><Ident text={idOf(r.name)} /></h3>
                <p class="role__list">
                  <Tags items={r.permissions} />
                </p>
                <ul class="role__list">
                  {r.principals.map((p) => (
                    <li key={p} class="line">
                      <Ident text={identityOf(p)} />
                      {revokedNames.has(p) && <>{" "}<span class="status-revoked">revoked</span></>}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </Section>
        )}
      </aside>
    </div>
  );
};
