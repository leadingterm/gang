// Access: the principals the server has seen, the caller's own session, and the configured roles.
import { useEffect, useState } from "preact/hooks";
import { identityOf, idOf, ms, splitIdentity } from "../wire-parse.ts";
import type { Principal, Session } from "../wire.ts";
import { day } from "../format.ts";
import { Section, Since, Tags, Until, useClient } from "../ui.tsx";

const DAY_MS = 24 * 3600 * 1000;

const PrincipalRow = ({ p, now }: { p: Principal; now: number }) => {
  const lastSeen = ms(p.lastSeenTime);
  return (
    <tr class={p.revokeTime !== undefined ? "is-revoked" : undefined}>
      <td class="a-kind mono dim">{p.provider}</td>
      <td class="a-main">
        <span class="target">{p.principal}</span>
      </td>
      <td class="a-m1 dim">{day(ms(p.createTime))}</td>
      <td class="a-m2">{now - lastSeen < DAY_MS ? <Since t={lastSeen} /> : day(lastSeen)}</td>
      <td class="a-age r">
        {p.revokeTime !== undefined && <span class="status-revoked">revoked {day(ms(p.revokeTime))}</span>}
      </td>
    </tr>
  );
};

const You = ({ session }: { session: Session }) => {
  const who = session.principal === undefined ? undefined : splitIdentity(identityOf(session.principal));
  return (
    <Section
      id="se-h"
      title="You"
      variant="panel"
      aside={session.expireTime === undefined ? undefined : <Until t={ms(session.expireTime)} />}
      asideCls="mono quiet"
    >
      {who && (
        <p class="you">
          <span class="mono">{who[1]}</span> <span class="quiet">{who[0]}</span>
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
  const access = client.access.value;
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
    <div class="access">
      {allowed && (
        <Section id="pr-h" title="Principals" variant="plain" count={principals.length}>
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
          <table class="table">
            <thead>
              <tr>
                <th>Provider</th>
                <th>Principal</th>
                <th>First seen</th>
                <th>Last seen</th>
                <th class="r">
                  <span class="visually-hidden">Revoked</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((p) => (
                <PrincipalRow key={p.name} p={p} now={now} />
              ))}
            </tbody>
          </table>
          {access?.nextPageToken !== undefined && (
            <div class="listfoot">
              <button class="btn" type="button" onClick={() => void client.loadMorePrincipals()}>
                More
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
                <h3>{idOf(r.name)}</h3>
                <p class="role__list">
                  <Tags items={r.permissions} />
                </p>
                <ul class="role__list">
                  {r.principals.map((p) => (
                    <li key={p}>
                      {identityOf(p)}
                      {revokedNames.has(p) && <span class="status-revoked"> revoked</span>}
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
