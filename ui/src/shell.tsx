// The app frame: top bar, annunciator lamps, freshness banner, main region and the phone tab bar.
import type { ComponentChildren } from "preact";
import { useEffect } from "preact/hooks";
import type { Link } from "./client.ts";
import { useComputed } from "@preact/signals";
import { identityOf, ms, splitIdentity } from "./wire-parse.ts";
import { PATHS } from "./wire.ts";
import { clock, humanize } from "./format.ts";
import { actionsStale } from "./store.ts";
import { Brand, Since, Until, useClient, useTitle } from "./ui.tsx";
import { SCREENS, type Screen } from "./routes.ts";

type Level = "normal" | "live" | "caution" | "warning";

interface Lamp {
  readonly id: string;
  readonly level: Level;
  readonly long?: ComponentChildren;
  readonly short?: ComponentChildren;
}

/** A reason this client does not know reads as its own name. */
const skipText = (reason: string | undefined): string => (reason === undefined ? "skipped" : humanize(reason));

const LampView = ({ id, level, long, short }: Lamp) => (
  <div class={`ann ann--${id}`} data-level={level}>
    <span class={id === "stream" ? "ann__lamp beat" : "ann__lamp"} aria-hidden="true" />
    <span class="ann__code">{id.toUpperCase()}</span>
    <span class="ann__value">
      {long !== undefined && <span class="ann__long">{long}</span>}
      {short !== undefined && (
        <span class="ann__short" aria-hidden="true">
          {short}
        </span>
      )}
    </span>
  </div>
);

/** The STREAM lamp dips once per heartbeat. */
const useBeat = (): void => {
  const client = useClient();
  const link = client.link.value;
  const beats = link.kind === "live" ? link.beats : 0;
  useEffect(() => {
    if (beats === 0) return;
    const lamps = document.querySelectorAll(".beat");
    for (const b of lamps) b.classList.add("is-beat");
    const id = setTimeout(() => {
      for (const b of lamps) b.classList.remove("is-beat");
    }, 160);
    return () => clearTimeout(id);
  }, [beats]);
};

const streamLamp = (link: Link): Lamp => {
  switch (link.kind) {
    case "live":
      return { id: "stream", level: "live" };
    case "lost":
      return {
        id: "stream",
        level: "caution",
        long: (
          <>
            reconnecting <Until t={link.retryAt} />
          </>
        ),
        short: <Until t={link.retryAt} />,
      };
    case "forbidden":
      return { id: "stream", level: "warning", long: "403 state.read", short: "403" };
    case "revoked":
      return { id: "stream", level: "warning", long: "revoked", short: "revoked" };
    case "connecting":
    case "signed_out":
      return { id: "stream", level: "normal" };
  }
};

const Annunciators = () => {
  const client = useClient();
  useBeat();
  const state = client.state.value;
  const session = client.session.value;
  const stale = useComputed(() => actionsStale(client.state.value?.actions, client.now.value)).value;
  const clockText = useComputed(() => clock(client.now.value));
  const pass = state?.pass;
  const unplaceable = state?.queue.unplaceable ?? 0;
  const lamps: Lamp[] = [
    streamLamp(client.link.value),
    pass?.state === "SKIPPED"
      ? {
          id: "pass",
          level: "warning",
          long: pass.lastOkTime === undefined ? "skipped" : <>skipped <Since t={ms(pass.lastOkTime)} /></>,
          short: "skipped",
        }
      : { id: "pass", level: "normal" },
    unplaceable > 0
      ? { id: "queue", level: "caution", long: `${unplaceable} unplaceable`, short: String(unplaceable) }
      : { id: "queue", level: "normal" },
    stale && state?.actions
      ? { id: "actions", level: "caution", long: `as of ${clock(ms(state.actions.sampleTime))}`, short: "stale" }
      : { id: "actions", level: "normal" },
    session?.expireTime
      ? { id: "session", level: "normal", long: <Until t={ms(session.expireTime)} />, short: <Until t={ms(session.expireTime)} /> }
      : { id: "session", level: "normal" },
  ];
  return (
    <section class="annun" aria-label="System status">
      <div class="shell annun__grid">
        {lamps.map((lamp) => (
          <LampView key={lamp.id} {...lamp} />
        ))}
        <div class="annun__clock">
          <span>{clockText}</span>&nbsp;UTC
        </div>
      </div>
    </section>
  );
};

const BannerView = ({ level, role, title, meta }: { level: Level; role: "status" | "alert"; title: ComponentChildren; meta?: ComponentChildren }) => (
  <div class="banner" data-level={level} role={role}>
    <div class="shell banner__inner">
      <span class="banner__title">{title}</span>
      {meta !== undefined && <span class="banner__meta">{meta}</span>}
    </div>
  </div>
);

const Banner = () => {
  const client = useClient();
  const link = client.link.value;
  const pass = client.state.value?.pass;
  if (link.kind === "lost") {
    return (
      <BannerView
        level="caution"
        role="status"
        title="Stream lost"
        meta={
          <>
            {link.frozenAt !== null && <>frozen at {clock(link.frozenAt)} · </>}retry <Until t={link.retryAt} />
          </>
        }
      />
    );
  }
  if (link.kind === "revoked") return <BannerView level="warning" role="alert" title="Access revoked" />;
  if (pass?.state === "SKIPPED") {
    return (
      <BannerView
        level="warning"
        role="alert"
        title={<>Pass skipped · {skipText(pass.skipReason)}</>}
        meta={
          pass.lastOkTime === undefined ? undefined : (
            <>
              frozen at {clock(ms(pass.lastOkTime))} · <Since t={ms(pass.lastOkTime)} />
            </>
          )
        }
      />
    );
  }
  return null;
};

const NavLinks = ({ current, keys }: { current: Screen; keys: boolean }) => (
  <>
    {SCREENS.map(({ id, label, path }, i) => (
      <a key={id} class="nav__link" href={path} aria-current={id === current ? "page" : undefined}>
        {label}
        {keys && (
          <span class="nav__key" aria-hidden="true">
            {i + 1}
          </span>
        )}
      </a>
    ))}
  </>
);

export const Shell = ({ screen, children }: { screen: Screen; children: ComponentChildren }) => {
  const client = useClient();
  const session = client.session.value;
  const link = client.link.value;
  const frozen = link.kind === "lost" || link.kind === "revoked" || client.state.value?.pass.state === "SKIPPED";
  const who = session?.principal === undefined ? undefined : splitIdentity(identityOf(session.principal));
  const label = SCREENS.find((s) => s.id === screen)?.label ?? "";
  useTitle(label);
  return (
    <>
      <a class="skip" href="#main">
        Skip to content
      </a>
      <header class="top">
        <div class="shell top__inner">
          <Brand href={SCREENS[0].path} />
          <span class="endpoint mono">{location.host}</span>
          <nav class="nav" aria-label="Screens">
            <NavLinks current={screen} keys />
          </nav>
          <div class="who">
            {who && (
              <span class="who__id">
                <span class="who__provider">{who[0]} ·</span> {who[1]}
              </span>
            )}
            {session?.login && (
              <form action={PATHS.logout} method="post">
                <button class="btn btn--quiet" type="submit">
                  Sign out
                </button>
              </form>
            )}
          </div>
        </div>
      </header>
      <Annunciators />
      <Banner />
      <main id="main" class="main shell" tabIndex={-1} data-fresh={frozen ? "frozen" : undefined}>
        <h1 class="visually-hidden">{label}</h1>
        {children}
      </main>
      <nav class="tabbar" aria-label="Screens">
        <NavLinks current={screen} keys={false} />
      </nav>
    </>
  );
};
