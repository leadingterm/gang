// The app: one client, one route, and the screen for both.
import { signal } from "@preact/signals";
import { useEffect } from "preact/hooks";
import type { StateClient } from "./client.ts";
import { routeOf, SCREENS, type Route, type Screen } from "./routes.ts";
import { Shell } from "./shell.tsx";
import { ClientContext, useClient } from "./ui.tsx";
import { Access } from "./views/access.tsx";
import { Actions } from "./views/actions.tsx";
import { Activity } from "./views/activity.tsx";
import { Device, DeviceApproved } from "./views/device.tsx";
import { Forbidden, SignIn, SignInError } from "./views/gate.tsx";
import { Overview } from "./views/overview.tsx";
import { Workers } from "./views/workers.tsx";

const SCREEN_VIEWS: Record<Screen, () => preact.JSX.Element | null> = {
  overview: Overview,
  actions: Actions,
  workers: Workers,
  activity: Activity,
  access: Access,
};

export const route = signal<Route>(routeOf(location.pathname, location.search));

const go = (href: string): void => {
  history.pushState(null, "", href);
  const url = new URL(href, location.href);
  route.value = routeOf(url.pathname, url.search);
  scrollTo(0, 0);
};

/** Same-document navigation for `/ui` links, so the stream stays open across screens. */
const useNavigation = (): void => {
  useEffect(() => {
    const onClick = (ev: MouseEvent): void => {
      if (ev.defaultPrevented || ev.button !== 0 || ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.altKey) return;
      const a = (ev.target as Element | null)?.closest("a");
      const href = a?.getAttribute("href");
      if (!a || !href || a.target || !SCREENS.some((s) => s.path === href)) return;
      ev.preventDefault();
      go(href);
    };
    const onKey = (ev: KeyboardEvent): void => {
      if (ev.metaKey || ev.ctrlKey || ev.altKey || (ev.target as Element | null)?.closest("input, select, textarea")) return;
      const screen = SCREENS[Number(ev.key) - 1];
      if (screen) go(screen.path);
    };
    const onPop = (): void => {
      route.value = routeOf(location.pathname, location.search);
    };
    document.addEventListener("click", onClick);
    document.addEventListener("keydown", onKey);
    addEventListener("popstate", onPop);
    return () => {
      document.removeEventListener("click", onClick);
      document.removeEventListener("keydown", onKey);
      removeEventListener("popstate", onPop);
    };
  }, []);
};

const Screens = () => {
  const client = useClient();
  useNavigation();
  const r = route.value;
  const link = client.link.value;
  if (r.kind === "error") return <SignInError reason={r.reason} />;
  if (r.kind === "device") return <Device error={r.error} />;
  if (r.kind === "deviceApproved") return <DeviceApproved />;
  if (link.kind === "signed_out") return <SignIn />;
  if (!client.session.value) return null;
  const View = SCREEN_VIEWS[r.screen];
  const blocked = link.kind === "forbidden" && r.screen !== "access";
  return <Shell screen={r.screen}>{blocked ? <Forbidden /> : <View />}</Shell>;
};

export const App = ({ client }: { client: StateClient }) => (
  <ClientContext.Provider value={client}>
    <Screens />
  </ClientContext.Provider>
);
