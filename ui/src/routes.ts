// The `/ui` routes. The server serves `index.html` for every extensionless `/ui` path.

import { PATHS } from "./wire.ts";

export type Screen = "overview" | "actions" | "builds" | "workers" | "activity" | "access";

export const SCREENS = [
  { id: "overview", label: "Overview", path: "/ui/" },
  { id: "actions", label: "Actions", path: "/ui/actions" },
  { id: "builds", label: "Builds", path: "/ui/builds" },
  { id: "workers", label: "Workers", path: "/ui/workers" },
  { id: "activity", label: "Activity", path: "/ui/activity" },
  { id: "access", label: "Access", path: "/ui/access" },
] as const satisfies readonly { id: Screen; label: string; path: string }[];

export type Route =
  | { readonly kind: "screen"; readonly screen: Screen }
  | { readonly kind: "build"; readonly id: string }
  | { readonly kind: "error"; readonly reason: string | null }
  | { readonly kind: "device"; readonly reason: string | null }
  | { readonly kind: "deviceApproved" };

/** Whether this route needs the authenticated state client. */
export const startsClient = (route: Route): boolean => route.kind === "screen" || route.kind === "build";

export const routeOf = (pathname: string, search: string): Route => {
  const path = pathname.replace(/\/+$/, "");
  if (path === PATHS.error) return { kind: "error", reason: new URLSearchParams(search).get("reason") };
  if (path === PATHS.device) return { kind: "device", reason: new URLSearchParams(search).get("reason") };
  if (path === PATHS.deviceApproved) return { kind: "deviceApproved" };
  const build = /^\/ui\/builds\/([^/]+)$/.exec(path);
  if (build?.[1]) {
    try {
      return { kind: "build", id: decodeURIComponent(build[1]) };
    } catch {
      return { kind: "screen", screen: "overview" };
    }
  }
  const screen = SCREENS.find((s) => s.path.replace(/\/+$/, "") === path);
  return { kind: "screen", screen: screen?.id ?? "overview" };
};
