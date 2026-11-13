// The `/ui` routes. The server serves `index.html` for every extensionless `/ui` path.

import { PATHS } from "./wire.ts";

export type Screen = "overview" | "actions" | "workers" | "activity" | "access";

export const SCREENS = [
  { id: "overview", label: "Overview", path: "/ui/" },
  { id: "actions", label: "Actions", path: "/ui/actions" },
  { id: "workers", label: "Workers", path: "/ui/workers" },
  { id: "activity", label: "Activity", path: "/ui/activity" },
  { id: "access", label: "Access", path: "/ui/access" },
] as const satisfies readonly { id: Screen; label: string; path: string }[];

export type Route =
  | { readonly kind: "screen"; readonly screen: Screen }
  | { readonly kind: "error"; readonly reason: string | null }
  | { readonly kind: "device"; readonly error: string | null }
  | { readonly kind: "deviceApproved" };

export const routeOf = (pathname: string, search: string): Route => {
  const path = pathname.replace(/\/+$/, "");
  if (path === PATHS.error) return { kind: "error", reason: new URLSearchParams(search).get("reason") };
  if (path === PATHS.device) return { kind: "device", error: new URLSearchParams(search).get("error") };
  if (path === PATHS.deviceApproved) return { kind: "deviceApproved" };
  const screen = SCREENS.find((s) => s.path.replace(/\/+$/, "") === path);
  return { kind: "screen", screen: screen?.id ?? "overview" };
};
