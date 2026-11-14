// Screens outside the app frame: sign-in, a failed sign-in, and the in-app refusals.
import type { ComponentChildren } from "preact";
import { loginUrl } from "../client.ts";
import { Brand, Empty, useClient, useTitle } from "../ui.tsx";
import { identityOf } from "../wire-parse.ts";

const CANOPY_CELLS = 24 * 16;
const CANOPY_STATES = ["busy", "busy", "busy", "idle", "idle", "lit", "pending", "draining"] as const;

/** A fixed pseudo-random farm, so the backdrop is the same on every load. */
const canopy = (density: number): readonly string[] => {
  let seed = 7;
  const next = (): number => (seed = (seed * 16807) % 2147483647) / 2147483647;
  return Array.from({ length: CANOPY_CELLS }, () => {
    if (next() < density) return CANOPY_STATES[Math.floor(next() * 5)] ?? "empty";
    return next() < 0.06 ? (CANOPY_STATES[Math.floor(next() * CANOPY_STATES.length)] ?? "empty") : "empty";
  });
};

/** A screen outside the app frame: the brand, a body and the canopy backdrop at `density`. */
export const Gate = ({ title, density, children }: { title: string; density: number; children: ComponentChildren }) => {
  useTitle(title);
  return (
    <div class="gate">
      <main class="gate__main" id="main">
        <Brand href="/ui/" />
        <div class="gate__body">{children}</div>
        <p class="gate__foot">
          <span>{location.host}</span>
        </p>
      </main>
      <div class="canopy" aria-hidden="true">
        {canopy(density).map((state, i) => (
          <span key={i} class="cell">
            <i class={`c-${state}`} />
          </span>
        ))}
      </div>
    </div>
  );
};

/** No identity: sign in when the server offers a login, otherwise say why nothing shows. */
export const SignIn = () => {
  const login = useClient().session.value?.login;
  if (!login) {
    return (
      <Gate title="Not signed in" density={0.08}>
        <span class="gate__code">401</span>
        <h1>Not signed in</h1>
        <p>This server offers no browser sign-in.</p>
      </Gate>
    );
  }
  return (
    <Gate title="Sign in" density={0.55}>
      <h1>Sign in</h1>
      <div class="gate__actions">
        <a class="btn btn--primary btn--lg" href={loginUrl(`${location.pathname}${location.search}`)}>
          <span class="btn__label">Continue with {login.provider.charAt(0).toUpperCase() + login.provider.slice(1)}</span>
        </a>
      </div>
    </Gate>
  );
};

export const SIGN_IN_ERRORS = {
  denied: { code: "DECLINED", caution: false, title: "Sign-in declined", text: "The identity provider refused this sign-in.", cta: "Use another account" },
  failed: { code: "FAILED", caution: false, title: "Sign-in failed", text: "The identity provider returned an error, or the attempt expired.", cta: "Try again" },
  unavailable: { code: "UNAVAILABLE", caution: true, title: "Sign-in unavailable", text: "The identity provider is unreachable or busy.", cta: "Try again" },
} as const;

export const isSignInError = (reason: string | null): reason is keyof typeof SIGN_IN_ERRORS => reason !== null && Object.hasOwn(SIGN_IN_ERRORS, reason);

/** `/ui/error?reason=denied|failed|unavailable`, where a failed callback lands. */
export const SignInError = ({ reason }: { reason: string | null }) => {
  const e = SIGN_IN_ERRORS[isSignInError(reason) ? reason : "failed"];
  return (
    <Gate title={e.title} density={0.08}>
      <span class={e.caution ? "gate__code caution" : "gate__code"}>{e.code}</span>
      <h1>{e.title}</h1>
      <p>{e.text}</p>
      <div class="gate__actions">
        <a class="btn btn--primary btn--lg" href={loginUrl("/ui/")}>
          {e.cta}
        </a>
      </div>
    </Gate>
  );
};

/** Signed in, but no role grants `state.read`. */
export const Forbidden = () => {
  const principal = useClient().session.value?.principal;
  return (
    <Empty>
      <div>
        <h2>No access</h2>
        <p class="mono">
          <span class="warning">state.read</span> missing{principal && ` for ${identityOf(principal)}`}
        </p>
      </div>
    </Empty>
  );
};
