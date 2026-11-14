// Device approval is available without a dashboard session.
import { useRef, useState } from "preact/hooks";
import { duration } from "../format.ts";
import { codeOf, emptyCells, enter, erase, type Cells, type Entry } from "../user-code.ts";
import { ms } from "../wire-parse.ts";
import { PATHS, USER_CODE, type ApproveDeviceAuthorizationRequest, type ApproveDeviceAuthorizationResponse, type DeviceAuthorization } from "../wire.ts";
import { Gate, SIGN_IN_ERRORS, isSignInError } from "./gate.tsx";

/** A failed device lookup, by HTTP status. */
export class LookupRefused extends Error {
  readonly status: number;
  constructor(status: number) { super(`device lookup refused: ${status}`); this.status = status; }
}

/** Looks up a user-entered code without including it in the page URL. */
export const lookupDevice = async (code: string, request: typeof fetch = fetch): Promise<DeviceAuthorization> => {
  const response = await request(PATHS.getDeviceAuthorization.replace("{deviceAuthorization}", encodeURIComponent(code)));
  if (!response.ok) throw new LookupRefused(response.status);
  return await response.json() as DeviceAuthorization;
};

/** The server's refusal to start an approval, by HTTP status. */
export class ApprovalRefused extends Error {
  readonly status: number;
  constructor(status: number) { super(`device approval refused: ${status}`); this.status = status; }
}

/** Starts the sign-in that approves `code` and returns the provider URL to navigate to. A form post cannot: the page's `form-action 'self'` also covers a submission's redirect to the provider. */
export const approveDevice = async (code: string, request: typeof fetch = fetch): Promise<string> => {
  const response = await request(PATHS.device, {
    method: "POST",
    credentials: "same-origin",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ userCode: code } satisfies ApproveDeviceAuthorizationRequest),
  });
  if (!response.ok) throw new ApprovalRefused(response.status);
  return (await response.json() as ApproveDeviceAuthorizationResponse).authorizationUrl;
};

/** The request a user should inspect before authorizing a device. */
export const DeviceRequest = ({ authorization, code, now = Date.now() }: { authorization: DeviceAuthorization; code: string; now?: number }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const provider = authorization.provider.charAt(0).toUpperCase() + authorization.provider.slice(1);
  const approve = async (): Promise<void> => {
    setLoading(true);
    setError(null);
    try {
      location.assign(await approveDevice(code));
    } catch (failure) {
      setError(failure instanceof ApprovalRefused && failure.status === 404
        ? "That code is unknown, expired or already approved."
        : failure instanceof ApprovalRefused && failure.status === 429
          ? "Too many attempts; try again shortly."
          : "The sign-in could not start. Try again.");
      setLoading(false);
    }
  };
  return (
    <section class="device-request" aria-labelledby="device-request-title">
      <h2 id="device-request-title">Pending login <span class="mono">{code}</span></h2>
      <dl class="device-request__facts">
        <div><dt>From</dt><dd>{authorization.clientAddress ?? "Unknown address"}</dd></div>
        <div><dt>Requested</dt><dd>{duration(now - ms(authorization.createTime))} ago</dd></div>
        <div><dt>Expires in</dt><dd>{duration(ms(authorization.expireTime) - now)}</dd></div>
        <div><dt>Sign in with</dt><dd>{provider}</dd></div>
      </dl>
      <p class="device-request__warning">Approve only if you just ran <code>gang login</code> yourself and the address is yours. Approving lets that terminal act as you.</p>
      <div class="gate__actions">
        <button class="btn btn--primary btn--lg" type="button" onClick={approve} disabled={loading} aria-busy={loading ? "true" : undefined}>{loading ? `Opening ${provider}…` : `Sign in with ${provider} to approve`}</button>
      </div>
      {error && <p class="device-alert" role="alert">{error}</p>}
    </section>
  );
};

type LookupState =
  | { readonly kind: "editing" }
  | { readonly kind: "looking" | "missing" | "failed"; readonly code: string }
  | { readonly kind: "found"; readonly code: string; readonly authorization: DeviceAuthorization };

const CodeInput = ({ cells, invalid, onChange }: { cells: Cells; invalid: boolean; onChange: (entry: Entry) => void }) => {
  const inputs = useRef<(HTMLInputElement | null)[]>([]);
  const focus = (index: number): void => {
    inputs.current[index]?.focus();
    inputs.current[index]?.select();
  };
  const apply = (entry: Entry): void => {
    onChange(entry);
    focus(entry.focus);
  };
  const cell = (index: number) => (
    <input
      key={index}
      class={cells[index] ? "code__cell is-filled" : "code__cell"}
      type="text"
      inputMode="text"
      autoComplete="off"
      autoCapitalize="characters"
      autoCorrect="off"
      spellcheck={false}
      enterKeyHint={index === USER_CODE.length - 1 ? "go" : "next"}
      aria-label={`Character ${index + 1} of ${USER_CODE.length}`}
      aria-invalid={invalid ? "true" : undefined}
      value={cells[index]}
      autoFocus={index === 0}
      ref={(element) => { inputs.current[index] = element; }}
      onFocus={(event) => event.currentTarget.select()}
      onInput={(event) => {
        const input = event as InputEvent;
        const next = input.inputType?.startsWith("delete")
          ? erase(cells, index)
          : enter(cells, index, input.data ?? event.currentTarget.value);
        event.currentTarget.value = next.cells[index] ?? "";
        apply(next);
      }}
      onPaste={(event) => {
        event.preventDefault();
        apply(enter(cells, index, event.clipboardData?.getData("text") ?? ""));
      }}
      onKeyDown={(event) => {
        if (event.key === "Backspace") {
          event.preventDefault();
          apply(erase(cells, index));
        } else if (event.key === "ArrowLeft" || event.key === "ArrowRight" || event.key === "Home" || event.key === "End") {
          event.preventDefault();
          focus(event.key === "Home" ? 0 : event.key === "End" ? USER_CODE.length - 1 : Math.max(0, Math.min(USER_CODE.length - 1, index + (event.key === "ArrowLeft" ? -1 : 1))));
        }
      }}
    />
  );
  const middle = USER_CODE.length / 2;
  return (
    <div class="code" role="group" aria-labelledby="device-code-label" aria-describedby="device-code-help">
      <div class="code__half">{Array.from({ length: middle }, (_, index) => cell(index))}</div>
      <span class="code__dash" aria-hidden="true">-</span>
      <div class="code__half">{Array.from({ length: middle }, (_, index) => cell(index + middle))}</div>
    </div>
  );
};

/** A code must be entered explicitly, even when the URL contains a query. */
export const Device = ({ reason }: { reason: string | null }) => {
  const [cells, setCells] = useState<Cells>(emptyCells);
  const [state, setState] = useState<LookupState>({ kind: "editing" });
  const sequence = useRef(0);
  const code = codeOf(cells);
  const lookUp = async (entered: string): Promise<void> => {
    const current = ++sequence.current;
    setState({ kind: "looking", code: entered });
    try {
      const authorization = await lookupDevice(entered);
      if (current === sequence.current) setState({ kind: "found", code: entered, authorization });
    } catch (failure) {
      if (current === sequence.current) setState({ kind: failure instanceof LookupRefused && failure.status === 404 ? "missing" : "failed", code: entered });
    }
  };
  const change = (entry: Entry): void => {
    const nextCode = codeOf(entry.cells);
    if (entry.cells.every((cell, index) => cell === cells[index])) return;
    sequence.current += 1;
    setCells(entry.cells);
    setState({ kind: "editing" });
    if (nextCode !== null && nextCode !== code) void lookUp(nextCode);
  };
  const help = state.kind === "looking" ? "Looking up…"
    : state.kind === "missing" ? "No pending login has this code. It may have expired or been approved already."
      : state.kind === "failed" ? "The lookup failed. Check your connection and try again."
        : state.kind === "found" ? "Found a pending login."
          : "Eight letters, no vowels. Paste or type it; the dash is optional.";
  return (
    <Gate title="Approve a device login" density={0.3}>
      <h1>Approve a device login</h1>
      <p class="gate__lede">Enter the code that <code>gang login</code> printed in your terminal.</p>
      {isSignInError(reason) && <p class="device-alert" role="alert">{SIGN_IN_ERRORS[reason].text} Enter the code again to retry.</p>}
      <form class="code-entry" onSubmit={(event) => { event.preventDefault(); if (code !== null && state.kind !== "looking") void lookUp(code); }} noValidate>
        <span class="field__label" id="device-code-label">Device code</span>
        <CodeInput cells={cells} invalid={state.kind === "missing"} onChange={change} />
        <p class={state.kind === "missing" ? "field__help is-error" : "field__help"} id="device-code-help" aria-live="polite">{help}</p>
        {state.kind !== "found" && <div class="gate__actions">
          <button class="btn btn--lg" type="submit" disabled={code === null || state.kind === "looking"} aria-busy={state.kind === "looking"}>{state.kind === "looking" ? "Looking up…" : "Look up"}</button>
        </div>}
      </form>
      {state.kind === "found" && <DeviceRequest authorization={state.authorization} code={state.code} now={Date.now()} />}
    </Gate>
  );
};

/** Confirmation after the browser has approved the request. */
export const DeviceApproved = () => (
  <Gate title="Device login approved" density={0.55}>
    <span class="gate__code ok">APPROVED</span>
    <h1>Device login approved</h1>
    <p>You can close this tab: <code>gang login</code> finishes by itself in your terminal.</p>
  </Gate>
);
