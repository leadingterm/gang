// Device approval is available without a dashboard session.
import { useRef, useState } from "preact/hooks";
import type { ComponentChildren } from "preact";
import { duration } from "../format.ts";
import { Brand, useTitle } from "../ui.tsx";
import { ms } from "../wire-parse.ts";
import { PATHS, type DeviceAuthorization } from "../wire.ts";

const DeviceGate = ({ title, children }: { title: string; children: ComponentChildren }) => {
  useTitle(title);
  return (
    <div class="gate">
      <main class="gate__main" id="main">
        <Brand href="/ui/" />
        <div class="gate__body">{children}</div>
        <p class="gate__foot"><span>{location.host}</span></p>
      </main>
    </div>
  );
};

/** Looks up a user-entered code without including it in the page URL. */
export const lookupDevice = async (code: string, request: typeof fetch = fetch): Promise<DeviceAuthorization> => {
  const response = await request(PATHS.getDeviceAuthorization.replace("{deviceAuthorization}", encodeURIComponent(code)));
  if (!response.ok) throw new Error("device authorization not found");
  return await response.json() as DeviceAuthorization;
};

/** The request a user should inspect before authorizing a device. */
export const DeviceRequest = ({ authorization, code, now = Date.now() }: { authorization: DeviceAuthorization; code: string; now?: number }) => (
  <div>
    {authorization.clientAddress && <p>Client address: {authorization.clientAddress}</p>}
    <p>Requested {duration(now - ms(authorization.createTime))} ago.</p>
    <p>Only continue if you just ran <code>gang login</code> yourself.</p>
    <form method="post" action={PATHS.device}>
      <input type="hidden" name="userCode" value={code} />
      <button class="btn btn--primary btn--lg" type="submit">Sign in with {authorization.provider} to approve</button>
    </form>
  </div>
);

/** A code must be entered explicitly, even when the URL contains a query. */
export const Device = ({ error }: { error: string | null }) => {
  const [code, setCode] = useState("");
  const [found, setFound] = useState<{ code: string; authorization: DeviceAuthorization } | null>(null);
  const [failed, setFailed] = useState(false);
  const [loading, setLoading] = useState(false);
  const sequence = useRef(0);
  const lookUp = async (event: Event): Promise<void> => {
    event.preventDefault();
    const current = ++sequence.current;
    setFound(null);
    setFailed(false);
    setLoading(true);
    try {
      const authorization = await lookupDevice(code);
      if (current === sequence.current) setFound({ code, authorization });
    } catch {
      if (current === sequence.current) setFailed(true);
    } finally {
      if (current === sequence.current) setLoading(false);
    }
  };
  return (
    <DeviceGate title="Approve a device login">
      <h1>Approve a device login</h1>
      {error === "unknown" && <p role="alert">That code is unknown, expired or already approved.</p>}
      <form onSubmit={lookUp}>
        <label for="device-code">Device code</label>
        <input id="device-code" type="text" autoComplete="off" autoCapitalize="characters" placeholder="XXXX-XXXX" value={code} onInput={(event) => {
          sequence.current += 1;
          setCode(event.currentTarget.value.toUpperCase());
          setFound(null);
          setFailed(false);
          setLoading(false);
        }} />
        <button class="btn btn--primary" type="submit" disabled={loading}>Look up</button>
      </form>
      {failed && <p role="alert">No pending login has this code.</p>}
      {found && <DeviceRequest authorization={found.authorization} code={found.code} />}
    </DeviceGate>
  );
};

/** Confirmation after the browser has approved the request. */
export const DeviceApproved = () => (
  <DeviceGate title="Device login approved">
    <h1>Device login approved.</h1>
    <p>You can close this tab; <code>gang login</code> finishes by itself.</p>
  </DeviceGate>
);
