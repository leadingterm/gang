// Headless Chromium driven through its DevTools browser endpoint.
import { spawn } from "node:child_process";
import { join } from "node:path";

export const launch = async (program, { profile }) => {
  const child = spawn(program, [
    "--headless", "--no-first-run", "--no-default-browser-check", "--disable-gpu",
    "--disable-extensions", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "about:blank",
  ], { detached: true, stdio: ["ignore", "ignore", "pipe"], env: { ...process.env, HOME: profile, XDG_CACHE_HOME: join(profile, ".cache") } });
  const exited = new Promise((resolve) => {
    child.once("exit", (code, signal) => resolve({ code, signal }));
    child.once("error", (error) => resolve({ error }));
  });
  const settled = new Promise((resolve) => child.once("close", resolve));
  const signalGroup = (signal) => {
    if (!child.pid) return;
    try {
      process.kill(-child.pid, signal);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  };
  let stderr = "";
  const tail = (s) => s.length > 16384 ? s.slice(-16384) : s;
  let endpointResolved = false;
  let pendingLine = "";
  const endpoint = new Promise((resolve, reject) => {
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      stderr = tail(stderr + chunk);
      if (endpointResolved) return;
      pendingLine += chunk;
      const lines = pendingLine.split("\n");
      pendingLine = lines.pop();
      for (const line of lines) {
        const prefix = "DevTools listening on ";
        if (line.startsWith(prefix)) {
          endpointResolved = true;
          pendingLine = "";
          resolve(line.slice(prefix.length).trim());
          break;
        }
      }
    });
    exited.then(({ code, signal, error }) => reject(new Error(`Chromium exited before DevTools (${error?.message ?? signal ?? code}):\n${stderr}`)));
  });
  let socket;
  try {
    socket = new WebSocket(await endpoint);
    await new Promise((resolve, reject) => {
      socket.addEventListener("open", resolve, { once: true });
      socket.addEventListener("error", () => reject(new Error("Could not connect to Chromium DevTools")), { once: true });
    });
  } catch (error) {
    signalGroup("SIGKILL");
    await settled;
    throw error;
  }

  let nextId = 0;
  const pending = new Map();
  const listeners = new Map();
  const waiters = new Set();
  let gone = null;
  let rejectClosed;
  const closed = new Promise((resolve, reject) => { rejectClosed = reject; });
  closed.catch(() => {});
  const fail = (error) => {
    if (gone) return;
    gone = error;
    for (const r of pending.values()) r.reject(error);
    pending.clear();
    for (const reject of waiters) reject(error);
    waiters.clear();
    rejectClosed(error);
  };
  socket.addEventListener("message", ({ data }) => {
    const message = JSON.parse(data);
    if (message.id !== undefined) {
      const request = pending.get(message.id);
      if (!request) return;
      pending.delete(message.id);
      if (message.error) request.reject(new Error(message.error.message));
      else request.resolve(message.result);
    } else if (message.method) {
      for (const fn of listeners.get(`${message.sessionId ?? ""}:${message.method}`) ?? []) fn(message.params);
    }
  });
  socket.addEventListener("close", () => fail(new Error(`Chromium DevTools socket closed\n${stderr}`)));
  exited.then(({ code, signal, error }) => fail(new Error(`Chromium exited (${error?.message ?? signal ?? code})\n${stderr}`)));
  let closing = false;
  const terminateOnExit = exited.then(() => {
    if (closing) signalGroup("SIGKILL");
  });
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    if (gone) return reject(gone);
    const id = ++nextId;
    pending.set(id, { resolve, reject });
    try {
      socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    } catch (error) {
      pending.delete(id);
      reject(error);
    }
  });
  return {
    pid: child.pid,
    closed,
    async newPage() {
      const { targetId } = await send("Target.createTarget", { url: "about:blank" });
      const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
      const key = (method) => `${sessionId}:${method}`;
      const on = (method, fn) => {
        if (!listeners.has(key(method))) listeners.set(key(method), new Set());
        listeners.get(key(method)).add(fn);
      };
      const off = (method, fn) => listeners.get(key(method))?.delete(fn);
      return {
        send: (method, params) => send(method, params, sessionId), on, off,
        once: (method, predicate = () => true) => new Promise((resolve, reject) => {
          if (gone) return reject(gone);
          const listener = (event) => {
            if (predicate(event)) {
              off(method, listener);
              waiters.delete(reject);
              resolve(event);
            }
          };
          waiters.add(reject);
          on(method, listener);
        }),
      };
    },
    async close() {
      closing = true;
      const open = !gone && socket.readyState === WebSocket.OPEN;
      gone ??= new Error("Chromium closed");
      // Keep the deadline until all inherited stdio handles have closed.
      const force = setTimeout(() => signalGroup("SIGKILL"), 5000);
      force.unref();
      try {
        if (open) {
          try {
            socket.send(JSON.stringify({ id: ++nextId, method: "Browser.close" }));
          } catch {
            signalGroup("SIGKILL");
          }
        } else signalGroup("SIGKILL");
        await Promise.all([settled, terminateOnExit]);
      } finally {
        clearTimeout(force);
      }
      socket.close();
    },
  };
};
