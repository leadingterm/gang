// Preview server: serves the built bundle with the production `/ui` headers (`headers.json`) and
// answers the HTTP API endpoints from the recorded fixtures, including device-code lookup and a
// simulated approval. `node tools/preview.mjs <dist>`, run from `ui/`.
// Open `/ui/?scenario=<name>` to pick a scenario: live (default), stale, lagging, empty, cold,
// disconnected, forbidden, signedout, long-session. Enter BCDF-GHJK at `/ui/device`. PORT selects the port (default 8412).
import { previewServer } from "./preview-server.mjs";

const dist = process.argv[2];
if (!dist) throw new Error("usage: preview.mjs <dist>");
const PORT = Number(process.env.PORT ?? 8412);
previewServer({ dist }).listen(PORT, "127.0.0.1", () => console.log(`preview: http://127.0.0.1:${PORT}/ui/`));
