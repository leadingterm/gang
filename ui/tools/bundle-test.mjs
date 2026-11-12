// Bundles a JSX test for Node, which strips types but cannot run JSX:
// `node tools/bundle-test.mjs <entry.test.tsx> <out.mjs>`, run from `ui/`.
// The output sits one directory below `ui/`, as the test itself does, so `import.meta.url`
// relative paths (fixtures, snapshots) resolve the same.
import { build } from "esbuild";

const [entry, outfile] = process.argv.slice(2);
if (!entry || !outfile) throw new Error("usage: bundle-test.mjs <entry> <out>");

await build({
  entryPoints: [entry],
  outfile,
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node24",
  logLevel: "warning",
});
