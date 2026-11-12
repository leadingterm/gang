// Builds the dashboard bundle: `node tools/build.mjs <out dir>`, run from `ui/`.
// The output is `index.html` plus content-hashed `assets/*`; every text file sits beside `.gz` and
// `.br` copies. The bytes and names are identical across hosts and runs.
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";
import { build } from "esbuild";

const ASSETS = "/ui/assets/";
const out = process.argv[2];
if (!out) throw new Error("usage: build.mjs <out dir>");
rmSync(out, { recursive: true, force: true });
mkdirSync(join(out, "assets"), { recursive: true });

await build({
  entryPoints: ["src/main.tsx"],
  bundle: true,
  format: "esm",
  target: "es2024",
  minify: true,
  legalComments: "none",
  charset: "utf8",
  outdir: join(out, "assets"),
  publicPath: ASSETS,
  entryNames: "[name]",
  assetNames: "[name]-[hash]",
  loader: { ".woff2": "file" },
  logLevel: "warning",
});

// esbuild's entry hash also covers host-dependent build inputs, so the bundles are named by a hash of
// their bytes alone; the same sources then give the same names on every host.
const hashed = (file) => {
  const path = join(out, "assets", file);
  const hash = createHash("sha256").update(readFileSync(path)).digest("hex").slice(0, 16);
  const name = file.replace(/\.(\w+)$/, `-${hash}.$1`);
  renameSync(path, join(out, "assets", name));
  return `${ASSETS}${name}`;
};

writeFileSync(
  join(out, "index.html"),
  readFileSync("index.html", "utf8").replace("{{CSS}}", hashed("main.css")).replace("{{JS}}", hashed("main.js")),
);

// gzip's header records the OS that compressed; fix it to "unknown" so every host emits the same bytes.
const GZIP_OS_BYTE = 9;
const GZIP_OS_UNKNOWN = 255;
const precompress = (path) => {
  const bytes = readFileSync(path);
  const gz = gzipSync(bytes, { level: constants.Z_BEST_COMPRESSION });
  gz[GZIP_OS_BYTE] = GZIP_OS_UNKNOWN;
  writeFileSync(`${path}.gz`, gz);
  writeFileSync(
    `${path}.br`,
    brotliCompressSync(bytes, {
      params: {
        [constants.BROTLI_PARAM_QUALITY]: constants.BROTLI_MAX_QUALITY,
        [constants.BROTLI_PARAM_MODE]: constants.BROTLI_MODE_TEXT,
        [constants.BROTLI_PARAM_SIZE_HINT]: bytes.length,
      },
    }),
  );
};
// woff2 is already brotli inside, so fonts ship as they are.
precompress(join(out, "index.html"));
for (const file of readdirSync(join(out, "assets"))) {
  if (!file.endsWith(".woff2")) precompress(join(out, "assets", file));
}
