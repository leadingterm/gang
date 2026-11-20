// Exercise the real fixture-backed overview at each supported viewport.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { launch } from "../tools/chromium.mjs";
import { previewServer } from "../tools/preview-server.mjs";
import { FOCUSED_WIDTHS, FULL_WIDTHS } from "./layout-widths.mjs";
import { load } from "./sse.ts";

process.env.FONTCONFIG_FILE = join(process.env.JS_BINARY__RUNFILES, process.env.FONTCONFIG_FILE);

const probe = async (scenario) => {
  if (!document.querySelector(".readouts")) await new Promise((resolve) => {
    const observer = new MutationObserver(() => {
      if (document.querySelector(".readouts")) {
        observer.disconnect();
        resolve();
      }
    });
    observer.observe(document, { subtree: true, childList: true });
  });
  await document.fonts.ready;
  const families = ["Atkinson Hyperlegible Next", "Atkinson Hyperlegible Mono"];
  const sample = "The quick brown fox 0123456789";
  const loaded = await Promise.all(families.map((family) => document.fonts.load(`16px "${family}"`, sample)));
  await Promise.all(document.getAnimations().filter((a) => a.effect?.getComputedTiming().iterations !== Infinity).map((a) => a.finished.catch(() => {})));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  const violations = [];
  const add = (message) => violations.push(message);
  const EPS = 0.5;
  const equal = (a, b) => Math.abs(a - b) <= EPS;
  const rect = (element) => {
    if (!element) return null;
    const { left, right, top, bottom, width, height } = element.getBoundingClientRect();
    return { left, right, top, bottom, width, height };
  };
  for (const [index, family] of families.entries()) {
    if (!loaded[index].length || !document.fonts.check(`16px "${family}"`, sample)) add(`font ${family} is not loaded`);
  }
  const container = document.querySelector(".readouts");
  const elements = [...container.querySelectorAll(":scope > .readout")];
  const expected = ["Queue", "Executing", "Instances", "Done / min", "Action cache"];
  if (elements.length !== expected.length) add(`expected five tiles, got ${elements.length}`);
  let isolatedMarkers = 0;
  const tiles = elements.map((element, index) => {
    const label = element.querySelector(".label");
    const name = label?.textContent.trim() ?? "(missing label)";
    if (name !== expected[index]) add(`tile ${index}: expected ${expected[index]}, got ${name}`);
    const value = element.querySelector(".readout__big");
    const meterSlot = element.querySelector(".readout__meter");
    const meter = element.querySelector(".meter");
    const caption = element.querySelector(".readout__sub");
    const chart = element.querySelector(".readout__chart");
    for (const [role, node] of [["label", label], ["value", value], ["meter", meterSlot], ["caption", caption], ["chart", chart]]) {
      if (!node) add(`${name}: missing ${role}`);
    }
    const bounds = rect(element);
    const labelRect = rect(label);
    const chartRect = rect(chart);
    const meterRect = rect(meter);
    const meterSlotRect = rect(meterSlot);
    const captionRect = rect(caption);
    const svgs = [...element.querySelectorAll("svg")];
    const chartSvgs = chart ? [...chart.querySelectorAll("svg")] : [];
    if (meterSlotRect && meterSlotRect.height <= EPS) add(`${name}: meter slot has no height`);
    if (captionRect && captionRect.height <= EPS) add(`${name}: caption slot has no height`);
    if (chartRect && chartRect.height <= 0) add(`${name}: chart slot has no height`);
    if (name === "Instances" && svgs.length) add("Instances: chart contains fabricated history");
    if (chart && chartSvgs.length !== (name === "Instances" ? 0 : 1)) add(`${name}: expected ${name === "Instances" ? 0 : 1} chart SVGs, got ${chartSvgs.length}`);
    if (name === "Action cache" && chartSvgs.length === 1) {
      const svg = chartSvgs[0];
      if (svg.getAttribute("aria-label") !== "Action-cache lookups as area and hits as line per step") add("Action cache: incorrect chart label");
      if (scenario === "isolated") {
        const runs = [...svg.querySelectorAll("path.spark__run")];
        const areas = [...svg.querySelectorAll("path.spark__area")];
        if (runs.length !== 1 || areas.length !== 1) add(`Action cache: expected one hit path and one lookup area, got ${runs.length} and ${areas.length}`);
        if (svg.querySelectorAll("circle.spark__point").length) add("Action cache: unexpected isolated hit points");
        if (runs.length === 1 && areas.length === 1) {
          const [run] = runs;
          const [area] = areas;
          if (!(area.compareDocumentPosition(run) & Node.DOCUMENT_POSITION_FOLLOWING)) add("Action cache: lookup area must precede hit path");
          const vertices = (path) => {
            const d = path.getAttribute("d") ?? "";
            if (!d.trim() || (d.match(/M/g) ?? []).length !== 1) add(`Action cache: ${path.getAttribute("class")} must have one nonempty run`);
            return [...d.matchAll(/[ML]\s*(-?(?:\d+\.?\d*|\.\d+))[\s,]+(-?(?:\d+\.?\d*|\.\d+))/g)].map((match) => new DOMPoint(Number(match[1]), Number(match[2])).matrixTransform(path.getScreenCTM()));
          };
          const hits = vertices(run);
          const lookups = vertices(area);
          if (hits.length < 2 || lookups.length < hits.length + 2) add("Action cache: missing hit or lookup vertices");
          for (let i = 0; i < Math.min(hits.length, lookups.length); i++) {
            if (!equal(hits[i].x, lookups[i].x) || hits[i].y < lookups[i].y - EPS) add(`Action cache: hit vertex ${i} is not aligned below its lookup vertex`);
          }
          const areaBox = rect(area);
          if (getComputedStyle(area).fill === "none" || areaBox.width <= EPS || areaBox.height <= EPS) add("Action cache: lookup area has no visible fill");
        }
      }
    }
    if (labelRect && bounds) {
      const left = labelRect.left - bounds.left;
      const right = bounds.right - labelRect.right;
      if (!equal(left, right)) add(`${name}: insets differ ${left} vs ${right}`);
    }
    if (labelRect && chartRect && (!equal(labelRect.left, chartRect.left) || !equal(labelRect.right, chartRect.right))) add(`${name}: chart slot does not match label width`);
    if (chartRect && meterRect && (!equal(meterRect.left, chartRect.left) || !equal(meterRect.right, chartRect.right))) add(`${name}: meter does not match chart width`);
    const svgRects = svgs.map((svg) => {
      const box = rect(svg);
      if (labelRect && (!equal(box.left, labelRect.left) || !equal(box.right, labelRect.right))) add(`${name}: SVG does not match label width`);
      if (meterRect && (!equal(box.left, meterRect.left) || !equal(box.right, meterRect.right))) add(`${name}: SVG does not match meter width`);
      for (const circle of svg.querySelectorAll("circle")) {
        const marker = rect(circle);
        if (marker.width <= EPS || marker.height <= EPS) {
          add(`${name}: marker has no rendered size ${JSON.stringify(marker)}`);
          continue;
        }
        if (marker.left < box.left - EPS || marker.right > box.right + EPS || marker.top < box.top - EPS || marker.bottom > box.bottom + EPS) add(`${name}: marker ${JSON.stringify(marker)} escapes SVG ${JSON.stringify(box)}`);
        if (["Queue", "Executing"].includes(name)) isolatedMarkers++;
      }
      return box;
    });
    return { name, tile: bounds, label: labelRect, value: rect(value), meter: meterSlotRect, actualMeter: meterRect, caption: captionRect, chart: chartRect, svgs: svgRects };
  });
  if (scenario === "isolated" && isolatedMarkers < 4) add(`isolated: expected at least four Queue/Executing markers, got ${isolatedMarkers}`);
  const reference = tiles.find((tile) => tile.label)?.label;
  const referenceTile = tiles.find((tile) => tile.label)?.tile;
  if (reference && referenceTile) {
    const left = reference.left - referenceTile.left;
    const right = referenceTile.right - reference.right;
    for (const tile of tiles) if (tile.label && (!equal(tile.label.left - tile.tile.left, left) || !equal(tile.tile.right - tile.label.right, right))) add(`${tile.name}: insets differ from other tiles`);
  }
  const rows = [];
  for (const tile of tiles) {
    let row = rows.find((items) => equal(items[0].tile.top, tile.tile.top));
    if (!row) rows.push(row = []);
    row.push(tile);
  }
  rows.sort((a, b) => a[0].tile.top - b[0].tile.top);
  for (let index = 1; index < rows.length; index++) {
    const previousBottom = Math.max(...rows[index - 1].map((tile) => tile.tile.bottom));
    const nextTop = Math.min(...rows[index].map((tile) => tile.tile.top));
    if (!equal(previousBottom, nextTop)) add(`tile rows do not touch: bottom ${previousBottom}, next top ${nextTop}`);
  }
  for (const row of rows) {
    for (const role of ["label", "value", "meter", "caption", "chart"]) {
      const present = row.filter((tile) => tile[role]);
      if (present.length && present.some((tile) => !equal(tile[role].top, present[0][role].top))) add(`row ${row.map((tile) => tile.name).join("/")}: ${role} tops differ: ${present.map((tile) => `${tile.name}=${tile[role].top}`).join(", ")}`);
    }
    const svgs = row.flatMap((tile) => tile.svgs.map((svg) => ({ name: tile.name, top: svg.top })));
    if (svgs.length && svgs.some((svg) => !equal(svg.top, svgs[0].top))) add(`row SVG tops differ: ${svgs.map((svg) => `${svg.name}=${svg.top}`).join(", ")}`);
  }
  const charts = ["Transferred per step", "Evicted per step"].map((label) => {
    const svg = document.querySelector(`svg[aria-label="${label}"]`);
    if (!svg) { add(`Cache: missing ${label} SVG`); return null; }
    const frame = svg.closest(".chart-frame");
    const plot = frame?.querySelector(".chart-frame__plot") ?? svg;
    const svgBox = rect(svg), plotBox = rect(plot);
    const ctm = svg.getScreenCTM();
    if (!ctm) { add(`Cache: ${label} has no SVG screen CTM`); return null; }
    const point = (x, y) => new DOMPoint(x, y).matrixTransform(ctm);
    const start = point(2, 2), end = point(298, 42);
    return { label, svg: svgBox, plot: plotBox, frame: rect(frame ?? svg.parentElement), baseline: end.y,
      padding: [(start.x - plotBox.left) / plotBox.width, (plotBox.right - end.x) / plotBox.width,
        (start.y - plotBox.top) / plotBox.height, (plotBox.bottom - end.y) / plotBox.height] };
  });
  if (charts.every(Boolean)) {
    const [a, b] = charts;
    const stacked = equal(a.svg.left, b.svg.left);
    if (!equal(a.plot.width, b.plot.width) || !equal(a.plot.height, b.plot.height) ||
        !equal(a.svg.width, b.svg.width) || !equal(a.svg.height, b.svg.height)) add(`Cache plot dimensions differ: ${JSON.stringify(charts)}`);
    if (a.padding.some((value, index) => Math.abs(value - b.padding[index]) > 0.001)) add(`Cache normalized inner padding differs: ${JSON.stringify(charts)}`);
    if (!stacked && !equal(a.baseline, b.baseline)) add(`Cache SVG baselines differ: ${a.baseline} vs ${b.baseline}`);
    const sections = ["pass-h", "recent-h"].map((id) => document.getElementById(id)?.closest("section"));
    if (sections.every(Boolean)) {
      for (let i = 0; i < 2; i++) {
        const edge = rect(sections[i]);
        if (!equal(charts[i].frame.left, edge.left) || !equal(charts[i].frame.right, edge.right)) add(`Cache ${charts[i].label} edges do not match ${sections[i].textContent.trim().slice(0, 20)} section`);
      }
    } else add("Cache: missing Control pass or Scaling section");
  }
  const pageGrid = document.querySelector(".page-grid");
  let referenceEdges;
  for (const group of document.querySelectorAll(".layout-columns")) {
    const children = [...group.children];
    if (!pageGrid || children.length !== 2) { add("columns require a page grid and two children"); continue; }
    const page = rect(pageGrid), outer = rect(group), boxes = children.map(rect);
    if (!equal(outer.left, page.left) || !equal(outer.right, page.right)) add("columns do not span page grid");
    if (equal(boxes[0].left, boxes[1].left)) {
      if (boxes.some((box) => !equal(box.left, page.left) || !equal(box.right, page.right))) add("stacked column edges differ from page grid");
    } else {
      if (!equal(boxes[0].width, boxes[1].width)) add(`column widths differ: ${boxes[0].width} vs ${boxes[1].width}`);
      const edges = [boxes[0].left, boxes[0].right, boxes[1].left, boxes[1].right];
      if (!equal(edges[0], page.left) || !equal(edges[3], page.right)) add("column outer edges differ from page grid");
      if (referenceEdges && edges.some((edge, i) => !equal(edge, referenceEdges[i]))) add(`column edges differ: ${edges} vs ${referenceEdges}`);
      referenceEdges ??= edges;
    }
  }
  return { violations, tiles, clip: rect(container) };
};

test("overview tile geometry", { timeout: 120000 }, async () => {
  const server = previewServer({ dist: "dist", replay: "instant" });
  const profile = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), "overview-layout-chromium-"));
  let browser;
  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const origin = `http://127.0.0.1:${server.address().port}`;
    browser = await launch(join(process.env.JS_BINARY__RUNFILES, process.env.CHROMIUM), { profile });
    const page = await browser.newPage();
    await page.send("Page.enable");
    await page.send("Network.enable");
    await page.send("Runtime.enable");
    await page.send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-reduced-motion", value: "reduce" }] });
    const fixtureNow = load("live").at(-1).at;
    await page.send("Page.addScriptToEvaluateOnNewDocument", { source: `{ const offset = ${fixtureNow} - Date.now(); const now = Date.now.bind(Date); Date.now = () => now() + offset; }` });
    const violations = [];
    for (const scenario of ["live", "empty", "isolated"]) {
      const frameCount = load(scenario).length;
      for (const width of [...FULL_WIDTHS, ...FOCUSED_WIDTHS]) {
        await page.send("Network.clearBrowserCookies");
        await page.send("Emulation.setDeviceMetricsOverride", { width, height: 1000, deviceScaleFactor: 1, mobile: false });
        const frames = new Promise((resolve) => {
          let count = 0;
          const listener = () => {
            if (++count === frameCount) {
              page.off("Network.eventSourceMessageReceived", listener);
              resolve();
            }
          };
          page.on("Network.eventSourceMessageReceived", listener);
        });
        const loaded = page.once("Page.loadEventFired");
        await page.send("Page.navigate", { url: `${origin}/ui/?scenario=${scenario}` });
        await Promise.race([loaded, browser.closed]);
        await Promise.race([frames, browser.closed]);
        const result = await Promise.race([page.send("Runtime.evaluate", { expression: `(${probe})(${JSON.stringify(scenario)})`, awaitPromise: true, returnByValue: true }), browser.closed]);
        if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
        const { violations: found, tiles, clip } = result.result.value;
        for (const violation of found) violations.push(`${scenario}@${width}: ${violation}`);
        if (width === 1516) console.log(`overview ${scenario}@1516: ${JSON.stringify(tiles.map(({ name, tile, label, value, meter, caption, chart, svgs }) => ({ name, tileTop: tile.top, insets: label && [label.left - tile.left, tile.right - label.right], tops: [label?.top, value?.top, meter?.top, caption?.top, chart?.top, ...svgs.map((svg) => svg.top)], chartWidth: chart?.width })))}`);
        if (process.env.GANG_LAYOUT_SCREENSHOTS && process.env.TEST_UNDECLARED_OUTPUTS_DIR &&
            (!process.env.GANG_LAYOUT_SCREENSHOT_WIDTH || Number(process.env.GANG_LAYOUT_SCREENSHOT_WIDTH) === width)) {
          const screenshot = await page.send("Page.captureScreenshot", { format: "png" });
          writeFileSync(join(process.env.TEST_UNDECLARED_OUTPUTS_DIR, `overview-${scenario}-${width}.png`), Buffer.from(screenshot.data, "base64"));
          const shot = await page.send("Page.captureScreenshot", { format: "png", captureBeyondViewport: true, clip: { x: clip.left, y: clip.top, width: clip.width, height: clip.height, scale: 1 } });
          writeFileSync(join(process.env.TEST_UNDECLARED_OUTPUTS_DIR, `overview-${scenario}-${width}-tiles.png`), Buffer.from(shot.data, "base64"));
        }
      }
    }
    assert.deepEqual(violations, []);
  } finally {
    if (browser) await browser.close();
    server.closeAllConnections();
    if (server.listening) await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    rmSync(profile, { recursive: true, force: true });
  }
});
