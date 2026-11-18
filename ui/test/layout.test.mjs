// Browser layout regressions across the fixture-backed dashboard screens.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { launch } from "../tools/chromium.mjs";
import { previewServer } from "../tools/preview-server.mjs";
import { load } from "./sse.ts";
import { FOCUSED_WIDTHS, FULL_WIDTHS } from "./layout-widths.mjs";

process.env.FONTCONFIG_FILE = join(process.env.JS_BINARY__RUNFILES, process.env.FONTCONFIG_FILE);

const build = JSON.parse(readFileSync("fixtures/build.json", "utf8")).name.slice("builds/".length);
const code = JSON.parse(readFileSync("fixtures/device-authorization.json", "utf8")).name
  .slice("deviceAuthorizations/".length).replaceAll("-", "");
const pages = [
  { name: "overview", path: "/ui/", open: ".tape", ready: ".tape__key", stream: "live" },
  { name: "actions", path: "/ui/actions", ready: "#run-h", stream: "live" },
  { name: "builds", path: "/ui/builds", ready: "#builds-h", stream: "live" },
  { name: "build", path: `/ui/builds/${build}`, ready: "#build-timeline", stream: "live" },
  { name: "workers", path: "/ui/workers", open: ".wrow__btn", ready: ".wdetail", stream: "live" },
  { name: "activity", path: "/ui/activity", open: ".listfoot .btn", ready: ".line__prose", stream: "live" },
  { name: "access", path: "/ui/access", ready: "#ro-h", stream: "live" },
  { name: "device", path: "/ui/device", ready: "#device-request-title", stream: null },
  { name: "forbidden", path: "/ui/?scenario=forbidden", ready: ".empty h2", stream: null },
  { name: "banner", path: "/ui/?scenario=stale", ready: ".banner", stream: "stale" },
  { name: "signedout", path: "/ui/?scenario=signedout", ready: ".gate__actions", stream: null },
  { name: "you", path: "/ui/access?scenario=long-session", ready: "#ro-h", stream: "live" },
];

const probe = async (ready, pageName) => {
  if (!document.querySelector(ready)) await new Promise((resolve) => {
    const observer = new MutationObserver(() => {
      if (document.querySelector(ready)) {
        observer.disconnect();
        resolve();
      }
    });
    observer.observe(document, { subtree: true, childList: true, attributes: true });
  });
  await document.fonts.ready;
  const families = ["Atkinson Hyperlegible Next", "Atkinson Hyperlegible Mono"];
  const sample = "The quick brown fox 0123456789";
  const loadedFonts = await Promise.all(families.map((family) => document.fonts.load(`16px "${family}"`, sample)));
  // Measure settled boxes: finite animations (e.g. a detail panel settling into place) end first.
  await Promise.all(document.getAnimations().filter((a) => a.effect?.getComputedTiming().iterations !== Infinity).map((a) => a.finished.catch(() => {})));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  const W = document.documentElement.clientWidth;
  const EPS = 0.5;
  const describe = (el) => `${el.tagName.toLowerCase()}${el.id ? `#${el.id}` : ""}${[...el.classList].map((c) => `.${c}`).join("")} "${el.textContent.trim().slice(0, 40)}"`;
  const styles = new WeakMap();
  const rectangles = new WeakMap();
  const hiddenCache = new WeakMap();
  const styleOf = (el) => {
    if (!styles.has(el)) styles.set(el, getComputedStyle(el));
    return styles.get(el);
  };
  const bounds = (el) => {
    if (!rectangles.has(el)) rectangles.set(el, el.getBoundingClientRect());
    return rectangles.get(el);
  };
  const hidden = (el) => {
    if (hiddenCache.has(el)) return hiddenCache.get(el);
    const style = styleOf(el);
    const rect = bounds(el);
    const result = style.display === "none" || style.visibility === "hidden" || (style.position === "absolute" && style.overflow === "hidden" && rect.width <= 1 && rect.height <= 1) || (el.parentElement ? hidden(el.parentElement) : false);
    hiddenCache.set(el, result);
    return result;
  };
  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d");
  const measure = (el, text) => {
    const style = styleOf(el);
    context.font = `${style.fontStyle} ${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    return context.measureText(text).width;
  };
  const naturalWidth = (el) => {
    const range = document.createRange();
    range.selectNodeContents(el);
    return range.getBoundingClientRect().width;
  };
  const skipped = (el) => {
    const rect = bounds(el);
    return hidden(el) || (styleOf(el).display !== "contents" && (rect.width === 0 || rect.height === 0));
  };
  const clips = (el) => el !== document.documentElement && el !== document.body && styleOf(el).overflowX !== "visible";
  const clip = (rect, ancestor) => {
    let left = rect.left;
    let right = rect.right;
    for (let parent = ancestor; parent && parent !== document.body; parent = parent.parentElement) {
      if (clips(parent)) {
        const parentBounds = bounds(parent);
        left = Math.max(left, parentBounds.left);
        right = Math.min(right, parentBounds.right);
      }
    }
    return right > left ? { left, right } : null;
  };
  const vertical = (el) => {
    let ancestor = el;
    while (ancestor.parentElement && styleOf(ancestor).display === "inline") ancestor = ancestor.parentElement;
    const { top, bottom } = bounds(ancestor);
    return { top, bottom };
  };
  const visible = (el) => {
    const rect = bounds(el);
    const span = clip(rect, el.parentElement);
    return span && { ...span, ...(styleOf(el).display === "inline" ? vertical(el) : { top: rect.top, bottom: rect.bottom }) };
  };
  const textVisible = (text) => {
    if (!text.textContent.trim()) return null;
    const parent = text.parentElement;
    if (hidden(parent)) return null;
    const range = document.createRange();
    range.selectNodeContents(text);
    const span = clip(range.getBoundingClientRect(), parent);
    return span && { ...span, ...vertical(parent) };
  };
  const textNodes = (el) => {
    const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    return nodes;
  };
  const visibleText = (el) => textNodes(el).map((text) => {
    const host = text.parentElement;
    const box = bounds(host);
    const cut = host.scrollWidth > host.clientWidth + EPS;
    const limit = box.right - (cut ? measure(host, "…") : 0) + EPS;
    const range = document.createRange();
    let shown = "";
    for (let i = 0; i < text.length; i++) {
      range.setStart(text, i);
      range.setEnd(text, i + 1);
      if (range.getBoundingClientRect().right > limit) break;
      shown += text.data[i];
    }
    return shown + (cut ? "…" : "");
  }).join("");
  const inkCache = new Map();
  const ink = (el) => {
    if (inkCache.has(el)) return inkCache.get(el);
    let result = null;
    const include = (rect) => {
      if (rect) result = result ? { left: Math.min(result.left, rect.left), right: Math.max(result.right, rect.right), top: Math.min(result.top, rect.top), bottom: Math.max(result.bottom, rect.bottom) } : rect;
    };
    if (!skipped(el)) include(visible(el));
    for (const child of el.childNodes) {
      if (child.nodeType === Node.ELEMENT_NODE) include(ink(child));
      else if (child.nodeType === Node.TEXT_NODE) include(textVisible(child));
    }
    inkCache.set(el, result);
    return result;
  };
  const lines = (el) => {
    const range = document.createRange();
    range.selectNodeContents(el);
    const rects = [...range.getClientRects()].filter((rect) => rect.width > 0).sort((a, b) => a.top - b.top);
    let count = 0;
    let bottom = -Infinity;
    for (const rect of rects) {
      if (rect.top >= bottom - EPS) {
        count++;
        bottom = rect.bottom;
      } else {
        bottom = Math.max(bottom, rect.bottom);
      }
    }
    return count;
  };
  const violations = [];
  let count = 0;
  const add = (message) => {
    count++;
    if (violations.length < 50) violations.push(message);
  };
  for (const [index, family] of families.entries()) {
    if (!loadedFonts[index].length || !document.fonts.check(`16px "${family}"`, sample)) add(`font ${family} is not loaded`);
  }
  const template = document.querySelector(".ident--split");
  if (template) {
    for (const [headText, tailText] of [["//", ":gang"], ["a", ":b"], ["ui", ":dist"]]) {
      const ident = template.cloneNode(true);
      ident.className = "ident ident--split target";
      ident.style.setProperty("width", "max-content");
      const head = ident.querySelector(".ident__head");
      const tail = ident.querySelector(".ident__tail");
      head.textContent = headText;
      tail.textContent = tailText;
      document.body.append(ident);
      const range = document.createRange();
      range.selectNodeContents(head);
      if (head.scrollWidth > head.clientWidth + EPS) add(`${headText}${tailText} short head is truncated`);
      const gap = tail.getBoundingClientRect().left - range.getBoundingClientRect().right;
      if (gap > EPS) add(`${headText}${tailText} has ${gap}px between its head and tail`);
      ident.remove();
    }
  }
  // The temporary clones are removed before any cached page geometry or style is read.
  const textRects = (el) => {
    const range = document.createRange();
    range.selectNodeContents(el);
    return [...range.getClientRects()].filter((rect) => rect.width > 0 && rect.height > 0).sort((a, b) => a.top - b.top || a.left - b.left);
  };
  const wraps = (rects) => rects.some((rect) => rect.top > rects[0].top + EPS);
  const failureCause = pageName === "activity"
    ? [...document.querySelectorAll(".event .event__cause.caution")].find((el) => !skipped(el.closest(".event")) && !hidden(el) && el.textContent.trim().startsWith("Could not launch Spot Instances."))
    : null;
  if (pageName === "activity" && !failureCause) add("Activity is missing the launch-failure cause");
  let failureRects = [];
  if (failureCause) {
    failureRects = textRects(failureCause);
    if (!wraps(failureRects)) {
      add(`${describe(failureCause)} does not wrap: line tops ${failureRects.map((rect) => rect.top).join(", ") || "(none)"}`);
    }
  }
  if (pageName === "activity" && ![...document.querySelectorAll(".event .line__prose")].some((el) => !hidden(el) && wraps(textRects(el)))) {
    add("Activity has no wrapped event prose");
  }
  for (const row of document.querySelectorAll(".event")) {
    if (skipped(row)) continue;
    const glyph = row.querySelector(".event__glyph");
    const time = row.querySelector(".event__time");
    if (!glyph || !time) {
      add(`${describe(row)} is missing ${!glyph ? ".event__glyph" : ".event__time"}`);
      continue;
    }
    const glyphRect = glyph.getBoundingClientRect();
    const timeTop = time.getBoundingClientRect().top;
    const timeBottom = timeTop + parseFloat(getComputedStyle(time).lineHeight);
    const center = (glyphRect.top + glyphRect.bottom) / 2;
    if (!Number.isFinite(timeBottom) || Math.abs(center - (timeTop + timeBottom) / 2) > EPS || glyphRect.top < timeTop - EPS || glyphRect.bottom > timeBottom + EPS) {
      add(`${describe(row)} glyph top=${glyphRect.top}, center=${center}, bottom=${glyphRect.bottom}; time first line=${timeTop}..${timeBottom}`);
    }
    if (failureCause && row.contains(failureCause) && failureRects.length) {
      const first = failureRects[0];
      if (first.top >= timeTop - EPS && first.top < timeBottom + EPS && (center < first.top - EPS || center > first.bottom + EPS)) {
        add(`${describe(row)} glyph center=${center}; cause first rect=${first.top}..${first.bottom}, time first line=${timeTop}..${timeBottom}`);
      }
    }
  }
  for (const option of document.querySelectorAll(".seg__opt")) {
    const svg = option.querySelector(":scope > svg");
    if (!svg || skipped(option)) continue;
    const bounds = option.getBoundingClientRect();
    const style = getComputedStyle(option);
    const top = bounds.top + parseFloat(style.borderTopWidth) + parseFloat(style.paddingTop);
    const bottom = bounds.bottom - parseFloat(style.borderBottomWidth) - parseFloat(style.paddingBottom);
    const icon = svg.getBoundingClientRect();
    const iconCenter = (icon.top + icon.bottom) / 2;
    const contentCenter = (top + bottom) / 2;
    if (Math.abs(iconCenter - contentCenter) > EPS) {
      add(`${describe(option)} icon center=${iconCenter} (${icon.top}..${icon.bottom}); content center=${contentCenter} (${top}..${bottom})`);
    }
  }
  const layoutChildren = (el) => [...el.children].flatMap((c) => getComputedStyle(c).display === "contents" ? layoutChildren(c) : [c]);
  for (const el of document.querySelectorAll("*")) {
    if (skipped(el)) continue;
    const rect = visible(el);
    if (rect && (rect.left < -EPS || rect.right > W + EPS)) add(`${describe(el)} spans x=${rect.left}..${rect.right}, outside the document's 0..${W}`);
    const display = getComputedStyle(el).display;
    if (["grid", "inline-grid", "flex", "inline-flex"].includes(display)) {
      const children = layoutChildren(el).filter((child) => !skipped(child) && !["absolute", "fixed"].includes(getComputedStyle(child).position));
      for (let i = 0; i < children.length; i++) for (let j = i + 1; j < children.length; j++) {
        const a = ink(children[i]);
        const b = ink(children[j]);
        if (a && b && Math.min(a.right, b.right) - Math.max(a.left, b.left) > EPS && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > EPS) {
          add(`${describe(children[i])} overlaps ${describe(children[j])} (in ${describe(el)})`);
        }
      }
    }
    if (el.matches(".ident, .target, time, .btn, .nav__link, .clause")) {
      const n = lines(el);
      if (n > 1) add(`${describe(el)} wraps onto ${n} lines`);
    }
  }
  for (const el of document.querySelectorAll("*")) {
    if (!hidden(el) && getComputedStyle(el).textOverflow === "ellipsis" && el.scrollWidth > el.clientWidth + EPS && !el.closest("[title]")) {
      add(`${describe(el)} truncates without a title`);
    }
  }
  // An identifier with a tail truncates only in its head (Ident).
  for (const el of document.querySelectorAll("*")) {
    if (hidden(el) || el.matches(".ident__head, .ident__tail") || getComputedStyle(el).textOverflow !== "ellipsis" || el.scrollWidth <= el.clientWidth + EPS) continue;
    const value = el.classList.contains("ident") ? el.textContent : (el.closest("[title]")?.title ?? "");
    if (value.lastIndexOf(":") > 0) add(`${describe(el)} truncates "${value}" at its end, hiding the part after its last ":"`);
  }
  for (const shead of document.querySelectorAll(".shead")) {
    const next = shead.nextElementSibling;
    if (hidden(shead) || !next || hidden(next) || !next.textContent.trim()) continue;
    const nextInk = ink(next);
    if (!nextInk) continue;
    const gap = nextInk.top - shead.getBoundingClientRect().bottom;
    if (gap < 8 - EPS) add(`${describe(next)} sits ${gap}px under ${describe(shead)}`);
  }
  for (const line of document.querySelectorAll(".line")) {
    if (hidden(line)) continue;
    // A .line is one row of identifier and marks; only its prose may wrap.
    for (const child of line.children) {
      if (hidden(child) || child.matches(".line__prose") || !child.textContent.trim()) continue;
      const n = lines(child);
      if (n > 1) add(`${describe(child)} wraps onto ${n} lines inside ${describe(line)}`);
    }
    let previous = null;
    let space = false;
    for (const node of line.childNodes) {
      if (node.nodeType === Node.TEXT_NODE) {
        if (/\s/.test(node.textContent)) space = true;
      } else if (node.nodeType === Node.ELEMENT_NODE && node.textContent.trim()) {
        if (previous && !space && !/\s$/.test(previous.textContent) && !/^\s/.test(node.textContent)) {
          add(`${describe(line)} joins "${previous.textContent.slice(0, 20)}" and "${node.textContent.slice(0, 20)}" without a space`);
        }
        previous = node;
        space = false;
      }
    }
  }
  for (const el of document.querySelectorAll(".ident, .target, time, .endpoint")) {
    if (hidden(el) || !el.textContent.trim()) continue;
    const span = clip(el.getBoundingClientRect(), el.parentElement);
    const shown = span ? span.right - span.left : 0;
    const endpoint = el.matches(".endpoint");
    const min = Math.min(naturalWidth(el), measure(el, endpoint ? "0000000000000000" : "000"));
    if (shown + EPS < min) add(`${describe(el)} is ${shown}px wide, below ${min}px (${endpoint ? "16ch" : "3ch"} or its whole text)`);
  }
  const headMetrics = [];
  for (const el of document.querySelectorAll(".ident__head")) {
    if (hidden(el) || el.scrollWidth <= el.clientWidth + EPS) continue;
    const room = measure(el, `${el.textContent.trim()[0]}…`);
    const width = el.getBoundingClientRect().width;
    const margin = width - room;
    const minimumMargin = ["access", "you"].includes(pageName) && window.innerWidth === 320 ? 1 : -EPS;
    if (margin < minimumMargin) add(`${describe(el)} truncates in ${width}px, less than ${minimumMargin}px beyond its first glyph and ellipsis (${room}px)`);
    headMetrics.push({ width, glyphEllipsis: room, margin });
  }
  if (["access", "you"].includes(pageName) && window.innerWidth === 320 && !headMetrics.length) add(`${pageName}@320 has no truncated identifier head`);
  for (const el of document.querySelectorAll(".line__prose")) {
    if (hidden(el) || !el.textContent.trim()) continue;
    const span = clip(el.getBoundingClientRect(), el.parentElement);
    const shown = span ? span.right - span.left : 0;
    const min = Math.min(naturalWidth(el), measure(el, "000000000000"));
    if (shown + EPS < min) add(`${describe(el)} is ${shown}px wide, below ${min}px (12ch or its whole text)`);
  }
  const identifiers = new Map();
  for (const el of document.querySelectorAll(".ident")) {
    if (hidden(el)) continue;
    const list = el.closest("table, ul, ol, dl, .wgrid, .chips") ?? document;
    if (!identifiers.has(list)) identifiers.set(list, new Map());
    const groups = identifiers.get(list);
    const shown = visibleText(el);
    if (!groups.has(shown)) groups.set(shown, new Map());
    const titles = groups.get(shown);
    if (titles.has(el.title)) continue;
    for (const [title, other] of titles) {
      if (title !== el.title) add(`identifiers "${title}" and "${el.title}" both render as "${shown}" (${describe(other)}, ${describe(el)})`);
    }
    titles.set(el.title, el);
  }
  for (const text of textNodes(document.body)) {
    const parent = text.parentElement;
    if (!hidden(parent) && !parent.closest("time, title, script, style")) {
      const match = text.data.match(/\b\d+[smhd](?: \d{2}[smh])?\b|\b\d{2}:\d{2}:\d{2}\b|\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}\b/);
      if (match) add(`time "${match[0]}" in ${describe(parent)} is not a <time>`);
    }
    const rect = textVisible(text);
    if (rect && (rect.left < -EPS || rect.right > W + EPS)) {
      add(`text "${text.textContent.trim().slice(0, 40)}" in ${describe(text.parentElement)} spans x=${rect.left}..${rect.right}, outside the document's 0..${W}`);
    }
  }
  if (count > 50) violations.push(`… ${count - 50} more`);
  const seen = Object.fromEntries([".line__prose", ".ident--split", ".ident__head", ".clause", ".cols", ".shead", ".table", ".line", ".seg__opt > svg"].map((selector) => [selector, [...document.querySelectorAll(selector)].filter((el) => !hidden(el)).length]));
  const elements = [...document.querySelectorAll("*")];
  const sansIndex = elements.findIndex((el) => !el.children.length && !skipped(el) && getComputedStyle(el).fontFamily.includes("Atkinson Hyperlegible Next") && [...el.childNodes].some((node) => node.nodeType === Node.TEXT_NODE && textVisible(node)));
  // clientWidth can include a stable reserved gutter when no scrollbar is present.
  return { contentWidth: document.body.getBoundingClientRect().width, violations, seen, families, headMetrics,
    visibleHeads: [...document.querySelectorAll(".ident__head")].map((el, index) => !skipped(el) ? index : -1).filter((index) => index >= 0), sansIndex };
};

test("dashboard layouts", { timeout: 120000 }, async () => {
  const server = previewServer({ dist: "dist", replay: "instant" });
  const profile = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), "layout-chromium-"));
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
    await page.send("DOM.enable");
    await page.send("CSS.enable");
    await page.send("Accessibility.enable");
    await page.send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-reduced-motion", value: "reduce" }] });
    // The fixture's session expires minutes after its last frame; a wall clock would renew it, and a renewal reconnects mid-test.
    const fixtureNow = load("live").at(-1).at;
    await page.send("Page.addScriptToEvaluateOnNewDocument", { source: `{ const offset = ${fixtureNow} - Date.now(); const now = Date.now.bind(Date); Date.now = () => now() + offset; }` });
    const violations = [];
    const seen = {};
    for (const width of [...FULL_WIDTHS, ...FOCUSED_WIDTHS]) {
      const focused = !FULL_WIDTHS.includes(width);
      const widths = [];
      for (const screen of pages) {
        if (focused && screen.name !== "activity" && screen.name !== "access") continue;
        await page.send("Network.clearBrowserCookies");
        await page.send("Emulation.setDeviceMetricsOverride", { width, height: 1000, deviceScaleFactor: 1, mobile: false });
        let frames;
        if (screen.stream) frames = new Promise((resolve) => {
          const frameCount = load(screen.stream).length;
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
        await page.send("Page.navigate", { url: origin + screen.path });
        await Promise.race([loaded, browser.closed]);
        if (frames) await Promise.race([frames, browser.closed]);
        if (screen.open) {
          const opened = await page.send("Runtime.evaluate", { expression: `document.querySelector(${JSON.stringify(screen.open)}).click()` });
          if (opened.exceptionDetails) throw new Error(opened.exceptionDetails.exception?.description ?? opened.exceptionDetails.text);
        }
        if (screen.name === "device") {
          const focused = await page.send("Runtime.evaluate", { expression: 'document.querySelector("main input").focus()' });
          if (focused.exceptionDetails) throw new Error(focused.exceptionDetails.exception?.description ?? focused.exceptionDetails.text);
          await page.send("Input.insertText", { text: code });
          await page.send("Input.dispatchKeyEvent", { type: "keyDown", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13, text: "\r" });
          await page.send("Input.dispatchKeyEvent", { type: "keyUp", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13 });
        }
        const result = await Promise.race([page.send("Runtime.evaluate", { expression: `(${probe})(${JSON.stringify(screen.ready)}, ${JSON.stringify(screen.name)})`, awaitPromise: true, returnByValue: true }), browser.closed]);
        if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
        widths.push(`${screen.name} ${result.result.value.contentWidth}`);
        for (const violation of result.result.value.violations) violations.push(`${screen.name}@${width}: ${violation}`);
        for (const [selector, count] of Object.entries(result.result.value.seen)) seen[selector] = (seen[selector] ?? 0) + count;
        const { root } = await page.send("DOM.getDocument", { depth: -1 });
        const { nodeIds: headIds } = await page.send("DOM.querySelectorAll", { nodeId: root.nodeId, selector: ".ident__head" });
        const actualFonts = [];
        const auditFonts = (fonts, role, expected) => {
          const rendered = fonts.filter((font) => font.glyphCount > 0);
          if (!rendered.length) violations.push(`${screen.name}@${width}: ${role} has no rendered glyphs`);
          for (const font of rendered) {
            if (!font.isCustomFont || font.familyName !== expected) {
              violations.push(`${screen.name}@${width}: ${role} uses ${font.familyName} (custom=${font.isCustomFont}), want custom ${expected}`);
            }
            if (["access", "you"].includes(screen.name) && width === 320) {
              actualFonts.push({ role, familyName: font.familyName, isCustomFont: font.isCustomFont, glyphCount: font.glyphCount });
            }
          }
        };
        const headFonts = await Promise.all(result.result.value.visibleHeads.map((index) =>
          page.send("CSS.getPlatformFontsForNode", { nodeId: headIds[index] })));
        for (const { fonts } of headFonts) auditFonts(fonts, "identifier head", "Atkinson Hyperlegible Mono ExtraLight");
        const { nodeIds: elementIds } = await page.send("DOM.querySelectorAll", { nodeId: root.nodeId, selector: "*" });
        const sansIndex = result.result.value.sansIndex;
        if (sansIndex < 0) violations.push(`${screen.name}@${width}: no visible sans text`);
        else {
          const { fonts } = await page.send("CSS.getPlatformFontsForNode", { nodeId: elementIds[sansIndex] });
          auditFonts(fonts, "sans text", "Atkinson Hyperlegible Next");
        }
        if (["access", "you"].includes(screen.name) && width === 320) {
          actualFonts.sort((a, b) => a.role.localeCompare(b.role) || a.familyName.localeCompare(b.familyName) || Number(a.isCustomFont) - Number(b.isCustomFont) || a.glyphCount - b.glyphCount);
          const label = screen.name === "access" ? "Access" : "You";
          console.log(`${label}@320 fonts/layout: ${JSON.stringify({ families: result.result.value.families, actualFonts, heads: result.result.value.headMetrics })}`);
        }
        const { nodes } = await page.send("Accessibility.getFullAXTree");
        const axById = new Map(nodes.map((node) => [node.backendDOMNodeId, node]));
        const elements = [];
        let tables = 0;
        const textOf = (node) => node.nodeType === 3 ? node.nodeValue : (node.children ?? []).map(textOf).join("");
        const walk = (node, table = null) => {
          const tag = node.localName;
          const attrs = node.attributes ?? [];
          const classIndex = attrs.indexOf("class");
          const isTable = node.nodeType === 1 && tag === "table" && classIndex >= 0 && attrs[classIndex + 1].split(/\s+/).includes("table");
          if (isTable) table = tables++;
          if (node.nodeType === 1 && (isTable || (table !== null && ["tr", "th", "td"].includes(tag)))) {
            elements.push({ id: node.backendNodeId, tag, table, text: textOf(node).trim().slice(0, 40) });
          }
          for (const child of node.children ?? []) walk(child, table);
        };
        walk(root);
        const tableViolations = new Map();
        for (const { id, tag, table, text } of elements) {
          const expected = { table: "table", tr: "row", th: "columnheader", td: "cell" }[tag];
          const ax = axById.get(id);
          if (ax && !ax.ignored && ax.role?.value === expected) continue;
          const count = tableViolations.get(table) ?? 0;
          if (count < 5) violations.push(`${screen.name}@${width}: <${tag}> "${text}" is exposed as ${!ax || ax.ignored ? "ignored" : ax.role?.value} (want ${expected})`);
          tableViolations.set(table, count + 1);
        }
        if (screen.name !== "banner") {
          const banner = await page.send("Runtime.evaluate", { expression: 'document.querySelector(".banner")?.textContent ?? ""', returnByValue: true });
          if (banner.result.value) violations.push(`${screen.name}@${width}: a banner appeared: "${banner.result.value}"`);
        }
        // Opt in so plain runs leave no outputs for Bazel to post-process.
        if (process.env.GANG_LAYOUT_SCREENSHOTS && process.env.TEST_UNDECLARED_OUTPUTS_DIR) {
          const { cssContentSize } = await page.send("Page.getLayoutMetrics");
          // Beyond-viewport capture mis-renders tall pages; 3000px bounds the long Actions list.
          await page.send("Emulation.setDeviceMetricsOverride", { width, height: Math.min(cssContentSize.height, 3000), deviceScaleFactor: 1, mobile: false });
          await page.send("Runtime.evaluate", { expression: "new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))", awaitPromise: true });
          const screenshot = await page.send("Page.captureScreenshot", { format: "png" });
          writeFileSync(join(process.env.TEST_UNDECLARED_OUTPUTS_DIR, `${screen.name}-${width}.png`), Buffer.from(screenshot.data, "base64"));
        }
      }
      if (new Set(widths.map((value) => value.split(" ").at(-1))).size > 1) violations.push(`gutter@${width}: the content width differs between pages: ${widths.join(", ")}`);
    }
    for (const [selector, count] of Object.entries(seen)) {
      if (count === 0) violations.push(`coverage: no screen rendered ${selector}`);
    }
    assert.deepEqual(violations, []);
  } finally {
    if (browser) await browser.close();
    server.closeAllConnections();
    if (server.listening) await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    rmSync(profile, { recursive: true, force: true });
  }
});
