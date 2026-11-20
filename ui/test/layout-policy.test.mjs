import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { test } from "node:test";

const positional = /^(?:(?:min|max)-)?(?:width|height|block-size|inline-size)$|^(?:margin|padding|inset|scroll-margin|scroll-padding)(?:-.+)?$|^(?:top|right|bottom|left|gap|row-gap|column-gap|grid-template|grid-template-columns|grid-template-rows|grid-auto-columns|grid-auto-rows|grid|flex|flex-basis|translate|transform|outline-offset|text-indent)$/i;
const dimension = /(?:^|[^\w.#-])([+-]?(?:\d*\.\d+|\d+\.?\d*)(?:[eE][+-]?\d+)?)(px|cm|mm|q|in|pc|pt|em|rem|ex|rex|cap|rcap|ch|rch|ic|ric|lh|rlh|vw|vh|vi|vb|vmin|vmax|svw|svh|svi|svb|svmin|svmax|lvw|lvh|lvi|lvb|lvmin|lvmax|dvw|dvh|dvi|dvb|dvmin|dvmax|cqw|cqh|cqi|cqb|cqmin|cqmax)\b/gi;

// Inspect declarations inside nested rules; braces and semicolons inside strings/functions are not separators.
const violations = (css) => {
  css = css.replace(/\/\*[\s\S]*?\*\//g, "");
  const result = [];
  let start = 0, depth = 0, quote = "", parens = 0;
  const declaration = (text) => {
    const colon = text.indexOf(":");
    if (colon < 0) return;
    const property = text.slice(0, colon).trim();
    const value = text.slice(colon + 1).trim();
    if (property.startsWith("--") || !positional.test(property)) return;
    for (const match of value.matchAll(dimension)) {
      if (Number(match[1]) !== 0) result.push(`${property}: ${value}`);
    }
  };
  for (let i = 0; i < css.length; i++) {
    const ch = css[i];
    if (quote) {
      if (ch === "\\") i++;
      else if (ch === quote) quote = "";
    } else if (ch === '"' || ch === "'") quote = ch;
    else if (ch === "(") parens++;
    else if (ch === ")") parens--;
    else if (!parens && ch === "{") { depth++; start = i + 1; }
    else if (!parens && (ch === ";" || ch === "}")) {
      if (depth) declaration(css.slice(start, i));
      if (ch === "}") depth--;
      start = i + 1;
    }
  }
  return result;
};

const styles = (dir) => readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
  const path = join(dir, entry.name);
  return entry.isDirectory() ? styles(path) : entry.isFile() && path.endsWith(".css") ? [path] : [];
});

test("dimensional policy matcher rejects raw layout lengths", () => {
  assert.deepEqual(violations(`/* width: 9px */ @media (min-width: 10px) { .a { --size: 4px; width: var(--size); gap: 0; grid-template-columns: 1fr 50%; grid-column: 1 / 3; font-size: 12px; color: red; } }`), []);
  for (const css of [".a { width: 12px }", ".a { padding-inline: calc(1rem + 2%); }", ".a { transform: translateX(-.5em); }", ".a { grid-template-columns: 1fr minmax(0, 40ch); }", ".a { scroll-margin-top: 2vh }", ".a { flex: 1 1 240px }"]) {
    assert.notDeepEqual(violations(css), [], css);
  }
});

test("view styles use tokens for positional dimensions", () => {
  const failures = styles("src").filter((path) => relative("src", path) !== "tokens.css").flatMap((path) => violations(readFileSync(path, "utf8")).map((item) => `${path}: ${item}`));
  assert.deepEqual(failures, []);
});
