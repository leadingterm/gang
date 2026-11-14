import assert from "node:assert/strict";
import { test } from "node:test";
import { codeOf, emptyCells, enter, erase, normalize } from "../src/user-code.ts";

// A dot represents an empty cell.
const cells = (text: string): readonly string[] => Array.from(text, (character) => character === "." ? "" : character);

test("normalization keeps only uppercase code characters", () => {
  assert.equal(normalize("bcdf-ghjk"), "BCDFGHJK");
  assert.equal(normalize(" BCDF  GHJK "), "BCDFGHJK");
  assert.equal(normalize("AEIOUY0123456789"), "");
  assert.equal(normalize("l"), "L");
});

test("empty cells match the code length", () => {
  assert.deepEqual(emptyCells(), cells("........"));
});

test("typing a valid character fills the cell and advances focus without mutating cells", () => {
  const original = cells("........");
  const result = enter(original, 2, "b");
  assert.deepEqual(result, { cells: cells("..B....."), focus: 3 });
  assert.deepEqual(original, cells("........"));
  assert.notEqual(result.cells, original);
});

test("typing an invalid character leaves the cells and focus unchanged", () => {
  for (const text of ["a", "1", "-"]) {
    const original = cells("..B.....");
    const result = enter(original, 2, text);
    assert.deepEqual(result, { cells: original, focus: 2 });
    assert.notEqual(result.cells, original);
  }
});

test("typing into a filled cell replaces it", () => {
  assert.deepEqual(enter(cells("..B....."), 2, "c"), { cells: cells("..C....."), focus: 3 });
});

test("pasting a complete code fills every cell regardless of focus", () => {
  assert.deepEqual(enter(cells("........"), 5, "bcdf-ghjk"), { cells: cells("BCDFGHJK"), focus: 7 });
  assert.deepEqual(enter(cells("........"), 5, "bcdf-ghjkxz"), { cells: cells("BCDFGHJK"), focus: 7 });
});

test("pasting a partial code fills onward and stops at the last cell", () => {
  assert.deepEqual(enter(cells("........"), 4, "GHJ"), { cells: cells("....GHJ."), focus: 7 });
  assert.deepEqual(enter(cells("........"), 6, "XZB"), { cells: cells("......XZ"), focus: 7 });
});

test("erasing a filled cell clears it and keeps focus without mutating cells", () => {
  const original = cells("..B.....");
  const result = erase(original, 2);
  assert.deepEqual(result, { cells: cells("........"), focus: 2 });
  assert.deepEqual(original, cells("..B....."));
  assert.notEqual(result.cells, original);
});

test("erasing an empty cell clears and focuses the previous cell", () => {
  assert.deepEqual(erase(cells("..B....."), 3), { cells: cells("........"), focus: 2 });
});

test("erasing an empty first cell leaves it and focus unchanged", () => {
  const original = cells("........");
  const result = erase(original, 0);
  assert.deepEqual(result, { cells: original, focus: 0 });
  assert.notEqual(result.cells, original);
});

test("codeOf formats a complete code and rejects a partial one", () => {
  assert.equal(codeOf(cells("BCDFGHJ.")), null);
  assert.equal(codeOf(cells("BCDFGHJK")), "BCDF-GHJK");
});
