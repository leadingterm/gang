// The user code of a device login, typed one character per cell.
import { USER_CODE } from "./wire.ts";

/** One character of the code alphabet per cell, or `""` for an empty cell. */
export type Cells = readonly string[];
/** Cells after an edit, and the cell to focus next. */
export type Entry = { readonly cells: Cells; readonly focus: number };

/** `USER_CODE.length` empty cells. */
export const emptyCells = (): Cells => Array.from({ length: USER_CODE.length }, () => "");

/** Uppercases `text` and keeps only characters a code can contain. */
export const normalize = (text: string): string =>
  Array.from(text.toUpperCase()).filter((character) => USER_CODE.alphabet.includes(character)).join("");

/** Writes code characters at `index`, or fills the entire code for a complete paste. */
export const enter = (cells: Cells, index: number, text: string): Entry => {
  const characters = normalize(text);
  const next = [...cells];
  if (characters.length === 0) return { cells: next, focus: index };
  const start = characters.length >= USER_CODE.length ? 0 : index;
  const count = Math.min(characters.length, USER_CODE.length - start);
  for (let offset = 0; offset < count; offset++) next[start + offset] = characters.charAt(offset);
  return { cells: next, focus: Math.min(start + count, USER_CODE.length - 1) };
};

/** Clears the current cell, or the previous cell if the current one is empty. */
export const erase = (cells: Cells, index: number): Entry => {
  const next = [...cells];
  const focus = cells[index] || index === 0 ? index : index - 1;
  next[focus] = "";
  return { cells: next, focus };
};

/** The code as the server displays it, or `null` until every cell is filled. */
export const codeOf = (cells: Cells): string | null => {
  if (cells.length !== USER_CODE.length || cells.some((cell) => cell === "")) return null;
  const middle = USER_CODE.length / 2;
  return `${cells.slice(0, middle).join("")}-${cells.slice(middle).join("")}`;
};
