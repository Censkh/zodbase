/** A code point's lowercase one where that is a single code point, as databases fold case. */
const fold = (char: string) => {
  const lower = char.toLowerCase();
  return Array.from(lower).length === 1 ? lower : char;
};

// The code points that fold to a letter without being its uppercase one: title-case digraphs,
// Greek letters with iota subscripts, and the Kelvin, Ångström, Ohm and capital sharp s signs.
const UNPAIRED = Array.from(
  "\u{1c5}\u{1c8}\u{1cb}\u{1f2}\u{3f4}\u{1e9e}\u{1f88}\u{1f89}\u{1f8a}\u{1f8b}\u{1f8c}\u{1f8d}\u{1f8e}\u{1f8f}\u{1f98}\u{1f99}\u{1f9a}\u{1f9b}\u{1f9c}\u{1f9d}\u{1f9e}\u{1f9f}\u{1fa8}\u{1fa9}\u{1faa}\u{1fab}\u{1fac}\u{1fad}\u{1fae}\u{1faf}\u{1fbc}\u{1fcc}\u{1ffc}\u{2126}\u{212a}\u{212b}",
);
let unpaired: Map<string, string[]> | undefined;

/** Every code point that folds to the same one as `char`, `char` among them. */
const caseVariants = (char: string): string[] => {
  unpaired ??= UNPAIRED.reduce((map, variant) => {
    const folded = fold(variant);
    map.set(folded, [...(map.get(folded) ?? []), variant]);
    return map;
  }, new Map<string, string[]>());
  const folded = fold(char);
  const upper = folded.toUpperCase();
  const variants = new Set([folded, char, ...(unpaired.get(folded) ?? [])]);
  if (Array.from(upper).length === 1 && fold(upper) === folded) variants.add(upper);
  return [...variants];
};

/**
 * A GLOB pattern matching `pattern` case-insensitively, across every alphabet: SQLite's LIKE and
 * LOWER fold only ASCII letters, but GLOB, which compares exactly, takes classes of characters, so
 * each letter becomes the class of its case variants. `*` stays a wildcard when `wildcard` is
 * set; every other character, GLOB's `?` and `[` among them, matches itself.
 */
export const caseInsensitiveGlob = (pattern: string, wildcard: boolean): string =>
  Array.from(pattern, (char) => {
    if (char === "*" && wildcard) return "*";
    const variants = caseVariants(char);
    if (variants.length > 1) return `[${variants.join("")}]`;
    return char === "*" || char === "?" || char === "[" ? `[${char}]` : char;
  }).join("");
