/**
 * Per-machine override merging for config files.
 *
 * Reads ~/.config/opencode/opencode-sync.overrides.jsonc and shallow-merges
 * the overrides over a base config object. This lets each machine customise
 * synced config files without creating conflicts.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { stripJsoncComments } from "./util.js";

// ── Public API ─────────────────────────────────────────────────────

const OVERRIDES_PATH = path.join(
  os.homedir(),
  ".config",
  "opencode",
  "opencode-sync.overrides.jsonc",
);

/**
 * Load per-machine overrides from disk. Returns null if the file
 * doesn't exist or can't be parsed.
 */
export function loadOverrides(): Record<string, unknown> | null {
  if (!fs.existsSync(OVERRIDES_PATH)) return null;

  try {
    const text = fs.readFileSync(OVERRIDES_PATH, "utf-8");
    return JSON.parse(stripJsoncComments(text)) as Record<string, unknown>;
  } catch {
    return null;
  }
}

/**
 * Returns true if the value is a plain object (not array, null, Date, etc.)
 * suitable for recursive merging.
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  if (Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === null || proto === Object.prototype;
}

/**
 * Deep-merge `overrides` over `base`. Returns a new object; neither input is
 * mutated.
 *
 * Semantics:
 * - For nested plain objects, recursively merge so sibling keys are preserved
 *   (e.g. `{server:{port:X}}` overrides only `server.port`, leaves
 *   `server.host` intact).
 * - For arrays and non-plain values (Date, etc.), the override REPLACES the
 *   base value — concatenation would be surprising and lossy for most config
 *   shapes.
 * - `undefined` in overrides is treated as "no override" and the base value
 *   is kept; explicit `null` REPLACES the base value.
 */
export function applyOverrides(
  base: Record<string, unknown>,
  overrides: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = { ...base };

  for (const key of Object.keys(overrides)) {
    const overrideValue = overrides[key];
    if (overrideValue === undefined) continue;

    const baseValue = result[key];
    if (isPlainObject(baseValue) && isPlainObject(overrideValue)) {
      result[key] = applyOverrides(baseValue, overrideValue);
    } else {
      result[key] = overrideValue;
    }
  }

  return result;
}

/**
 * Load local-only overrides and deep-merge them into the in-memory config
 * object. Returns true if any overrides were applied. Mutates `base` in
 * place so callers passing a live config reference see the changes.
 */
export function applyLoadedOverrides(base: Record<string, unknown>): boolean {
  const overrides = loadOverrides();
  if (!overrides) return false;

  const merged = applyOverrides(base, overrides);

  // Replace top-level keys in `base` with the merged result. We do NOT
  // delete keys that aren't in `merged` — those are base-only and should
  // remain. Object.assign would leave deleted base keys behind, which is
  // exactly the behaviour we want here.
  for (const key of Object.keys(merged)) {
    base[key] = merged[key];
  }

  return Object.keys(overrides).length > 0;
}
