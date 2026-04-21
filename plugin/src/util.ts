/**
 * Shared utility functions for the opencode-sync plugin.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { logger } from "./logger.js";

/**
 * Promisified setTimeout.
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Standard debounce — delays invocation until `ms` milliseconds have elapsed
 * since the last call. Trailing-edge only.
 */
export function debounce<T extends (...args: any[]) => void>(fn: T, ms: number): T {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const debounced = (...args: any[]) => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      // Handle async functions — catch unhandled rejections
      Promise.resolve(fn(...args)).catch((err) => {
        logger.error("debounced function error:", err);
      });
    }, ms);
  };
  return debounced as unknown as T;
}

/**
 * Compute SHA-256 hex digest using Bun's CryptoHasher.
 */
export function sha256Hex(data: string | Buffer | Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(data);
  return hasher.digest("hex");
}

/**
 * Write to a temporary file then atomically rename into place.
 * Ensures readers never see a partially-written file.
 */
/**
 * Synchronous atomic write: write to temp file then rename.
 * Used for critical state writes where we need guaranteed persistence.
 */
export function atomicWriteFileSync(filePath: string, data: string | Buffer): void {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, `.tmp-${path.basename(filePath)}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  try {
    fs.writeFileSync(tmp, data);
    fs.renameSync(tmp, filePath);
  } catch (err) {
    try { fs.unlinkSync(tmp); } catch { /* ignore */ }
    throw err;
  }
}

/**
 * Async atomic write: write to a temporary file then rename into place.
 */
export async function atomicWriteFile(filePath: string, data: string | Buffer): Promise<void> {
  const dir = path.dirname(filePath);
  const tmp = path.join(dir, `.tmp-${path.basename(filePath)}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  try {
    await fs.promises.writeFile(tmp, data);
    await fs.promises.rename(tmp, filePath);
  } catch (err) {
    // Clean up the temp file if the rename failed
    try {
      await fs.promises.unlink(tmp);
    } catch {
      // ignore cleanup errors
    }
    throw err;
  }
}

/**
 * Strip single-line (`//`) and block (`/* ... *\/`) comments from JSONC
 * so the result can be parsed with `JSON.parse`. Quoted strings are
 * passed through verbatim (handles escaped quotes correctly).
 *
 * Both the plugin config loader and the override loader need this — the
 * `jsonc-parser` npm package would do the same job but adds a
 * dependency for what's effectively 30 lines of state-machine.
 */
export function stripJsoncComments(text: string): string {
  let result = "";
  let i = 0;
  const len = text.length;

  while (i < len) {
    const ch = text[i]!;

    // Quoted string — pass through verbatim
    if (ch === '"') {
      let j = i + 1;
      while (j < len) {
        if (text[j] === "\\") {
          j += 2; // skip escaped char
        } else if (text[j] === '"') {
          j += 1;
          break;
        } else {
          j += 1;
        }
      }
      result += text.slice(i, j);
      i = j;
      continue;
    }

    // Single-line comment
    if (ch === "/" && text[i + 1] === "/") {
      const eol = text.indexOf("\n", i);
      i = eol === -1 ? len : eol; // skip to end of line (keep the \n)
      continue;
    }

    // Block comment
    if (ch === "/" && text[i + 1] === "*") {
      const end = text.indexOf("*/", i + 2);
      i = end === -1 ? len : end + 2;
      continue;
    }

    result += ch;
    i += 1;
  }

  return result;
}
