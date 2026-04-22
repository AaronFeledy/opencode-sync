import { afterEach, beforeEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { applyLoadedOverrides } from "./overrides.js";

// Module-level guard — throws at import time if HOME is not a test sandbox.
// This runs BEFORE any test body, so a misconfigured HOME can never reach a
// `writeOverrides(...)` call that would clobber the developer's real overrides.
const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load overrides tests against non-test HOME: ${HOME}. ` +
      `Run via the workspace 'bun test' script (which sets HOME), or export ` +
      `HOME to a directory containing 'opencode-sync-test-home'.`,
  );
}

const OVERRIDES_PATH = path.join(
  HOME,
  ".config",
  "opencode",
  "opencode-sync.overrides.jsonc",
);

function writeOverrides(contents: string): void {
  fs.mkdirSync(path.dirname(OVERRIDES_PATH), { recursive: true });
  fs.writeFileSync(OVERRIDES_PATH, contents);
}

beforeEach(() => {
  if (!os.homedir().includes("opencode-sync-test-home")) {
    throw new Error(`HOME changed mid-suite to a non-test path: ${os.homedir()}`);
  }
});

afterEach(() => {
  fs.rmSync(path.join(HOME, ".config", "opencode"), { recursive: true, force: true });
});

test("applyLoadedOverrides deep-merges nested objects, preserving sibling keys", () => {
  writeOverrides('{"server":{"port":4096},"model":"anthropic/test-model"}\n');

  const config: Record<string, unknown> = {
    server: { port: 3000, host: "127.0.0.1" },
    model: "openai/base",
    theme: "dark",
  };

  const applied = applyLoadedOverrides(config);

  expect(applied).toBe(true);
  // server.host is preserved because deep merge keeps sibling keys that the
  // override doesn't mention. Only server.port is replaced.
  expect(config).toEqual({
    server: { port: 4096, host: "127.0.0.1" },
    model: "anthropic/test-model",
    theme: "dark",
  });
});

test("applyLoadedOverrides is a no-op when no overrides file exists", () => {
  const config: Record<string, unknown> = { model: "openai/base" };

  const applied = applyLoadedOverrides(config);

  expect(applied).toBe(false);
  expect(config).toEqual({ model: "openai/base" });
});

test("applyLoadedOverrides replaces arrays wholesale (no concatenation)", () => {
  writeOverrides('{"plugins":["a","b"]}\n');

  const config: Record<string, unknown> = {
    plugins: ["x", "y", "z"],
  };

  applyLoadedOverrides(config);

  expect(config).toEqual({ plugins: ["a", "b"] });
});

test("applyLoadedOverrides recurses through multiple nesting levels", () => {
  writeOverrides('{"a":{"b":{"c":2}}}\n');

  const config: Record<string, unknown> = {
    a: { b: { c: 1, d: 99 }, e: "kept" },
    f: "untouched",
  };

  applyLoadedOverrides(config);

  expect(config).toEqual({
    a: { b: { c: 2, d: 99 }, e: "kept" },
    f: "untouched",
  });
});

test("applyLoadedOverrides treats explicit null as a replacement", () => {
  writeOverrides('{"model":null}\n');

  const config: Record<string, unknown> = { model: "openai/base" };

  applyLoadedOverrides(config);

  expect(config).toEqual({ model: null });
});

test("M4: applyLoadedOverrides ignores __proto__ keys (prototype-pollution guard)", () => {
  writeOverrides('{"__proto__":{"polluted":true},"model":"keep"}\n');

  const config: Record<string, unknown> = { model: "base" };
  applyLoadedOverrides(config);

  expect(({} as Record<string, unknown>)["polluted"]).toBeUndefined();
  expect((config as Record<string, unknown>)["polluted"]).toBeUndefined();
  expect(config.model).toBe("keep");
});

test("M4: applyLoadedOverrides ignores constructor/prototype keys", () => {
  writeOverrides('{"constructor":{"prototype":{"x":1}},"prototype":{"y":2}}\n');
  const config: Record<string, unknown> = {};
  applyLoadedOverrides(config);
  expect(({} as Record<string, unknown>)["x"]).toBeUndefined();
  expect(({} as Record<string, unknown>)["y"]).toBeUndefined();
});
