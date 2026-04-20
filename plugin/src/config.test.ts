import { afterEach, beforeEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { loadPluginConfig } from "./config.js";

// Module-level guard — throws at import time if HOME is not a test sandbox.
// This runs BEFORE any test body, so a misconfigured HOME can never reach a
// `writeConfig(...)` call that would clobber the developer's real config.
const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load config tests against non-test HOME: ${HOME}. ` +
      `Run via the workspace 'bun test' script (which sets HOME), or export ` +
      `HOME to a directory containing 'opencode-sync-test-home'.`,
  );
}

const CONFIG_PATH = path.join(HOME, ".config", "opencode", "opencode-sync.jsonc");

function writeConfig(contents: string): void {
  fs.mkdirSync(path.dirname(CONFIG_PATH), { recursive: true });
  fs.writeFileSync(CONFIG_PATH, contents);
}

// Run guard again per-test in case something mutates HOME mid-suite.
beforeEach(() => {
  if (!os.homedir().includes("opencode-sync-test-home")) {
    throw new Error(`HOME changed mid-suite to a non-test path: ${os.homedir()}`);
  }
});

afterEach(() => {
  fs.rmSync(path.join(HOME, ".config", "opencode"), { recursive: true, force: true });
  delete process.env.OPENCODE_SYNC_SERVER_URL;
  delete process.env.OPENCODE_SYNC_TOKEN;
  delete process.env.TEST_OPENCODE_SYNC_TOKEN;
});

test("requires a token during plugin startup", () => {
  writeConfig('{"server_url":"http://127.0.0.1:4455"}\n');

  expect(() => loadPluginConfig()).toThrow(/"token" must be set/);
});

test("loads token from token_env", () => {
  process.env.TEST_OPENCODE_SYNC_TOKEN = "secret-token";
  writeConfig('{"server_url":"http://127.0.0.1:4455","token_env":"TEST_OPENCODE_SYNC_TOKEN"}\n');

  const config = loadPluginConfig();

  expect(config.token).toBe("secret-token");
});
