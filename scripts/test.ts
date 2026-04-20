/**
 * Test runner — invokes `bun test` with HOME pinned to a sandbox directory.
 *
 * Why: several plugin tests write into `~/.config/opencode/` and
 * `~/.local/share/opencode/`. Running them against the developer's real
 * HOME would clobber live config. The test files have a module-level guard
 * that throws if HOME doesn't contain "opencode-sync-test-home", and this
 * runner exists so `bun test` from the repo root is safe by default.
 */
import { mkdirSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";

const TEST_HOME = process.env.OPENCODE_SYNC_TEST_HOME ?? join(tmpdir(), "opencode-sync-test-home");

// Wipe any leftover state from a previous run so tests start from a clean slate.
if (existsSync(TEST_HOME)) {
  rmSync(TEST_HOME, { recursive: true, force: true });
}
mkdirSync(TEST_HOME, { recursive: true });

const args = process.argv.slice(2);
const child = spawn("bun", ["test", ...args], {
  stdio: "inherit",
  env: { ...process.env, HOME: TEST_HOME },
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
