/**
 * StateManager tests — focus on the corruption-handling path from M3
 * (backup + log instead of silent reset).
 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { StateManager } from "./state.js";

const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load state tests against non-test HOME: ${HOME}.`,
  );
}

const STATE_DIR = path.join(HOME, ".local", "share", "opencode", "opencode-sync");
const STATE_FILE = path.join(STATE_DIR, "state.json");

beforeEach(() => {
  fs.rmSync(STATE_DIR, { recursive: true, force: true });
});

afterEach(() => {
  fs.rmSync(STATE_DIR, { recursive: true, force: true });
});

test("M3: load() on missing state file is a no-op", () => {
  const sm = new StateManager("desktop");
  sm.load();
  expect(sm.state.lastPulledSeq).toBe(0);
});

test("M3: load() backs up a corrupt state.json instead of silently resetting", () => {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  fs.writeFileSync(STATE_FILE, '{"machineId":"desktop","lastPul'); // truncated JSON

  const sm = new StateManager("desktop");
  // Must not throw — corruption is a soft failure that resets to defaults.
  sm.load();

  // State reset to defaults.
  expect(sm.state.lastPulledSeq).toBe(0);
  expect(Object.keys(sm.state.knownRows)).toEqual([]);

  // A backup file must exist alongside (and the original moved away).
  const files = fs.readdirSync(STATE_DIR);
  const backups = files.filter((f) => f.startsWith("state.json.corrupt-"));
  expect(backups.length).toBe(1);
  // Backup contains the original corrupt bytes so the operator can inspect.
  const backupContent = fs.readFileSync(path.join(STATE_DIR, backups[0]!), "utf-8");
  expect(backupContent).toBe('{"machineId":"desktop","lastPul');
});

test("M3: load() tolerates partial JSON (missing fields default safely)", () => {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  // Valid JSON but only has one field — the rest should default.
  fs.writeFileSync(STATE_FILE, '{"lastPulledSeq": 42}');

  const sm = new StateManager("desktop");
  sm.load();

  expect(sm.state.lastPulledSeq).toBe(42);
  expect(sm.state.lastPushedRowTime).toBe(0);
  expect(Object.keys(sm.state.knownRows)).toEqual([]);
  expect(sm.state.rowParents).toEqual({});
  // No backup — this wasn't a parse failure.
  const files = fs.readdirSync(STATE_DIR);
  expect(files.filter((f) => f.startsWith("state.json.corrupt-"))).toEqual([]);
});

test("M6: pushReadSince detects wall-clock backjump and resets the cursor", () => {
  const sm = new StateManager("desktop");

  // Stamp the cursor far in the "future" relative to current Date.now()
  // — simulating a scenario where previous pushes happened under a
  // forward-skewed clock that has since been corrected backward.
  const now = Date.now();
  const farFuture = now + 30 * 60_000; // 30 min ahead
  // Bypass advancePushedRowTime's clamp by directly setting state via
  // a sequence that reaches the state through normal channels — the
  // cleanest way is to write state.json manually.
  // Use reflection through the public `state` getter's mutability.
  // NOTE: internal field access for testing only.
  (sm as unknown as { _state: { lastPushedRowTime: number } })._state.lastPushedRowTime = farFuture;

  // First call must detect the backjump and reset.
  const since = sm.pushReadSince();
  // After reset, lastPushedRowTime should be roughly now (±1s test jitter).
  expect(sm.state.lastPushedRowTime).toBeLessThan(farFuture);
  expect(sm.state.lastPushedRowTime).toBeGreaterThan(now - 1000);
  // pushReadSince returns the reset-time minus margin.
  expect(since).toBeLessThan(now);
});

test("M6: pushReadSince is unchanged under normal forward-clock conditions", () => {
  const sm = new StateManager("desktop");
  sm.advancePushedRowTime(Date.now() - 100); // recent, in the past
  const before = sm.state.lastPushedRowTime;
  sm.pushReadSince();
  // No reset — clock hasn't gone backward.
  expect(sm.state.lastPushedRowTime).toBe(before);
});

test("M3: save() then load() round-trips all fields", () => {
  const sm1 = new StateManager("desktop");
  sm1.updateSeq(123);
  sm1.rememberRows({ "session:s1": 1000, "message:m1": { time_updated: 1001, parent: "s1" } });
  sm1.save();

  const sm2 = new StateManager("desktop");
  sm2.load();
  expect(sm2.state.lastPulledSeq).toBe(123);
  expect(sm2.state.knownRows["session:s1"]).toBe(1000);
  expect(sm2.state.knownRows["message:m1"]).toBe(1001);
  expect(sm2.state.rowParents["message:m1"]).toBe("s1");
});
