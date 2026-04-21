import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import {
  HALT_MARKER_PATH,
  HALT_REASONS,
  clearHaltMarker,
  isSyncHalted,
  readHaltDetails,
  writeHaltMarker,
} from "./halt.js";

// Test isolation: marker is sticky on disk by design (humans clear via
// `rm`). Tests must clean up explicitly so they don't poison each other.
const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load halt tests against non-test HOME: ${HOME}.`,
  );
}

afterEach(() => {
  clearHaltMarker();
});

test("isSyncHalted returns false when no marker exists", () => {
  expect(isSyncHalted()).toBe(false);
});

test("writeHaltMarker creates the marker file with a human-readable header", () => {
  writeHaltMarker({
    triggeredAt: 1_000_000,
    reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
    message: "test halt",
    candidateCount: 42,
    knownRowsSize: 100,
    sampleCandidates: ["session:foo", "message:bar"],
  });

  expect(isSyncHalted()).toBe(true);
  const text = fs.readFileSync(HALT_MARKER_PATH, "utf-8");
  // Header is for humans — must mention what to do.
  expect(text).toContain("HALTED");
  expect(text).toContain("rm ");
  // JSON section is for tooling.
  const jsonStart = text.indexOf("{");
  const parsed = JSON.parse(text.slice(jsonStart));
  expect(parsed.reason).toBe(HALT_REASONS.TOMBSTONE_THRESHOLD);
  expect(parsed.candidateCount).toBe(42);
});

test("readHaltDetails returns null when the marker is absent", () => {
  expect(readHaltDetails()).toBeNull();
});

test("readHaltDetails round-trips through writeHaltMarker", () => {
  const details = {
    triggeredAt: Date.now(),
    reason: HALT_REASONS.DB_FINGERPRINT_MISMATCH,
    message: "the database changed under us",
    extra: { weird: "stuff", nested: { ok: true } },
  };
  writeHaltMarker(details);
  const got = readHaltDetails();
  expect(got).not.toBeNull();
  expect(got!.reason).toBe(HALT_REASONS.DB_FINGERPRINT_MISMATCH);
  expect(got!.message).toBe("the database changed under us");
  expect(got!.extra).toEqual({ weird: "stuff", nested: { ok: true } });
});

test("writeHaltMarker overwrites stale details on re-trip (freshest wins)", () => {
  writeHaltMarker({
    triggeredAt: 1,
    reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
    message: "first",
  });
  writeHaltMarker({
    triggeredAt: 2,
    reason: HALT_REASONS.DB_FINGERPRINT_MISMATCH,
    message: "second",
  });
  const got = readHaltDetails()!;
  expect(got.message).toBe("second");
  expect(got.reason).toBe(HALT_REASONS.DB_FINGERPRINT_MISMATCH);
});

test("clearHaltMarker is a no-op when nothing is halted", () => {
  expect(() => clearHaltMarker()).not.toThrow();
});

test("clearHaltMarker removes a present marker", () => {
  writeHaltMarker({
    triggeredAt: Date.now(),
    reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
    message: "x",
  });
  expect(isSyncHalted()).toBe(true);
  clearHaltMarker();
  expect(isSyncHalted()).toBe(false);
});

test("readHaltDetails returns null on a corrupted marker (best-effort)", () => {
  // Write garbage that doesn't contain a JSON object — readHaltDetails
  // should fail gracefully rather than throwing on the boot path.
  fs.mkdirSync(require("node:path").dirname(HALT_MARKER_PATH), { recursive: true });
  fs.writeFileSync(HALT_MARKER_PATH, "no json in here at all");
  expect(readHaltDetails()).toBeNull();
  // But isSyncHalted is still true — the FILE exists, that's the signal.
  expect(isSyncHalted()).toBe(true);
});
