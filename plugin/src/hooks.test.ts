import { afterEach, expect, test } from "bun:test";
import * as os from "node:os";
import { createEventHandler } from "./hooks.js";
import type { SessionSync } from "./sessions.js";
import type { FileSync } from "./files.js";
import { clearHaltMarker, writeHaltMarker, HALT_REASONS } from "./halt.js";

// Same HOME guard as sessions.test.ts — hook tests touch the halt
// marker on disk and the marker lives under ~/.local/share/opencode/.
const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load hook tests against non-test HOME: ${HOME}.`,
  );
}

afterEach(() => {
  clearHaltMarker();
});

/**
 * Minimal SessionSync stub recording every call. Behaves like the
 * real instance for `isHalted()` (consults the on-disk marker) so
 * hook code paths that check halt state see realistic behaviour.
 */
function makeStubSync(opts?: { halted?: boolean }) {
  const calls: string[] = [];
  const stub: Partial<SessionSync> = {
    isHalted: () => opts?.halted ?? false,
    markExpectedDeletion: (rowKey: string) => {
      calls.push(`mark:${rowKey}`);
    },
    pushAll: async () => {
      calls.push("pushAll");
    },
    pushSession: async (sessionId: string) => {
      calls.push(`pushSession:${sessionId}`);
    },
    sync: async () => {
      calls.push("sync");
    },
  };
  return { stub: stub as SessionSync, calls };
}

const noopFileSync = {
  sync: async () => {},
} as unknown as FileSync;

test("session.deleted does NOT call markExpectedDeletion or pushAll while halted", async () => {
  // Regression: previously, a `session.deleted` event during the halt
  // window would still mark the deletion as expected. The mark
  // persisted in `expectedDeletions`, and when the user manually cleared
  // the marker (after restoring data, etc.), the next pushAll would
  // immediately fast-path a tombstone for that session — bypassing
  // both the two-cycle confirmation AND the threshold guard.
  //
  // Conservative behaviour: while halted, ignore deletion hints. Real
  // deletions will be re-detected via the missing-row scan after the
  // user clears the marker, and route through the normal confirmation
  // path.
  writeHaltMarker({
    triggeredAt: Date.now(),
    reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
    message: "test halt",
  });

  const { stub, calls } = makeStubSync({ halted: true });
  const handler = createEventHandler({
    sessionSync: stub,
    fileSync: noopFileSync,
    machineId: "desktop",
    log: () => {},
  });

  await handler({
    event: {
      type: "session.deleted",
      properties: { sessionId: "ses_doomed" },
    },
  });

  // Neither the mark nor the push should have run.
  expect(calls).toEqual([]);
});

test("session.deleted DOES call markExpectedDeletion + pushAll when not halted", async () => {
  // Sanity check the happy path still works after the halt-skip was added.
  const { stub, calls } = makeStubSync({ halted: false });
  const handler = createEventHandler({
    sessionSync: stub,
    fileSync: noopFileSync,
    machineId: "desktop",
    log: () => {},
  });

  await handler({
    event: {
      type: "session.deleted",
      properties: { sessionId: "ses_real" },
    },
  });

  expect(calls).toEqual(["mark:session:ses_real", "pushAll"]);
});

test("server.connected calls sync.sync() — relies on sync()'s entry guard for halt", async () => {
  // The hook itself doesn't gate on halt for `server.connected` — the
  // entry guard inside `SessionSync.sync()` does. This test just
  // verifies the hook still wires through to sync.sync() (so the entry
  // guard gets a chance to run). Halt-bypass coverage for sync.sync()
  // itself lives in sessions.test.ts.
  const { stub, calls } = makeStubSync({ halted: true });
  const handler = createEventHandler({
    sessionSync: stub,
    fileSync: noopFileSync,
    machineId: "desktop",
    log: () => {},
  });

  await handler({
    event: { type: "server.connected", properties: {} },
  });

  expect(calls).toEqual(["sync"]);
});
