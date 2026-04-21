/**
 * Event hook wiring — maps opencode plugin events to sync actions.
 *
 * The event handler is registered as the `event` hook and receives
 * every event emitted by the opencode server process.
 */
import type { SessionSync } from "./sessions.js";
import type { FileSync } from "./files.js";
import { debounce } from "./util.js";

// ── Types ──────────────────────────────────────────────────────────

export interface HookContext {
  /** Getter that returns null if DB isn't available yet */
  readonly sessionSync: SessionSync | null;
  fileSync: FileSync;
  machineId: string;
  log: (msg: string, data?: Record<string, unknown>) => void;
}

interface PluginEvent {
  type: string;
  properties?: Record<string, unknown>;
}

// ── Event handler factory ──────────────────────────────────────────

export function createEventHandler(ctx: HookContext): (input: { event: any }) => Promise<void> {
  const { fileSync, log } = ctx;

  // ── Debounced session push (2s) ──
  const pendingSessionIds = new Set<string>();

  const flushPendingSessions = debounce(async () => {
    const sync = ctx.sessionSync;
    if (!sync) return;

    const ids = [...pendingSessionIds];
    pendingSessionIds.clear();

    for (const id of ids) {
      try {
        await sync.pushSession(id);
      } catch (err) {
        log("failed to push session", { sessionId: id, error: String(err) });
      }
    }
  }, 2_000);

  function queueSessionPush(sessionId: string): void {
    pendingSessionIds.add(sessionId);
    flushPendingSessions();
  }

  // ── Debounced file sync (30s) ──
  const debouncedFileSync = debounce(async () => {
    try {
      await fileSync.sync();
    } catch (err) {
      log("file sync error", { error: String(err) });
    }
  }, 30_000);

  // ── The actual handler ──

  return async (input: { event: any }) => {
    const event = input.event as PluginEvent;
    if (!event || !event.type) return;

    const props = event.properties ?? {};

    switch (event.type) {
      // ── Session events ──

      case "session.idle": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) queueSessionPush(sessionId);
        break;
      }

      case "session.created": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) {
          const sync = ctx.sessionSync;
          if (sync) {
            try {
              await sync.pushSession(sessionId);
            } catch (err) {
              log("failed to push new session", { sessionId, error: String(err) });
            }
          }
        }
        break;
      }

      case "session.updated": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) queueSessionPush(sessionId);
        break;
      }

      case "session.deleted": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) {
          const sync = ctx.sessionSync;
          if (sync) {
            // While the deletion-safety halt is in effect, do NOT mark
            // this deletion as "expected". The marker means we can't
            // currently trust local DB state (wipe / restore / fingerprint
            // mismatch), so a deletion observed during the halt window
            // could be a legitimate user action OR a symptom of the same
            // corruption that tripped the halt. Marking it expected would
            // pollute the in-memory `expectedDeletions` set and cause an
            // immediate fast-path tombstone (no two-cycle confirmation,
            // no threshold gate) the moment the user clears the marker.
            //
            // The conservative path: skip the mark, skip the pushAll (it
            // would be a no-op anyway after the entry guard). When sync
            // resumes, the deletion will be detected via the normal
            // missing-row scan and routed through full two-cycle
            // confirmation — or trip the threshold again if it really
            // was part of the original corruption.
            if (sync.isHalted()) {
              log("session.deleted while sync halted — deferring to post-recovery confirmation", {
                sessionId,
              });
              break;
            }

            // Fast-path the deletion past the deletion-safety guard. Without
            // this, the cascade tombstones (session + todos + session_share)
            // would either be deferred for a sync cycle (~15s) or counted
            // toward the threshold halt. With the explicit hint, the guard
            // emits them immediately and exempts them from the percentage
            // calculation entirely — only legitimately-suspicious deletions
            // (i.e. ones we can't attribute to a user action) wait or halt.
            //
            // Note: messages and parts that cascaded with the session are
            // NOT auto-marked here — we don't know their session_id at this
            // point (they're already gone from local DB). They'll go through
            // the conservative path: pending → confirmed after one cycle →
            // tombstoned. Acceptable trade-off; ~15s of stale messages on
            // peers vs. risk of mis-tombstoning.
            sync.markExpectedDeletion(`session:${sessionId}`);
            try {
              await sync.pushAll();
            } catch (err) {
              log("failed to push session deletion", { sessionId, error: String(err) });
            }
          }
        }
        break;
      }

      // ── Message events ──

      case "message.updated": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) queueSessionPush(sessionId);
        break;
      }

      case "message.part.updated": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) queueSessionPush(sessionId);
        break;
      }

      // ── Todo events ──

      case "todo.updated": {
        const sessionId = props["sessionId"] as string | undefined;
        if (sessionId) queueSessionPush(sessionId);
        break;
      }

      // ── Server events ──

      case "server.connected": {
        log("server connected, running full sync");
        try {
          const sync = ctx.sessionSync;
          if (sync) await sync.sync();
          await fileSync.sync();
        } catch (err) {
          log("full sync error on connect", { error: String(err) });
        }
        break;
      }

      // ── File events ──

      case "file.watcher.updated": {
        debouncedFileSync();
        break;
      }

      default:
        // Unhandled event — ignore
        break;
    }
  };
}
