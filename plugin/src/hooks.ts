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
