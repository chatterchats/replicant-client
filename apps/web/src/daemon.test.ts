import { describe, expect, it } from "vitest";

import { daemonReducer, initialDaemonState, retryDelay } from "./daemon";
import type { DaemonHealth, LiveMessage, RuntimeSnapshot } from "./protocol";

const health: DaemonHealth = {
  status: "healthy",
  daemon_version: "0.1.0",
  detail: null,
};
const snapshot: RuntimeSnapshot = {
  metadata: { revision: 10, generated_at_ms: 1 },
  sync: {
    phase: "ready",
    revision: 10,
    last_event_at_ms: null,
    detail: null,
  },
  workflows: [],
};

function live(revision: number, delta: LiveMessage["delta"]): LiveMessage {
  return { protocol_version: 1, revision, delta };
}

describe("daemonReducer", () => {
  it("applies ordered deltas to the browser projection", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(11, {
        type: "entity_upsert",
        data: {
          entity: { kind: "device", id: "D-1" },
          value: { name: "Miner" },
        },
      }),
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(12, {
        type: "notification",
        data: {
          id: "N-1",
          level: "info",
          title: "Ready",
          message: "Synced",
          created_at_ms: 2,
        },
      }),
    });

    expect(state.revision).toBe(12);
    expect(state.entities["device:D-1"]).toEqual({ name: "Miner" });
    expect(state.notifications).toHaveLength(1);
    expect(state.galaxyRevision).toBe(10);
  });

  it("resnapshots after a revision gap and ignores uncertain deltas", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(12, {
        type: "domain_invalidated",
        data: { slice: "devices" },
      }),
    });
    expect(state.needsResnapshot).toBe(true);
    expect(state.syncing).toBe(true);

    const unchanged = daemonReducer(state, {
      type: "live",
      message: live(13, {
        type: "domain_invalidated",
        data: { slice: "inventory" },
      }),
    });
    expect(unchanged).toBe(state);

    state = daemonReducer(state, {
      type: "snapshot",
      snapshot: {
        ...snapshot,
        metadata: { ...snapshot.metadata, revision: 13 },
      },
      health,
    });
    expect(state.needsResnapshot).toBe(false);
    expect(state.revision).toBe(13);
  });

  it("resnapshots without disconnecting when the socket starts ahead", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(11, {
        type: "snapshot",
        data: { revision: 11, generated_at_ms: 2 },
      }),
    });
    expect(state.needsResnapshot).toBe(true);
    expect(state.connection).toBe("connecting");

    state = daemonReducer(state, {
      type: "snapshot",
      snapshot: {
        ...snapshot,
        metadata: { ...snapshot.metadata, revision: 11 },
      },
      health,
    });
    expect(state.needsResnapshot).toBe(false);
    expect(state.revision).toBe(11);
  });

  it("tracks reconnect state with capped backoff", () => {
    const offline = daemonReducer(initialDaemonState, {
      type: "disconnected",
      error: "closed",
    });
    const reconnecting = daemonReducer(offline, {
      type: "connecting",
      retry: true,
    });
    expect(reconnecting.connection).toBe("reconnecting");
    expect(retryDelay(0)).toBe(500);
    expect(retryDelay(100)).toBe(10_000);
  });
});
