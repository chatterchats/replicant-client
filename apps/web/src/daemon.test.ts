import { describe, expect, it } from "vitest";

import {
  daemonReducer,
  initialDaemonState,
  retryDelay,
  socketUrl,
} from "./daemon";
import type {
  DaemonHealth,
  EntityIndexSnapshot,
  LiveMessage,
  RuntimeSnapshot,
} from "./protocol";

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
  automation: {
    automatic_triggers_enabled: true,
    workflows_paused: false,
  },
  workflows: [],
  requirements: [],
  notifications: [],
  slice_revisions: {},
};
const entities: EntityIndexSnapshot = {
  metadata: snapshot.metadata,
  entities: [
    {
      entity: { kind: "replicant", id: "R-1" },
      label: "R-1",
      secondary_label: "Ada",
      system: "SOL",
      location: "EARTH",
      entity_type: null,
      status: "idle",
    },
  ],
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
      entities,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(11, {
        type: "entity_upsert",
        data: {
          entity: { kind: "device", id: "D-1" },
          value: {
            entity: { kind: "device", id: "D-1" },
            label: "D-1",
            secondary_label: "mining_drone",
            system: "SOL",
            location: "EARTH",
            entity_type: "mining_drone",
            status: "idle",
          },
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
    expect(state.entities["device:D-1"]?.label).toBe("D-1");
    expect(state.entities["replicant:R-1"]?.location).toBe("EARTH");
    expect(state.notifications).toHaveLength(1);
    expect(state.galaxyRevision).toBe(10);
  });

  it("applies invalidations across a revision gap without resnapshotting", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
      entities,
    });
    // Revision 11 was missed. Invalidations carry the revision each slice
    // reached, so the affected pages refetch and no full resnapshot is needed.
    state = daemonReducer(state, {
      type: "live",
      message: live(12, {
        type: "domain_invalidated",
        data: { slice: "devices" },
      }),
    });
    expect(state.needsResnapshot).toBe(false);
    expect(state.invalidated.devices).toBe(12);
    expect(state.revision).toBe(12);

    state = daemonReducer(state, {
      type: "live",
      message: live(13, {
        type: "domain_invalidated",
        data: { slice: "inventory" },
      }),
    });
    expect(state.invalidated.inventory).toBe(13);
    expect(state.revision).toBe(13);
  });

  it("ignores replayed messages at or below the loaded revision", () => {
    const state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
      entities,
    });
    // Buffered while the snapshot was in flight; already reflected in it.
    const unchanged = daemonReducer(state, {
      type: "live",
      message: live(10, {
        type: "domain_invalidated",
        data: { slice: "devices" },
      }),
    });
    expect(unchanged).toBe(state);
    expect(unchanged.invalidated.devices).toBeUndefined();
  });

  it("applies coalesced invalidations for every slice they carry", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
      entities,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(14, {
        type: "domains_invalidated",
        data: { slices: { devices: 14, inventory: 14, universe: 14 } },
      }),
    });
    expect(state.invalidated.devices).toBe(14);
    expect(state.invalidated.inventory).toBe(14);
    expect(state.galaxyRevision).toBe(14);
    expect(state.needsResnapshot).toBe(false);
  });

  it("keeps the highest revision seen per slice", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
      entities,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(12, {
        type: "domains_invalidated",
        data: { slices: { devices: 12 } },
      }),
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(13, {
        type: "domains_invalidated",
        data: { slices: { devices: 11, inventory: 13 } },
      }),
    });
    expect(state.invalidated.devices).toBe(12);
    expect(state.invalidated.inventory).toBe(13);
  });

  it("seeds slice revisions from the snapshot", () => {
    const state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot: { ...snapshot, slice_revisions: { devices: 9, cargo: 7 } },
      health,
      entities,
    });
    expect(state.invalidated.devices).toBe(9);
    expect(state.invalidated.cargo).toBe(7);
  });

  it("resnapshots without disconnecting when the socket starts ahead", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
      entities,
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
      entities,
    });
    expect(state.needsResnapshot).toBe(false);
    expect(state.revision).toBe(11);
  });

  it("tracks invalidations by slice revision for targeted refetches", () => {
    let state = daemonReducer(initialDaemonState, {
      type: "snapshot",
      snapshot,
      health,
      entities,
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(11, {
        type: "domain_invalidated",
        data: { slice: "inventory" },
      }),
    });
    expect(state.invalidated.inventory).toBe(11);
    state = daemonReducer(state, {
      type: "live",
      message: live(12, {
        type: "domain_invalidated",
        data: { slice: "devices" },
      }),
    });
    expect(state.invalidated.devices).toBe(12);
    state = daemonReducer(state, {
      type: "live",
      message: live(13, {
        type: "domain_invalidated",
        data: { slice: "events" },
      }),
    });
    state = daemonReducer(state, {
      type: "live",
      message: live(14, {
        type: "domain_invalidated",
        data: { slice: "trade" },
      }),
    });
    expect(state.invalidated.events).toBe(13);
    expect(state.invalidated.trade).toBe(14);
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

  it("connects a packaged desktop UI to the loopback daemon", () => {
    expect(
      socketUrl({ href: "tauri://localhost/" }, "http://127.0.0.1:8080"),
    ).toBe("ws://127.0.0.1:8080/ws");
  });
});
