import { describe, expect, it } from "vitest";

import {
  parseHealthResponse,
  parseDevicesResponse,
  parseEntityIndexResponse,
  parseGalaxySceneResponse,
  parseLiveMessage,
  parseOverviewResponse,
  parseSnapshotResponse,
  parseSystemSceneResponse,
  parseTriggerListResponse,
} from "./protocol";

describe("parseDevicesResponse", () => {
  it("preserves forward-compatible device types and missing fields", () => {
    const parsed = parseDevicesResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 3, generated_at_ms: 10 },
        devices: [
          {
            entity: { kind: "device", id: "D-1" },
            device_type: "future_device",
            status: null,
            ownership: "owned",
            owner: null,
            system: null,
            location: null,
            tags: [],
            attached_to: null,
            stowed_in: null,
            controller: null,
            linked_device: null,
            attached_devices: [],
            controlled_devices: [],
            stowed_devices: [],
            attach_capacity: null,
            cargo_capacity: null,
            cargo_used: null,
            operational_capacity_percent: null,
            active_directive: null,
            directive_status: null,
            travel_destination: null,
            claim: null,
          },
        ],
      },
    });
    expect(parsed.payload.devices[0]).toMatchObject({
      device_type: "future_device",
      status: null,
      owner_name: null,
      claim: null,
    });
  });
});

describe("parseHealthResponse", () => {
  it("accepts the versioned daemon health shape", () => {
    expect(
      parseHealthResponse({
        protocol_version: 1,
        payload: { status: "healthy", daemon_version: "0.1.0", detail: null },
      }),
    ).toMatchObject({ protocol_version: 1, payload: { status: "healthy" } });
  });

  it("rejects an untyped backend response", () => {
    expect(() => parseHealthResponse({ status: "healthy" })).toThrow(
      "Unsupported daemon protocol version",
    );
  });

  it("parses the mirrored snapshot and tagged delta contracts", () => {
    expect(
      parseSnapshotResponse({
        protocol_version: 1,
        payload: {
          metadata: { revision: 4, generated_at_ms: 10 },
          sync: {
            phase: "ready",
            revision: 12,
            last_event_at_ms: null,
            detail: null,
          },
          automation: {
            automatic_triggers_enabled: true,
            workflows_paused: false,
          },
          workflows: [],
          notifications: [],
          requirements: [
            {
              id: "relay-sol",
              name: "SOL relay coverage",
              target: "relay infrastructure",
              scope: "system SOL",
              desired: 2,
              actual: 1,
              in_progress: 1,
              missing: 0,
              workflow_id: "workflow-1",
              status: "running",
            },
          ],
        },
      }).payload.metadata.revision,
    ).toBe(4);
    expect(
      parseLiveMessage({
        protocol_version: 1,
        revision: 5,
        delta: {
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
        },
      }).delta.type,
    ).toBe("entity_upsert");
  });

  it("parses typed entity index snapshots", () => {
    const index = parseEntityIndexResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 10 },
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
      },
    }).payload;
    expect(index.metadata.revision).toBe(4);
    expect(index.entities[0]?.location).toBe("EARTH");
  });

  it("parses typed overview snapshots", () => {
    const overview = parseOverviewResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 10 },
        health: { status: "healthy", daemon_version: "test", detail: null },
        sync: {
          phase: "ready",
          revision: 4,
          last_event_at_ms: null,
          detail: null,
        },
        automation: {
          automatic_triggers_enabled: true,
          workflows_paused: false,
        },
        replicants: [],
        active_travel: [],
        active_workflows: [],
        workflow_counts: [{ status: "running", count: 2 }],
        attention_workflows: [],
        notifications: [],
        recent_activity: [],
      },
    }).payload;
    expect(overview.workflow_counts).toEqual([{ status: "running", count: 2 }]);
  });

  it("rejects unknown protocol versions and delta variants", () => {
    expect(() =>
      parseLiveMessage({ protocol_version: 2, revision: 1, delta: {} }),
    ).toThrow("Unsupported daemon protocol version");
    expect(() =>
      parseLiveMessage({
        protocol_version: 1,
        revision: 1,
        delta: { type: "raw_upstream_event", data: {} },
      }),
    ).toThrow("Unsupported live delta");
  });

  it("parses an application-owned galaxy scene", () => {
    const scene = parseGalaxySceneResponse({
      protocol_version: 1,
      payload: {
        revision: 7,
        generated_at_ms: 8,
        stars: [
          {
            id: "SOL",
            name: null,
            spectral_type: "G",
            position: { x: 0, y: 1, z: 2 },
            exploration: "explored",
            current: true,
            has_hub: true,
            has_life: true,
            has_relay: false,
          },
        ],
        relay_edges: [],
        active_travel: [],
        signals: [],
        highlights: [],
        overlays: [],
        workflow_targets: [],
      },
    }).payload;
    expect(scene.stars[0]?.id).toBe("SOL");
    expect(scene.stars[0]?.position.z).toBe(2);
  });

  it("parses an application-owned system scene", () => {
    const scene = parseSystemSceneResponse({
      protocol_version: 1,
      payload: {
        system: "SOL",
        revision: 7,
        generated_at_ms: 8,
        markers: [
          {
            id: "SOL-1",
            label: "SOL-1",
            kind: "planet",
            entity: { kind: "location", id: "SOL-1" },
            location: "SOL-1",
            parent: null,
            in_habitable_zone: true,
            position: { x: 1.25, y: 2.5 },
            count: 1,
          },
        ],
        active_travel: [],
        workflow_markers: [],
      },
    }).payload;
    expect(scene.system).toBe("SOL");
    expect(scene.markers[0]?.entity.kind).toBe("location");
    expect(scene.markers[0]?.in_habitable_zone).toBe(true);
    expect(scene.markers[0]?.position.x).toBe(1.25);
  });

  it("parses durable trigger status", () => {
    const triggers = parseTriggerListResponse({
      protocol_version: 1,
      payload: {
        triggers: [
          {
            id: "trigger-1",
            name: "hourly survey",
            condition: { kind: "schedule", interval_seconds: 3600 },
            target: {
              operation_class: "workflow",
              kind: "survey.route",
              parameters: {},
            },
            enabled: true,
            created_at_ms: 1,
            updated_at_ms: 2,
            last_fired_at_ms: null,
            next_run_at_ms: 3,
            last_error: null,
            revision: 0,
          },
        ],
      },
    }).payload;
    expect(triggers[0]?.condition.kind).toBe("schedule");
    expect(triggers[0]?.next_run_at_ms).toBe(3);
  });
});
