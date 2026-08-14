import { describe, expect, it } from "vitest";

import {
  parseHealthResponse,
  parseGalaxySceneResponse,
  parseLiveMessage,
  parseSnapshotResponse,
} from "./protocol";

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
          workflows: [],
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
            value: { name: "Miner" },
          },
        },
      }).delta.type,
    ).toBe("entity_upsert");
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
});
