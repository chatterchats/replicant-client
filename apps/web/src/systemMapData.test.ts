import { describe, expect, it } from "vitest";

import type { DescriptorCatalog, SystemSceneSnapshot } from "./protocol";
import { mapSystemScene, markerActions } from "./systemMapData";

const scene: SystemSceneSnapshot = {
  system: "SOL",
  revision: 1,
  generated_at_ms: 2,
  markers: [
    {
      id: "SOL",
      label: "SOL",
      kind: "star",
      entity: { kind: "system", id: "SOL" },
      location: "SOL",
      parent: null,
      position: { x: 500, y: 500 },
      count: 1,
    },
    {
      id: "SOL-1",
      label: "SOL-1",
      kind: "planet",
      entity: { kind: "location", id: "SOL-1" },
      location: "SOL-1",
      parent: "SOL",
      position: { x: 600, y: 500 },
      count: 1,
    },
  ],
  active_travel: [
    {
      entity: { kind: "device", id: "SHIP" },
      from: "SOL",
      to: "SOL-1",
      started_at: null,
      arrives_at: null,
    },
  ],
  workflow_markers: [],
};

describe("system marker mapping", () => {
  it("maps parent orbits and active travel", () => {
    expect(mapSystemScene(scene).map((line) => line.kind)).toEqual([
      "orbit",
      "travel",
    ]);
  });

  it("uses descriptor entity applicability for marker actions", () => {
    const catalog: DescriptorCatalog = {
      reports: [],
      actions: [],
      workflows: [
        {
          kind: "survey.route",
          display_name: "Survey",
          aliases: [],
          description: "Survey a system",
          category: "mission",
          parameters: [
            {
              name: "system",
              label: "System",
              description: "Target system",
              kind: { type: "system" },
              required: true,
              default: null,
              options: [],
              validation: {
                minimum: null,
                maximum: null,
                min_length: null,
                max_length: null,
              },
            },
          ],
          risk: "low",
          supported_triggers: ["manual"],
        },
      ],
    };
    const [system, location] = scene.markers;
    if (!system || !location) throw new Error("missing marker fixture");
    expect(markerActions(catalog, system)).toHaveLength(1);
    expect(markerActions(catalog, location)).toHaveLength(0);
  });
});
