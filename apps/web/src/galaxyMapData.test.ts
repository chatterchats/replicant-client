import { describe, expect, it } from "vitest";

import {
  defaultGalaxyLayers,
  filterGalaxyStars,
  mapGalaxyScene,
} from "./galaxyMapData";
import type { GalaxySceneSnapshot } from "./protocol";

const scene: GalaxySceneSnapshot = {
  revision: 2,
  generated_at_ms: 3,
  stars: [
    {
      id: "SOL",
      name: "Home",
      spectral_type: "G",
      position: { x: 0, y: 0, z: 0 },
      exploration: "explored",
      current: true,
      has_hub: true,
      has_life: true,
      has_relay: true,
      has_megastructure: true,
    },
    {
      id: "ALPHA",
      name: null,
      spectral_type: "K",
      position: { x: 7, y: 0, z: 0 },
      exploration: "undiscovered",
      current: false,
      has_hub: false,
      has_life: false,
      has_relay: true,
    },
  ],
  relay_edges: [{ from: "SOL", to: "ALPHA" }],
  active_travel: [],
  signals: [],
  highlights: [{ workflow_id: "workflow-1", from: "SOL", to: "ALPHA" }],
  overlays: [
    {
      kind: "life",
      system: "SOL",
      position: { x: 0, y: 0, z: 0 },
      count: 1,
    },
  ],
  workflow_targets: [
    {
      workflow_id: "workflow-1",
      workflow_kind: "relay.expansion",
      system: "ALPHA",
    },
  ],
};

describe("galaxy map mapping", () => {
  it("keeps the current system visible and maps semantic edges", () => {
    const visible = filterGalaxyStars(scene.stars, {
      search: "alpha",
      exploration: "undiscovered",
    });
    const geometry = mapGalaxyScene(scene, visible, defaultGalaxyLayers);

    expect(visible.map((star) => star.id)).toEqual(["SOL", "ALPHA"]);
    expect(geometry.stars[0]?.is_megastructure).toBe(true);
    expect(geometry.relays).toEqual([
      {
        from: { x: 0, y: 0, z: 0 },
        to: { x: 7, y: 0, z: 0 },
        relay: true,
      },
    ]);
    expect(geometry.life).toEqual([{ x: 0, y: 0, z: 0 }]);
    expect(geometry.highlights).toEqual([
      {
        from: { x: 0, y: 0, z: 0 },
        to: { x: 7, y: 0, z: 0 },
        exploration_route: true,
        workflow_id: "workflow-1",
      },
    ]);
  });

  it("honors renderer layer toggles", () => {
    const geometry = mapGalaxyScene(scene, scene.stars, {
      ...defaultGalaxyLayers,
      relays: false,
      life: false,
    });
    expect(geometry.relays).toEqual([]);
    expect(geometry.life).toEqual([]);
  });
});
