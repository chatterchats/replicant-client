import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { GalaxyMapWasm } from "./GalaxyMapWasm";
import { defaultGalaxyLayers } from "./galaxyMapData";
import type { GalaxySceneSnapshot } from "./protocol";

const scene: GalaxySceneSnapshot = {
  revision: 1,
  generated_at_ms: 1,
  stars: [],
  relay_edges: [],
  active_travel: [],
  signals: [],
  highlights: [],
  overlays: [],
  workflow_targets: [],
};

describe("GalaxyMapWasm", () => {
  it("provides the renderer canvas", () => {
    expect(
      renderToStaticMarkup(
        <GalaxyMapWasm
          scene={scene}
          visibleStars={[]}
          layers={defaultGalaxyLayers}
          centerSystem=""
          onSelectStar={() => undefined}
          onContextStar={() => undefined}
          onSelectWorkflow={() => undefined}
        />,
      ),
    ).toContain('aria-label="Interactive galaxy map"');
  });
});
