import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { GalaxyMapWasm } from "./GalaxyMapWasm";

describe("GalaxyMapWasm", () => {
  it("provides the renderer canvas", () => {
    expect(renderToStaticMarkup(<GalaxyMapWasm />)).toContain(
      'aria-label="Interactive galaxy map"',
    );
  });
});
