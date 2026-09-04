import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { EntitySummary, LocationInspectorSummary } from "../protocol";
import { LocationInspector } from "./LocationInspector";
const summary: EntitySummary = {
  entity: { kind: "location", id: "SOL-BELT" },
  label: "SOL-BELT",
  secondary_label: null,
  system: "SOL",
  location: "SOL-BELT",
  entity_type: "asteroid_belt",
  status: null,
};
const detail: LocationInspectorSummary = {
  location_type: "asteroid_belt",
  system: "SOL",
  parent: null,
  scanned: false,
  system_scanned: true,
  system_tags: [],
  survey: {
    planets_total: null,
    planets_scanned: null,
    moons_total: null,
    moons_scanned: 0,
    moons_total_estimated: false,
  },
  environment: {
    atmosphere: null,
    magnetic_field: false,
    gravity_g: 0,
    surface_temperature_c: null,
    habitable_zone: null,
    life_stage: "none",
    axial_tilt_degrees: null,
    rotation_state: null,
    star_spectral_type: null,
    nearby_belt_richness: null,
    distance_from_sol_light_years: null,
  },
  physical: {},
  belt: {},
  lagrange: {},
  outer_system: {},
  incoming_object: {},
  megastructure: {},
  resource_sites: [],
  inventory: [],
  advanced: {},
  contents: {
    total: 393,
    items: [],
    groups: [
      {
        entity_kind: "device",
        entity_type: "mining_drone",
        count: 393,
        statuses: [{ status: "idle", count: 393 }],
      },
    ],
  },
};
describe("LocationInspector", () => {
  it("keeps false and zero while omitting unobserved environment rows", () => {
    const html = renderToStaticMarkup(
      <LocationInspector summary={summary} detail={detail} />,
    );
    expect(html).toContain("Magnetic field");
    expect(html).toContain("Gravity");
    expect(html).toContain("Moons scanned");
    expect(html).toContain("Life stage");
    expect(html).not.toContain("Atmosphere");
    expect(html).not.toContain("Surface temperature");
    expect(html).toContain("393");
    expect(html).toContain("<details");
  });
});
