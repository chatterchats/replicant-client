import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type {
  DeviceInspectorSummary,
  LocationInspectorSummary,
} from "../protocol";
import {
  DeviceInspectorVitals,
  LocationInspectorVitals,
} from "./InspectorVitals";

const device: DeviceInspectorSummary = {
  device: {
    entity: { kind: "device", id: "FACTORY" },
    device_type: "autofactory",
    status: "active",
    ownership: "owned",
    owner: null,
    owner_name: null,
    system: "SOL",
    region: "Alpha",
    location: "SOL-3-L4",
    available_commands: [],
    available_directives: [],
    features: [],
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
    cargo: [],
    stow_capacity: null,
    stow_used: null,
    operational_capacity_percent: 92,
    grace_period_remaining: null,
    upkeep_requirements: [],
    system_status: null,
    active_directive: null,
    directive_status: null,
    directive_details: {},
    directive_collect_system: null,
    directive_target_system: null,
    travel_destination: null,
    claim: null,
  },
  deployed_at: null,
  in_control_range: null,
  settings: {},
  hosting_replicant: null,
  travel: null,
  runtime: {
    created_at: null,
    short_description: null,
    description: null,
    printing: { device_type: "mining_drone" },
    mining: null,
    prospect: null,
    repair: null,
    scan: null,
    waiting_for: null,
    print_queue: [],
    queue_size: 5,
    taxi_mode: null,
    tracking_site_id: null,
    beacon_only: null,
    welcome_message: null,
    repair_paid_pct: null,
  },
};

const location: LocationInspectorSummary = {
  location_type: "asteroid_belt",
  custom_name: null,
  system: "SOL",
  parent: null,
  scanned: true,
  system_scanned: true,
  system_tags: [],
  survey: {
    system_complete: true,
    planets_total: 3,
    planets_scanned: 3,
    moons_total: 2,
    moons_scanned: 2,
    moons_total_estimated: false,
  },
  environment: {
    atmosphere: null,
    magnetic_field: null,
    gravity_g: null,
    surface_temperature_c: null,
    surface_temperature_k: null,
    atmospheric_pressure_atm: null,
    oxygen_percent: null,
    atmospheric_toxicity: null,
    hydrosphere_percent: null,
    tectonic_index: null,
    biosphere_index: null,
    subsurface_ocean: null,
    habitable_zone: null,
    life_stage: null,
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
  resource_sites: [{ designation: "SOL-BELT-SITE-0" }],
  inventory: [],
  advanced: {},
  contents: { total: 0, items: [], groups: [] },
};

describe("Inspector vitals", () => {
  it("surfaces current device work and operational capacity", () => {
    const html = renderToStaticMarkup(
      <DeviceInspectorVitals detail={device} />,
    );
    expect(html).toContain("Printing");
    expect(html).toContain("92% operational");
  });

  it("surfaces survey completion and resource-site count", () => {
    const html = renderToStaticMarkup(
      <LocationInspectorVitals detail={location} />,
    );
    expect(html).toContain("System survey complete");
    expect(html).toContain("1 resource site");
  });
});
