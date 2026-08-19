import { describe, expect, it } from "vitest";

import { effectiveDeviceLocation } from "./CloningPage";
import type { DeviceSummary } from "./protocol";

function device(
  id: string,
  overrides: Partial<DeviceSummary> = {},
): DeviceSummary {
  return {
    entity: { kind: "device", id },
    device_type: null,
    status: null,
    ownership: "owned",
    owner: null,
    owner_name: null,
    system: null,
    location: null,
    available_commands: [],
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
    ...overrides,
  };
}

describe("cloning locations", () => {
  it("resolves a stowed replicant matrix through its parent vessel", () => {
    const vessel = device("VESSEL-1", {
      device_type: "surge_carrier",
      system: "SCEPTURUM",
      location: "SCEPTURUM-3-L4",
    });
    const matrix = device("MATRIX-1", {
      device_type: "replicant_matrix",
      // A contained matrix can have no location or stale location data; the
      // vessel is authoritative for where replication is taking place.
      location: "STALE-MATRIX-LOCATION",
      stowed_in: vessel.entity.id,
    });
    const devices = new Map([
      [vessel.entity.id, vessel],
      [matrix.entity.id, matrix],
    ]);

    expect(effectiveDeviceLocation(matrix, devices)).toBe("SCEPTURUM-3-L4");
  });

  it("resolves nested stowage until it reaches a located parent", () => {
    const vessel = device("VESSEL-1", { location: "SOL-4-L4" });
    const cradle = device("CRADLE-1", { stowed_in: vessel.entity.id });
    const matrix = device("MATRIX-1", { stowed_in: cradle.entity.id });
    const devices = new Map([
      [vessel.entity.id, vessel],
      [cradle.entity.id, cradle],
      [matrix.entity.id, matrix],
    ]);

    expect(effectiveDeviceLocation(matrix, devices)).toBe("SOL-4-L4");
  });
});
