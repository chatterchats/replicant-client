/** @vitest-environment jsdom */
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import type { DeviceSummary } from "../protocol";
import { DeviceRolePanel, deviceRoleProfile } from "./DeviceRolePanel";

const base: DeviceSummary = {
  entity: { kind: "device", id: "DEVICE-1" },
  device_type: "compute_core",
  status: "active",
  ownership: "owned",
  owner: null,
  owner_name: null,
  system: "SOL",
  region: "alpha",
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
  attach_capacity: 0,
  cargo_capacity: 0,
  cargo_used: 0,
  cargo: [],
  stow_capacity: 0,
  stow_used: 0,
  operational_capacity_percent: 100,
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
};

function title(type: string) {
  return deviceRoleProfile({ ...base, device_type: type })?.title;
}

describe("DeviceRolePanel", () => {
  it("covers the remaining Phase 7 device families", () => {
    expect(title("ami_trade_controller")).toBe("Trade controller summary");
    expect(title("ami_transport_controller")).toBe(
      "Transport controller summary",
    );
    expect(title("ami_fleet_controller")).toBe("Fleet controller summary");
    expect(title("transport_drone")).toBe("Transport device summary");
    expect(title("belt_surveyor")).toBe("Survey & sensing summary");
    expect(title("defence_grid")).toBe("Defence & protection summary");
    expect(title("hab_module")).toBe("Habitat & planetary support summary");
    expect(title("casimir_array")).toBe("Power & propulsion summary");
    expect(title("replicant_matrix")).toBe("Matrix & compute summary");
    expect(title("future_device")).toBe("Operational summary");
  });

  it("renders role relationships as Inspector navigation links", () => {
    const html = renderToStaticMarkup(
      <DeviceRolePanel
        device={{
          ...base,
          device_type: "ftl_slingshot",
          linked_device: "MATRIX-1",
        }}
        onNavigate={vi.fn()}
      />,
    );
    expect(html).toContain("FTL slingshot summary");
    expect(html).toContain("MATRIX-1");
    expect(html).toContain("inspector-inline-link");
  });
});
