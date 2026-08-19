import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { CargoContent, filterCargo } from "./CargoPage";
import type {
  CargoCarrierSummary,
  CargoSnapshot,
  DeviceSummary,
} from "./protocol";

const device: DeviceSummary = {
  entity: { kind: "device", id: "CARRIER-1" },
  device_type: "future_carrier",
  status: "active",
  ownership: "owned",
  owner: "R-1",
  owner_name: "Ada",
  system: "SOL",
  location: "SOL-HUB",
  available_commands: [],
  tags: [],
  attached_to: null,
  stowed_in: null,
  controller: null,
  linked_device: null,
  attached_devices: ["D-1"],
  controlled_devices: [],
  stowed_devices: ["D-2"],
  attach_capacity: 4,
  cargo_capacity: 10,
  cargo_used: 3,
  operational_capacity_percent: 100,
  active_directive: null,
  directive_status: null,
  travel_destination: "VEGA-1",
  claim: {
    workflow_id: "WF-1",
    workflow_kind: "transport.delivery",
    workflow_status: "running",
  },
};
const carrier: CargoCarrierSummary = {
  device,
  resources: [{ resource: "silicates", quantity: 3 }],
  attachment_used: 1,
};
const snapshot: CargoSnapshot = {
  metadata: { revision: 8, generated_at_ms: 10 },
  cargo_used: 3,
  cargo_capacity: 10,
  attachment_used: 1,
  attachment_capacity: 4,
  carriers: [carrier],
};

describe("CargoContent", () => {
  it("filters by capability content and activity", () => {
    expect(filterCargo([carrier], "silicates", "active")).toEqual([carrier]);
    expect(filterCargo([carrier], "missing", "")).toEqual([]);
    expect(filterCargo([carrier], "", "idle")).toEqual([]);
  });

  it("renders compact capacities, cargo, travel, and workflow links", () => {
    const html = renderToStaticMarkup(
      <CargoContent
        data={snapshot}
        status="loaded"
        error={null}
        refreshing={false}
        refresh={vi.fn()}
        descriptors={{ reports: [], actions: [], workflows: [] }}
        onSelectDevice={vi.fn()}
        onSelectEntity={vi.fn()}
        onOpenSystem={vi.fn()}
        onSelectWorkflow={vi.fn()}
        onRunCommand={vi.fn()}
      />,
    );
    expect(html).toContain("<meter");
    expect(html).toContain("silicates");
    expect(html).toContain("Traveling to VEGA-1");
    expect(html).toContain("transport.delivery");
  });
});
