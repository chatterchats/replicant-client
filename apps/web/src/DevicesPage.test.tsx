import { Children, isValidElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import {
  DeviceSelection,
  DevicesContent,
  filterAndSortDevices,
  type DeviceFilters,
} from "./DevicesPage";
import type { DeviceSummary, DevicesSnapshot } from "./protocol";

const device = (
  id: string,
  overrides: Partial<DeviceSummary> = {},
): DeviceSummary => ({
  entity: { kind: "device", id },
  device_type: "mining_drone",
  status: "idle",
  ownership: "owned",
  owner: "R-1",
  system: "SOL",
  location: "EARTH",
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
});

const filters: DeviceFilters = {
  search: "",
  status: "",
  type: "",
  system: "",
  location: "",
  ownership: "",
  claim: "",
};

function click(node: ReactNode): boolean {
  if (!isValidElement<{ children?: ReactNode; onClick?: () => void }>(node))
    return false;
  if (typeof node.props.onClick === "function") {
    node.props.onClick();
    return true;
  }
  return Children.toArray(node.props.children).some(click);
}

describe("device fleet browser", () => {
  it("searches, filters, and sorts normalized device fields", () => {
    const rows = [
      device("D-10", { tags: ["hauler"], location: "MARS", system: "SOL" }),
      device("D-2", {
        device_type: "survey_drone",
        owner: "R-2",
        status: "active",
      }),
      device("D-1", { ownership: "public", owner: null }),
    ];
    expect(
      filterAndSortDevices(rows, { ...filters, search: "hauler" }, "code").map(
        (row) => row.entity.id,
      ),
    ).toEqual(["D-10"]);
    expect(
      filterAndSortDevices(
        rows,
        { ...filters, status: "active", type: "survey_drone" },
        "code",
      ).map((row) => row.entity.id),
    ).toEqual(["D-2"]);
    expect(
      filterAndSortDevices(rows, filters, "code", true).map(
        (row) => row.entity.id,
      ),
    ).toEqual(["D-10", "D-2", "D-1"]);
  });

  it("selects a row for the global inspector", () => {
    const onSelectDevice = vi.fn();
    const row = device("D-1");
    expect(click(DeviceSelection({ device: row, onSelectDevice }))).toBe(true);
    expect(onSelectDevice).toHaveBeenCalledWith(row);
  });

  it("presents claims and unknown device types without raw JSON", () => {
    const snapshot: DevicesSnapshot = {
      metadata: { revision: 7, generated_at_ms: 10 },
      devices: [
        device("D-1", {
          device_type: "future_device",
          claim: {
            workflow_id: "wf-1",
            workflow_kind: "transport.route",
            workflow_status: "running",
          },
        }),
      ],
    };
    const html = renderToStaticMarkup(
      <DevicesContent
        data={snapshot}
        status="loaded"
        error={null}
        refreshing={false}
        refresh={() => Promise.resolve()}
        descriptors={{ reports: [], actions: [], workflows: [] }}
        onSelectDevice={() => undefined}
        onSelectEntity={() => undefined}
        onOpenSystem={() => undefined}
        onSelectWorkflow={() => undefined}
        onRunCommand={() => undefined}
      />,
    );
    expect(html).toContain("future_device");
    expect(html).toContain("transport.route");
    expect(html).toContain("wf-1");
  });
});
