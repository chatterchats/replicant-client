import { Children, isValidElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import {
  DeviceSelection,
  DevicesContent,
  deviceCategory,
  filterAndSortDevices,
  groupDevices,
  normalizedDeviceStatus,
  systemOptions,
  visibleDeviceRows,
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
  owner_name: "Ada",
  system: "SOL",
  location: "SOL-1",
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
  owner: "",
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
  it("consolidates activity statuses and filters by owning replicant", () => {
    const rows = [
      device("D-1", { status: "mining (Conductive)", owner: "R-1" }),
      device("D-2", { status: "mining (Silicates)", owner: "R-2" }),
      device("D-3", { status: "repairing hull", owner: "R-1" }),
    ];
    expect(normalizedDeviceStatus(rows[0]?.status ?? null)).toBe("mining");
    expect(normalizedDeviceStatus(rows[2]?.status ?? null)).toBe("repairing");
    expect(
      filterAndSortDevices(
        rows,
        { ...filters, status: "mining", owner: "R-2" },
        "code",
      ).map((row) => row.entity.id),
    ).toEqual(["D-2"]);
  });

  it("orders systems by device count and then system name", () => {
    const rows = [
      device("A", { system: "VEGA" }),
      device("B", { system: "SOL" }),
      device("C", { system: "SOL" }),
      device("D", { system: "ALPHA" }),
      device("E", { system: null }),
    ];
    expect(systemOptions(rows)).toEqual([
      { system: "SOL", count: 2 },
      { system: "ALPHA", count: 1 },
      { system: "VEGA", count: 1 },
    ]);
  });

  it("sorts by type and system while nesting hosted and controlled devices", () => {
    const vessel = device("VESSEL", {
      device_type: "heaven_vessel",
      controlled_devices: ["DRONE"],
    });
    const relay = device("RELAY", {
      device_type: "ftl_relay",
      stowed_in: "VESSEL",
    });
    const drone = device("DRONE", {
      device_type: "survey_drone",
      controller: "VESSEL",
    });
    const laterSystem = device("MINER-B", {
      device_type: "mining_drone",
      system: "VEGA",
    });
    const earlierSystem = device("MINER-A", {
      device_type: "mining_drone",
      system: "ALPHA",
    });
    const ordered = filterAndSortDevices(
      [laterSystem, relay, drone, vessel, earlierSystem],
      filters,
      "type",
    );
    const groups = groupDevices(ordered);

    expect(deviceCategory("ftl_relay")).toBe("ftl_comms");
    expect(deviceCategory("future_device")).toBe("other");
    expect(groups.map((group) => group.label)).toEqual(["Vessel", "Mining"]);
    expect(
      groups
        .at(0)
        ?.rows.map((row) => [
          row.device.entity.id,
          row.depth,
          row.relationship,
        ]),
    ).toEqual([
      ["VESSEL", 0, null],
      ["RELAY", 1, "stowed"],
      ["DRONE", 1, "controlled"],
    ]);
    expect(groups.at(1)?.rows.map((row) => row.device.entity.id)).toEqual([
      "MINER-A",
      "MINER-B",
    ]);
    const vesselRows = groups.at(0)?.rows ?? [];
    expect(
      visibleDeviceRows(vesselRows, new Set(["VESSEL"])).map(
        (row) => row.device.entity.id,
      ),
    ).toEqual(["VESSEL"]);
    expect(
      visibleDeviceRows(vesselRows, new Set()).map((row) => [
        row.device.entity.id,
        row.hasChildren,
      ]),
    ).toEqual([
      ["VESSEL", true],
      ["RELAY", false],
      ["DRONE", false],
    ]);
  });

  it("selects a row for the global inspector", () => {
    const onSelectDevice = vi.fn();
    const row = device("D-1");
    expect(click(DeviceSelection({ device: row, onSelectDevice }))).toBe(true);
    expect(onSelectDevice).toHaveBeenCalledWith(row);
  });

  it("renders counted systems and collapsible category headers without claims", () => {
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
        onRunCommand={() => undefined}
      />,
    );
    expect(html).toContain("SOL");
    expect(html).toContain("1 device");
    expect(html).toContain("Other");
    expect(html).toContain("Ada");
    expect(html).not.toContain("transport.route");
    expect(html).not.toContain("Claim");
  });
});
