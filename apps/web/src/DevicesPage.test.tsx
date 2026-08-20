/** @vitest-environment jsdom */
import { act, Children, isValidElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import {
  DeviceSelection,
  DevicesContent,
  bulkDeviceEligibility,
  bulkDeviceResultItems,
  deviceCategory,
  filterAndSortDevices,
  groupDevices,
  normalizedDeviceStatus,
  systemOptions,
  visibleDeviceRows,
  type DeviceFilters,
} from "./DevicesPage";
import type {
  DescriptorCatalog,
  DeviceSummary,
  DevicesSnapshot,
} from "./protocol";

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
});

const filters: DeviceFilters = {
  search: "",
  status: "",
  type: "",
  system: "",
  owner: "",
};

const bulkDescriptors: DescriptorCatalog = {
  reports: [],
  actions: [
    {
      kind: "device.lifecycle.bulk",
      display_name: "Control selected devices",
      aliases: [],
      description: "Bulk lifecycle",
      category: "devices",
      operation_class: "action" as const,
      risk: "elevated" as const,
      applicable_to: [],
      parameters: [],
    },
  ],
  workflows: [],
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
      controlled_devices: ["MINER-C"],
    });
    const controlledMiner = device("MINER-C", {
      device_type: "mining_drone",
      system: "VEGA",
      controller: "MINER-B",
    });
    const earlierSystem = device("MINER-A", {
      device_type: "mining_drone",
      system: "ALPHA",
    });
    const ordered = filterAndSortDevices(
      [earlierSystem, relay, drone, controlledMiner, vessel, laterSystem],
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
      "MINER-B",
      "MINER-C",
      "MINER-A",
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

  it("computes command eligibility and parses per-device bulk results", () => {
    const rows = [
      device("D-1", { available_commands: ["decommission", "deactivate"] }),
      device("D-2", { available_commands: ["deactivate"] }),
      device("D-3", { available_commands: ["decommission"] }),
    ];
    const eligibility = bulkDeviceEligibility(
      rows,
      new Set(["D-1", "D-2"]),
      "decommission",
    );
    expect(eligibility.eligible.map((row) => row.entity.id)).toEqual(["D-1"]);
    expect(eligibility.incompatible.map((row) => row.entity.id)).toEqual([
      "D-2",
    ]);
    expect(
      bulkDeviceResultItems({
        results: [
          {
            kind: "succeeded",
            device: "D-1",
            operation_id: "OP-1",
            operation_status: "completed",
          },
          { kind: "failed", device: "D-2", error: "not inactive" },
          { kind: "ignored", device: "D-3" },
        ],
      }),
    ).toEqual([
      {
        kind: "succeeded",
        device: "D-1",
        operation_id: "OP-1",
        operation_status: "completed",
        error: null,
      },
      {
        kind: "failed",
        device: "D-2",
        operation_id: null,
        operation_status: null,
        error: "not inactive",
      },
    ]);
  });

  it("multi-selects devices and requires typed confirmation for bulk decommission", () => {
    const snapshot: DevicesSnapshot = {
      metadata: { revision: 7, generated_at_ms: 10 },
      devices: [
        device("D-1", { available_commands: ["decommission"] }),
        device("D-2", { available_commands: ["decommission"] }),
      ],
    };
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    act(() => {
      root.render(
        <DevicesContent
          data={snapshot}
          status="loaded"
          error={null}
          refreshing={false}
          refresh={() => Promise.resolve()}
          descriptors={bulkDescriptors}
          onSelectDevice={() => undefined}
          onSelectEntity={() => undefined}
          onOpenSystem={() => undefined}
          onRunCommand={() => undefined}
        />,
      );
    });

    const selectAll = container.querySelector<HTMLInputElement>(
      'input[aria-label="Select all filtered devices"]',
    );
    expect(selectAll).not.toBeNull();
    act(() => {
      if (!selectAll) return;
      selectAll.click();
    });
    expect(container.textContent).toContain("2 selected");

    const command = container.querySelector<HTMLSelectElement>(
      'select[aria-label="Bulk device command"]',
    );
    expect(command).not.toBeNull();
    act(() => {
      if (!command) return;
      command.value = "decommission";
      command.dispatchEvent(new Event("change", { bubbles: true }));
    });
    const run = [...container.querySelectorAll("button")].find((button) =>
      button.textContent.includes("Decommission 2"),
    );
    expect(run).toBeDefined();
    act(() => {
      run?.click();
    });
    expect(container.textContent).toContain("Decommission 2 devices?");
    expect(container.textContent).toContain("Type DECOMMISSION to continue");
    const confirm = [...container.querySelectorAll("button")].find(
      (button) => button.textContent === "Decommission 2",
    );
    expect(confirm?.disabled).toBe(true);

    act(() => {
      root.unmount();
    });
    container.remove();
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
