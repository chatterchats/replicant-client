/* eslint-disable @typescript-eslint/require-await, @typescript-eslint/no-unnecessary-condition */
/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import type {
  ActionDescriptor,
  DescriptorCatalog,
  DeviceSummary,
  ParameterDescriptor,
} from "../protocol";
import { DeviceInspector } from "./DeviceInspector";
import { advertisedDeviceCommands } from "./inspectorModel";

const validation = {
  minimum: null,
  maximum: null,
  min_length: null,
  max_length: null,
};
const parameter = (
  name: string,
  required = true,
  type: "string" | "integer" | "device" | "location" = "string",
): ParameterDescriptor => ({
  name,
  label: name,
  description: name,
  kind: { type },
  required,
  default: null,
  options: [],
  validation,
});
const action = (
  kind: string,
  command: string,
  risk: ActionDescriptor["risk"] = "elevated",
  parameters: ParameterDescriptor[] = [parameter("device", true, "device")],
  fixed: Record<string, unknown> = {},
): ActionDescriptor => ({
  kind,
  display_name: kind,
  aliases: [],
  description: kind,
  category: "devices",
  operation_class: "action",
  risk,
  applicable_to: ["device"],
  parameters,
  device_commands: [{ command, parameters: fixed }],
});
const actions: ActionDescriptor[] = [
  action("autofactory.print", "enqueue_print", "elevated", [
    parameter("device", true, "device"),
    parameter("device_type"),
  ]),
  action("device.travel", "travel", "elevated", [
    parameter("device", true, "device"),
    parameter("destination", true, "location"),
  ]),
  action("device.change_owner", "change_owner", "elevated", [
    parameter("device", true, "device"),
    parameter("target"),
  ]),
  ...["activate", "deactivate", "clear_queue", "system_scan"].map((command) =>
    action(
      "device.lifecycle",
      command,
      "elevated",
      [
        parameter("device", true, "device"),
        {
          ...parameter("command"),
          kind: { type: "enum" as const },
          options: [{ value: command, label: command.replaceAll("_", " ") }],
        },
      ],
      { command },
    ),
  ),
  action("device.retarget", "retarget", "elevated", [
    parameter("device", true, "device"),
    parameter("resource_type"),
  ]),
  action("device.start_mining", "start_mining", "elevated", [
    parameter("device", true, "device"),
    parameter("resource_type"),
    parameter("target", false, "location"),
  ]),
  action("device.stellar_census", "stellar_census", "low", [
    parameter("device", true, "device"),
    {
      ...parameter("page", false, "integer"),
      validation: { ...validation, minimum: 1 },
    },
    {
      ...parameter("per_page", false, "integer"),
      validation: { ...validation, minimum: 1 },
    },
  ]),
  action("autofactory.dequeue_print", "dequeue_print", "elevated", [
    parameter("device", true, "device"),
    {
      ...parameter("index", false, "integer"),
      validation: { ...validation, minimum: 0 },
    },
  ]),
];
const catalog: DescriptorCatalog = { reports: [], actions, workflows: [] };
const device: DeviceSummary = {
  entity: { kind: "device", id: "HEAVEN" },
  device_type: "heaven_vessel",
  status: "active",
  ownership: "owned",
  owner: "R-1",
  owner_name: "Ada",
  system: "SOL",
  region: "core",
  location: "SOL-1",
  available_commands: [
    "enqueue_print",
    "travel",
    "change_owner",
    "activate",
    "deactivate",
    "clear_queue",
    "system_scan",
    "retarget",
    "start_mining",
    "stellar_census",
  ],
  available_directives: [],
  features: ["travel", "mining"],
  tags: [],
  attached_to: null,
  stowed_in: null,
  controller: null,
  linked_device: null,
  attached_devices: [],
  controlled_devices: [],
  stowed_devices: [],
  attach_capacity: 2,
  cargo_capacity: 20,
  cargo_used: 3,
  cargo: [{ resource: "iron", quantity: 3 }],
  stow_capacity: 10,
  stow_used: 1,
  operational_capacity_percent: 100,
  grace_period_remaining: null,
  upkeep_requirements: [],
  system_status: null,
  active_directive: null,
  directive_status: null,
  travel_destination: null,
  claim: null,
};

describe("DeviceInspector", () => {
  it("renders the advertised ten-command set once with distinct cargo and stow", async () => {
    const container = document.createElement("div");
    const root = createRoot(container);
    const onRunCommand = vi.fn();
    await act(async () => {
      root.render(
        <DeviceInspector
          device={device}
          descriptors={catalog}
          entities={{}}
          onRunCommand={onRunCommand}
          onOperationFinished={vi.fn()}
        />,
      );
    });
    const commandButtons = [
      ...container.querySelectorAll(".inspector-command-grid button"),
    ];
    expect(commandButtons).toHaveLength(10);
    expect(container.textContent).not.toContain("slingshot");
    expect(container.textContent).toContain("Cargo");
    expect(container.textContent).toContain("3 / 20");
    expect(container.textContent).toContain("Stow");
    expect(container.textContent).toContain("1 / 10");
    expect(
      new Set(commandButtons.map((button) => button.textContent)).size,
    ).toBe(10);

    const census = commandButtons.find((button) =>
      button.textContent?.includes("stellar_census"),
    ) as HTMLButtonElement;
    await act(async () => {
      census.click();
    });
    expect(onRunCommand).not.toHaveBeenCalled();
    expect(container.querySelector('input[name="page"]')).not.toBeNull();

    for (const button of commandButtons.filter((item) => item !== census)) {
      await act(async () => {
        (button as HTMLButtonElement).click();
      });
    }
    expect(onRunCommand).toHaveBeenCalledTimes(9);
    await act(async () => {
      root.unmount();
    });
  });

  it("shows no actions for empty availability and resolves duplicates catalogue-first", () => {
    expect(
      advertisedDeviceCommands(catalog, { ...device, available_commands: [] }),
    ).toEqual([]);
    const duplicate = {
      ...catalog,
      actions: [
        action("first", "travel", "elevated", [
          parameter("device", true, "device"),
        ]),
        action("second", "travel", "elevated", [
          parameter("device", true, "device"),
        ]),
      ],
    };
    expect(
      advertisedDeviceCommands(duplicate, {
        ...device,
        available_commands: ["travel"],
      })[0]?.descriptor.kind,
    ).toBe("first");
  });

  it("never lets fixed parameters override the selected device", () => {
    const forged: DescriptorCatalog = {
      reports: [],
      workflows: [],
      actions: [
        action(
          "forged",
          "travel",
          "elevated",
          [parameter("device", true, "device")],
          { device: "WRONG" },
        ),
      ],
    };
    expect(
      advertisedDeviceCommands(forged, {
        ...device,
        available_commands: ["travel"],
      })[0]?.initialParameters?.device,
    ).toBe("HEAVEN");
  });

  it("filters controller, release, and stow targets from live device state", () => {
    const controller: DeviceSummary = {
      ...device,
      entity: { kind: "device", id: "TRANSPORT-CONTROLLER" },
      device_type: "transport_controller",
      available_commands: ["adopt", "release", "stow", "set_directive"],
      available_directives: ["delivery", "shuttle"],
      controlled_devices: ["ADOPTED"],
    };
    const candidate = (
      id: string,
      deviceType: string,
      overrides: Partial<DeviceSummary> = {},
    ): DeviceSummary => ({
      ...device,
      entity: { kind: "device", id },
      device_type: deviceType,
      available_commands: [],
      controlled_devices: [],
      stow_capacity: 0,
      stow_used: 0,
      ...overrides,
    });
    const devices = [
      controller,
      candidate("TRANSPORT", "transport_drone"),
      candidate("MINER", "mining_drone"),
      candidate("REMOTE", "transport_drone", { system: "ALPHA" }),
      candidate("ADOPTED", "transport_drone", {
        controller: controller.entity.id,
      }),
      candidate("STOW-HOST", "cargo_vessel", {
        stow_capacity: 10,
        stow_used: 9,
      }),
      candidate("FULL-HOST", "cargo_vessel", {
        stow_capacity: 10,
        stow_used: 10,
      }),
    ];
    const contextualCatalog: DescriptorCatalog = {
      reports: [],
      workflows: [],
      actions: [
        action("device.adopt", "adopt", "elevated", [
          parameter("device", true, "device"),
          parameter("target", true, "device"),
        ]),
        action("device.release", "release", "elevated", [
          parameter("device", true, "device"),
          parameter("target", true, "device"),
        ]),
        action("device.stow", "stow", "elevated", [
          parameter("device", true, "device"),
          parameter("target", false, "device"),
        ]),
        action("device.set_directive", "set_directive", "elevated", [
          parameter("device", true, "device"),
          parameter("directive"),
        ]),
      ],
    };
    const commands = advertisedDeviceCommands(
      contextualCatalog,
      controller,
      {},
      devices,
      [
        {
          device_type: "transport_drone",
          short_description: null,
          description: null,
          print_time_seconds: null,
          resources: [],
          components: [],
          features: ["transport"],
          directives: [],
          cargo_capacity: 20,
          attach_capacity: 0,
          stow_capacity: 0,
          queue_size: null,
        },
        {
          device_type: "mining_drone",
          short_description: null,
          description: null,
          print_time_seconds: null,
          resources: [],
          components: [],
          features: ["mine"],
          directives: [],
          cargo_capacity: 0,
          attach_capacity: 0,
          stow_capacity: 0,
          queue_size: null,
        },
      ],
    );
    const options = (kind: string, parameterName: string) =>
      commands
        .find((command) => command.descriptor.kind === kind)
        ?.descriptor.parameters.find(
          (parameter) => parameter.name === parameterName,
        )
        ?.options.map((option) => option.value);

    expect(options("device.adopt", "target")).toEqual(["TRANSPORT"]);
    expect(options("device.release", "target")).toEqual(["ADOPTED"]);
    expect(options("device.stow", "target")).toEqual(["STOW-HOST"]);
    expect(options("device.set_directive", "directive")).toEqual([
      "delivery",
      "shuttle",
    ]);
  });
  it("shows directive configuration and places child devices after capabilities", () => {
    const html = renderToStaticMarkup(
      <DeviceInspector
        device={{
          ...device,
          active_directive: "ferry",
          directive_status: "running",
          directive_details: {
            directive: "ferry",
            configuration: {
              collect: "TARAZEDAR-BELT-1",
              deliver: "SOL-3-L4",
            },
          },
          directive_target_system: "SOL",
          available_commands: ["travel"],
          controlled_devices: ["TRANSPORT-1"],
        }}
        descriptors={catalog}
        entities={{}}
        onRunCommand={vi.fn()}
        onOperationFinished={vi.fn()}
      />,
    );

    expect(html).toContain("Directive");
    expect(html).toContain("Ferry");
    expect(html).toContain("Collect");
    expect(html).toContain("TARAZEDAR-BELT-1");
    expect(html).toContain("Deliver");
    expect(html).toContain("SOL-3-L4");
    expect(html.indexOf("Capabilities")).toBeLessThan(
      html.indexOf("Controlled devices"),
    );
  });
});
