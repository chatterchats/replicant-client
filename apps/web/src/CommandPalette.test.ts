import { describe, expect, it } from "vitest";

import {
  applicableDescriptorCommands,
  descriptorCommands,
  requiresTypedConfirmation,
  resolveContextDefaults,
  searchDescriptors,
  visibleParameters,
} from "./CommandPalette";
import type {
  ActionDescriptor,
  DescriptorCatalog,
  WorkflowDescriptor,
} from "./protocol";

const descriptor: WorkflowDescriptor = {
  kind: "survey.route",
  display_name: "Survey route",
  aliases: ["scan systems"],
  description: "Survey nearby systems",
  category: "missions",
  operation_class: "workflow",
  applicable_to: ["system", "replicant"],
  risk: "elevated",
  supported_triggers: ["manual"],
  parameters: [
    {
      name: "center",
      label: "Center",
      description: "Center",
      kind: { type: "system" },
      required: true,
      default: null,
      options: [],
      validation: {
        minimum: null,
        maximum: null,
        min_length: null,
        max_length: null,
      },
    },
    {
      name: "replicant",
      label: "Replicant",
      description: "Replicant",
      kind: { type: "replicant" },
      required: true,
      default: "fallback",
      options: [],
      validation: {
        minimum: null,
        maximum: null,
        min_length: null,
        max_length: null,
      },
    },
  ],
};
const catalog: DescriptorCatalog = {
  reports: [],
  actions: [],
  workflows: [descriptor],
};

describe("descriptor command discovery", () => {
  it.each(["Survey route", "scan systems", "missions", "system", "workflow"])(
    "finds descriptors by %s",
    (query) => {
      expect(searchDescriptors(catalog, query)).toHaveLength(1);
    },
  );

  it("hides compatibility workflows from normal command discovery", () => {
    const compatibility: WorkflowDescriptor = {
      ...descriptor,
      kind: "event.fulfillment",
      category: "compatibility",
    };
    expect(
      descriptorCommands({
        ...catalog,
        workflows: [descriptor, compatibility],
      }).map(({ descriptor: item }) => item.kind),
    ).toEqual(["survey.route"]);
  });

  it("only applies descriptors with matching semantic entity parameters", () => {
    expect(
      applicableDescriptorCommands(catalog, "system").map(
        ({ descriptor: item }) => item.kind,
      ),
    ).toEqual(["survey.route"]);
    expect(applicableDescriptorCommands(catalog, "device")).toEqual([]);
  });
});

it("resolves selected entity context ahead of static defaults", () => {
  expect(
    resolveContextDefaults(descriptor, { system: "SOL", replicant: "R-1" }),
  ).toEqual({
    center: "SOL",
    replicant: "R-1",
  });
});

it("uses selected device context only once when an action has two device parameters", () => {
  const baseParameter = descriptor.parameters[0];
  if (!baseParameter)
    throw new Error("test descriptor must define a parameter");
  const twoDeviceDescriptor: WorkflowDescriptor = {
    ...descriptor,
    kind: "device.attach",
    applicable_to: ["device"],
    parameters: [
      {
        ...baseParameter,
        name: "device",
        kind: { type: "device" },
      },
      {
        ...baseParameter,
        name: "target",
        kind: { type: "device" },
      },
    ],
  };
  expect(
    resolveContextDefaults(twoDeviceDescriptor, { device: "HOST-1" }),
  ).toEqual({
    device: "HOST-1",
    target: "",
  });
});

it.each([
  "device.lifecycle",
  "device.travel",
  "device.stow",
  "device.attach",
  "device.detach",
  "device.adopt",
  "device.release",
  "device.set_directive",
  "device.repair",
  "device.change_owner",
])("uses click confirmation for individual %s controls", (kind) => {
  const control: ActionDescriptor = {
    ...descriptor,
    kind,
    display_name: "Control device",
    operation_class: "action",
  };
  expect(
    requiresTypedConfirmation({
      descriptor: control,
      operationClass: "action",
    }),
  ).toBe(false);
});

it("shows only configuration fields used by the selected directive", () => {
  const base = descriptor.parameters[0];
  if (!base) throw new Error("test descriptor must define a parameter");
  const directive = {
    ...descriptor,
    kind: "device.set_directive",
    parameters: [
      "device",
      "directive",
      "resources_json",
      "location",
      "recall",
      "collect",
      "deliver",
      "priority",
      "name",
      "description",
      "announcement",
      "configuration_json",
      "notify_json",
    ].map((name) => ({ ...base, name })),
  };

  expect(
    visibleParameters(directive, { directive: "gather_salvage" }).map(
      (parameter) => parameter.name,
    ),
  ).toEqual(["device", "directive", "location", "recall", "notify_json"]);
  expect(
    visibleParameters(directive, { directive: "patrol" }).map(
      (parameter) => parameter.name,
    ),
  ).toEqual(["device", "directive", "configuration_json", "notify_json"]);
  expect(
    visibleParameters(directive, { directive: "trade" }).map(
      (parameter) => parameter.name,
    ),
  ).toEqual([
    "device",
    "directive",
    "name",
    "description",
    "announcement",
    "notify_json",
  ]);
});

it("keeps typed confirmation for bulk and non-device elevated actions", () => {
  const control: ActionDescriptor = {
    ...descriptor,
    kind: "device.lifecycle.bulk",
    display_name: "Control selected devices",
    operation_class: "action",
  };
  expect(
    requiresTypedConfirmation({
      descriptor: control,
      operationClass: "action",
    }),
  ).toBe(true);
  expect(
    requiresTypedConfirmation({
      descriptor: { ...control, kind: "trade.delete" },
      operationClass: "action",
    }),
  ).toBe(true);
});
