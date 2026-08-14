import { describe, expect, it } from "vitest";

import { resolveContextDefaults, searchDescriptors } from "./CommandPalette";
import type { DescriptorCatalog, WorkflowDescriptor } from "./protocol";

const descriptor: WorkflowDescriptor = {
  kind: "survey.route",
  display_name: "Survey route",
  aliases: ["scan systems"],
  description: "Survey nearby systems",
  category: "missions",
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
});

it("resolves selected entity context ahead of static defaults", () => {
  expect(
    resolveContextDefaults(descriptor, { system: "SOL", replicant: "R-1" }),
  ).toEqual({
    center: "SOL",
    replicant: "R-1",
  });
});
