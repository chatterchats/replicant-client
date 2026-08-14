import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";

import { daemonApi } from "./api";
import { ParameterField, validateParameters } from "./AutomationsPage";
import type { ParameterDescriptor, WorkflowDescriptor } from "./protocol";

const validation = {
  minimum: null,
  maximum: null,
  min_length: null,
  max_length: null,
};

function parameter(
  name: string,
  kind: ParameterDescriptor["kind"],
): ParameterDescriptor {
  return {
    name,
    label: name,
    description: `${name} help`,
    kind,
    required: true,
    default: null,
    options: kind.type === "enum" ? [{ value: "run", label: "Run" }] : [],
    validation,
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("descriptor workflow form", () => {
  it("renders semantic selectors and native primitive controls", () => {
    const entities = {
      "system:SOL": {},
      "location:SOL-HUB": {},
      "replicant:R-1": {},
      "device:D-1": { device_type: "survey_drone" },
    };
    const kinds: ParameterDescriptor["kind"][] = [
      { type: "system" },
      { type: "location" },
      { type: "replicant" },
      { type: "device" },
      { type: "device_type" },
      { type: "enum" },
      { type: "boolean" },
      { type: "integer" },
      { type: "number" },
      { type: "string" },
    ];
    const html = kinds
      .map((kind, index) =>
        renderToStaticMarkup(
          <ParameterField
            parameter={parameter(`field-${String(index)}`, kind)}
            value={kind.type === "boolean" ? false : ""}
            entities={entities}
            onChange={() => undefined}
          />,
        ),
      )
      .join("");

    expect(html).toContain("SOL-HUB");
    expect(html).toContain("survey_drone");
    expect(html).toContain("<select");
    expect(html).toContain('type="checkbox"');
    expect(html).toContain('type="number"');
  });

  it("validates required descriptor fields before submission", () => {
    const descriptor: WorkflowDescriptor = {
      kind: "test.workflow",
      display_name: "Test",
      aliases: [],
      description: "Test",
      category: "test",
      risk: "low",
      supported_triggers: ["manual"],
      parameters: [parameter("replicant", { type: "replicant" })],
    };
    expect(validateParameters(descriptor, {})).toEqual({
      replicant: "Required",
    });
  });
});

describe("workflow lifecycle requests", () => {
  it.each(["pause", "resume", "cancel"] as const)(
    "posts the %s command to the workflow route",
    async (action) => {
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            protocol_version: 1,
            payload: {
              workflow: {
                id: "workflow id",
                kind: "survey.route",
                status: action === "cancel" ? "cancelled" : "paused",
                current_step: null,
                revision: 2,
                updated_at_ms: 10,
              },
            },
          }),
      });
      vi.stubGlobal("fetch", fetchMock);

      await daemonApi.controlWorkflow("workflow id", action);

      expect(fetchMock).toHaveBeenCalledWith(
        `/api/workflows/workflow%20id/${action}`,
        expect.objectContaining({ method: "POST" }),
      );
    },
  );
});
