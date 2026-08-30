// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";

import { daemonApi } from "./api";
import {
  AutomationsPage,
  ParameterField,
  validateParameters,
} from "./AutomationsPage";
import type { ParameterDescriptor, WorkflowDescriptor } from "./protocol";

vi.mock("./daemon", () => ({
  useDaemonState: () => ({ invalidated: {} }),
}));

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
  vi.useRealTimers();
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
      operation_class: "workflow",
      applicable_to: [],
      risk: "low",
      supported_triggers: ["manual"],
      parameters: [parameter("replicant", { type: "replicant" })],
    };
    expect(validateParameters(descriptor, {})).toEqual({
      replicant: "Required",
    });
  });

  it("offers all devices when detach has no target", () => {
    const target = {
      ...parameter("target", { type: "device" } as const),
      required: false,
    };
    const html = renderToStaticMarkup(
      <ParameterField
        parameter={target}
        value=""
        entities={{ "device:D-1": { device_type: "carrier" } }}
        operationKind="device.detach"
        onChange={() => undefined}
      />,
    );

    expect(html).toContain("<select");
    expect(html).toContain("All devices");
    expect(html).toContain("D-1");
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

describe("Director goal controls", () => {
  it("sends the selected region with a goal toggle", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          protocol_version: 1,
          payload: {
            metadata: { revision: 2, generated_at_ms: 10 },
            mode: "automatic",
            regions: [],
            goals: [],
            replicants: [],
            requirements: [],
            workforce: {
              total: 0,
              busy: 0,
              idle: 0,
              idle_ratio: 1,
              pending_worker_demand: 0,
              scale_up_recommended: false,
              scale_reason: null,
            },
          },
        }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await daemonApi.setDirectorGoal("salvage_recovery", "alpha", true);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/goals/salvage_recovery",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ region: "alpha", enabled: true }),
      }),
    );
  });
  it("updates the regional mining density policy", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          protocol_version: 1,
          payload: {
            metadata: { revision: 3, generated_at_ms: 20 },
            mode: "automatic",
            regions: [],
            goals: [],
            mining_policies: [
              {
                region: "delta",
                expand_moderate: false,
                expand_sparse: true,
              },
            ],
            replicants: [],
            requirements: [],
            workforce: {
              total: 0,
              busy: 0,
              idle: 0,
              idle_ratio: 1,
              pending_worker_demand: 0,
              scale_up_recommended: false,
              scale_reason: null,
            },
          },
        }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await daemonApi.setDirectorMiningPolicy("delta", false, true);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/mining-policies/delta",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          expand_moderate: false,
          expand_sparse: true,
        }),
      }),
    );
  });

  it("keeps the manual salvage recovery template visible", async () => {
    vi.useFakeTimers();
    const descriptors = {
      reports: [],
      actions: [],
      workflows: [
        {
          kind: "salvage.recovery",
          display_name: "Recover regional salvage",
          aliases: ["salvage_recovery"],
          description: "Recover discovered salvage in a region.",
          category: "salvage",
          operation_class: "workflow",
          applicable_to: [],
          parameters: [],
          risk: "low",
          supported_triggers: ["manual"],
        },
      ],
    };
    const director = {
      metadata: { revision: 2, generated_at_ms: 10 },
      mode: "automatic",
      regions: [],
      goals: [],
      replicants: [],
      requirements: [],
      workforce: {
        total: 0,
        busy: 0,
        idle: 0,
        idle_ratio: 1,
        pending_worker_demand: 0,
        scale_up_recommended: false,
        scale_reason: null,
      },
    };
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      const payload = url === "/api/descriptors" ? descriptors : director;
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ protocol_version: 1, payload }),
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const container = document.createElement("div");
    document.body.appendChild(container);
    let root: Root | undefined;
    await act(async () => {
      const mountedRoot = createRoot(container);
      root = mountedRoot;
      mountedRoot.render(<AutomationsPage workflows={[]} entities={{}} />);
      await vi.runAllTimersAsync();
    });

    const startWorkflow =
      container.querySelector<HTMLButtonElement>("button.primary");
    expect(startWorkflow).not.toBeNull();
    act(() => {
      startWorkflow?.click();
    });
    expect(container.querySelector(".template-list")?.textContent).toContain(
      "Recover regional salvage",
    );

    act(() => {
      root?.unmount();
    });
    container.remove();
  });
});
