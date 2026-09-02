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
  useDaemonState: () => ({
    connection: "connected",
    revision: 1,
    error: null,
    invalidated: {},
  }),
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

  it("renders addable resource and device manifests", () => {
    const resourceHtml = renderToStaticMarkup(
      <ParameterField
        parameter={parameter("resources", { type: "resource_manifest" })}
        value={{ silicates: 100 }}
        entities={{}}
        onChange={() => undefined}
      />,
    );
    const deviceHtml = renderToStaticMarkup(
      <ParameterField
        parameter={parameter("devices", { type: "device_manifest" })}
        value={[{ device_type: "mining_drone", quantity: 2 }]}
        entities={{ "device:D-1": { device_type: "mining_drone" } }}
        onChange={() => undefined}
      />,
    );

    expect(resourceHtml).toContain("silicates");
    expect(resourceHtml).toContain("+ Add resource");
    expect(deviceHtml).toContain("mining_drone");
    expect(deviceHtml).toContain("+ Add device");
  });

  it("offers only owned System Hub locations as dispatch sources", () => {
    const html = renderToStaticMarkup(
      <ParameterField
        parameter={parameter("source", { type: "location" })}
        value=""
        entities={{
          "location:ALPHA-HUB": {},
          "location:OTHER": {},
          "device:HUB-1": {
            device_type: "system_hub",
            location: "ALPHA-HUB",
          },
        }}
        operationKind="logistics.regional_dispatch"
        onChange={() => undefined}
      />,
    );

    expect(html).toContain("ALPHA-HUB");
    expect(html).not.toContain('value="OTHER"');
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

    await daemonApi.setDirectorGoal("asteroid_diversion", "alpha", true);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/goals/asteroid_diversion",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ region: "alpha", enabled: true }),
      }),
    );
  });

  it("sends the stranded recovery goal key with the regional toggle body", async () => {
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

    await daemonApi.setDirectorGoal("stranded_device_recovery", "alpha", true);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/goals/stranded_device_recovery",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ region: "alpha", enabled: true }),
      }),
    );
  });
  it("sends the Unserviced Resources regional toggle request", async () => {
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

    await daemonApi.setDirectorGoal("unserviced_resources", "alpha", true);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/goals/unserviced_resources",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ region: "alpha", enabled: true }),
      }),
    );
  });

  it("renders regional Director labels through the generic goal surface", async () => {
    vi.useFakeTimers();
    const recoveryDirector = (enabled: boolean, revision: number) => ({
      metadata: { revision, generated_at_ms: revision * 10 },
      mode: "automatic" as const,
      regions: [
        {
          region: "alpha",
          status: "established" as const,
          hub_system: "SOL",
          hub_location: "ALPHA-HUB",
          replicants: [],
          known_systems: 2,
        },
      ],
      goals: [
        {
          id: "stranded_device_recovery:alpha",
          kind: "stranded_device_recovery" as const,
          region: "alpha",
          status: "active" as const,
          objective: "Recover stranded owned devices to regional System Hubs",
          blocker: null,
          next_action:
            "Recover stranded device DEVICE-1 from ALPHA-BELT to ALPHA-HUB",
          progress_current: 0,
          progress_total: 2,
          active_workflows: ["WF-RECOVERY"],
          enabled,
        },
        {
          id: "unserviced_resources:alpha",
          kind: "unserviced_resources" as const,
          region: "alpha",
          status: "satisfied" as const,
          objective:
            "Establish AMI transport service for producing regional resources",
          blocker: null,
          next_action: null,
          progress_current: 1,
          progress_total: 1,
          active_workflows: [],
          enabled: false,
        },
        {
          id: "expand_ftl_network:alpha",
          kind: "expand_ftl_network" as const,
          region: "alpha",
          status: "satisfied" as const,
          objective: "Expand FTL network",
          blocker: null,
          next_action: null,
          progress_current: 0,
          progress_total: 0,
          active_workflows: [],
          enabled: false,
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
    });
    let directorGets = 0;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      let payload: unknown;
      if (url === "/api/descriptors") {
        payload = { reports: [], actions: [], workflows: [] };
      } else if (
        url === "/api/director/goals/stranded_device_recovery" &&
        init?.method === "PUT"
      ) {
        payload = recoveryDirector(true, 2);
      } else {
        directorGets += 1;
        payload = recoveryDirector(directorGets > 1, directorGets);
      }
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

    const campaign = container.querySelector(".director-regional-goal");
    const regionalGoals = container.querySelector(".director-regional-goals");
    const toggle = container.querySelector<HTMLInputElement>(
      'input[aria-label="Stranded Device Recovery in alpha"]',
    );
    expect(regionalGoals?.textContent).toContain("Unserviced Resources");
    expect(regionalGoals?.textContent).toContain(
      "Establish AMI transport service for producing regional resources",
    );
    expect(regionalGoals?.textContent).toContain("satisfied · 1 / 1");
    expect(regionalGoals?.textContent).toContain("Stranded Device Recovery");
    expect(regionalGoals?.textContent).toContain("Expand FTL Network");
    expect(campaign?.textContent).toContain(
      "Recover stranded owned devices to regional System Hubs",
    );
    expect(campaign?.textContent).toContain("active · 0 / 2");
    expect(campaign?.textContent).toContain(
      "Next: Recover stranded device DEVICE-1 from ALPHA-BELT to ALPHA-HUB",
    );
    expect(
      campaign?.querySelector<HTMLButtonElement>("button.text-button")
        ?.textContent,
    ).toBe("WF-RECOVERY");
    expect(toggle?.checked).toBe(false);

    await act(async () => {
      toggle?.click();
      await vi.runAllTimersAsync();
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/goals/stranded_device_recovery",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ region: "alpha", enabled: true }),
      }),
    );
    expect(directorGets).toBe(2);
    expect(
      container.querySelector<HTMLInputElement>(
        'input[aria-label="Stranded Device Recovery in alpha"]',
      )?.checked,
    ).toBe(true);

    act(() => {
      root?.unmount();
    });
    container.remove();
  });

  it("renders advisory actions, blockers, and satisfied recovery states", async () => {
    vi.useFakeTimers();
    const region = (name: string) => ({
      region: name,
      status: "established" as const,
      hub_system: "SOL",
      hub_location: `${name.toUpperCase()}-HUB`,
      replicants: [],
      known_systems: 2,
    });
    const goal = (
      region: string,
      status: "waiting" | "active" | "blocked" | "satisfied",
      overrides: Record<string, unknown> = {},
    ) => ({
      id: `stranded_device_recovery:${region}`,
      kind: "stranded_device_recovery" as const,
      region,
      status,
      objective: "Recover stranded owned devices to regional System Hubs",
      blocker: null,
      next_action: null,
      progress_current: status === "satisfied" ? 1 : 0,
      progress_total: status === "satisfied" ? 1 : 1,
      active_workflows: [],
      enabled: status !== "waiting",
      ...overrides,
    });
    const director = {
      metadata: { revision: 1, generated_at_ms: 10 },
      mode: "advisory" as const,
      regions: ["alpha", "beta", "gamma", "delta"].map(region),
      goals: [
        goal("alpha", "active", {
          next_action:
            "Recover stranded device DEVICE-1 from ALPHA-BELT to ALPHA-HUB",
          active_workflows: ["WF-RECOVERY"],
          enabled: true,
        }),
        goal("beta", "blocked", {
          blocker: "Placement authority is incomplete for this region",
          enabled: true,
        }),
        goal("gamma", "satisfied", { enabled: true }),
        goal("delta", "waiting", {
          next_action: "Enable Stranded Device Recovery for this region",
          enabled: false,
        }),
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
    };
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      const payload =
        url === "/api/descriptors"
          ? { reports: [], actions: [], workflows: [] }
          : director;
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

    expect(
      container.querySelector(".director-regional-goal.active")?.textContent,
    ).toContain(
      "Next: Recover stranded device DEVICE-1 from ALPHA-BELT to ALPHA-HUB",
    );
    expect(
      container.querySelector(".director-regional-goal.blocked")?.textContent,
    ).toContain("Blocked: Placement authority is incomplete for this region");
    expect(
      container.querySelector(".director-regional-goal.satisfied")?.textContent,
    ).toContain("satisfied · 1 / 1");
    expect(
      container.querySelector<HTMLInputElement>(
        'input[aria-label="Stranded Device Recovery in delta"]',
      )?.checked,
    ).toBe(false);

    act(() => {
      root?.unmount();
    });
    container.remove();
  });

  it("opens the active recovery workflow ID from a selected workflow", async () => {
    vi.useFakeTimers();
    const workflow = {
      id: "WF-RECOVERY",
      kind: "logistics.manifest",
      status: "running" as const,
      current_step: "deliver",
      revision: 3,
      updated_at_ms: 20,
    };
    const director = {
      metadata: { revision: 1, generated_at_ms: 10 },
      mode: "advisory" as const,
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
    const detail = {
      summary: workflow,
      schema_version: 1,
      parameters: { device_codes: ["DEVICE-1"] },
      wait_reason: null,
      parent_id: null,
      claims: [],
      created_at_ms: 10,
      finished_at_ms: null,
      error: null,
    };
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      let payload: unknown;
      if (url === "/api/descriptors") {
        payload = { reports: [], actions: [], workflows: [] };
      } else if (url === "/api/director") {
        payload = director;
      } else if (url === "/api/workflows/WF-RECOVERY") {
        payload = detail;
      } else {
        payload = { activity: [] };
      }
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
      mountedRoot.render(
        <AutomationsPage
          workflows={[workflow]}
          entities={{}}
          selectedWorkflowId="WF-RECOVERY"
        />,
      );
      await vi.runAllTimersAsync();
    });

    expect(container.querySelector('[aria-label="Active workflows"]')).not.toBe(
      null,
    );
    expect(
      container.querySelector(".workflow-inspector")?.textContent,
    ).toContain("WF-RECOVERY");
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/workflows/WF-RECOVERY",
      expect.anything(),
    );

    act(() => {
      root?.unmount();
    });
    container.remove();
  });

  it("renders and enables the regional Asteroid Diversion campaign", async () => {
    vi.useFakeTimers();
    const asteroidDirector = (enabled: boolean, revision: number) => ({
      metadata: { revision, generated_at_ms: revision * 10 },
      mode: "automatic" as const,
      regions: [
        {
          region: "alpha",
          status: "established" as const,
          hub_system: "SOL",
          hub_location: "SOL-L4",
          replicants: [],
          known_systems: 4,
        },
      ],
      goals: [
        {
          id: "asteroid_diversion:alpha",
          kind: "asteroid_diversion" as const,
          region: "alpha",
          status: "waiting" as const,
          objective: "Divert incoming asteroids threatening regional systems",
          blocker: null,
          next_action: "Enable Asteroid Diversion for this region",
          progress_current: 0,
          progress_total: 0,
          active_workflows: [],
          enabled,
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
    });
    let directorGets = 0;
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      let payload: unknown;
      if (url === "/api/descriptors") {
        payload = { reports: [], actions: [], workflows: [] };
      } else if (
        url === "/api/director/goals/asteroid_diversion" &&
        init?.method === "PUT"
      ) {
        payload = asteroidDirector(true, 2);
      } else {
        directorGets += 1;
        payload = asteroidDirector(directorGets > 1, directorGets);
      }
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

    const campaign = container.querySelector(".director-regional-goal");
    const toggle = container.querySelector<HTMLInputElement>(
      'input[aria-label="Asteroid Diversion in alpha"]',
    );
    expect(campaign?.textContent).toContain("Asteroid Diversion");
    expect(campaign?.textContent).toContain(
      "Divert incoming asteroids threatening regional systems",
    );
    expect(campaign?.textContent).toContain("waiting");
    expect(campaign?.textContent).toContain(
      "Next: Enable Asteroid Diversion for this region",
    );
    expect(toggle?.checked).toBe(false);

    await act(async () => {
      toggle?.click();
      await vi.runAllTimersAsync();
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/director/goals/asteroid_diversion",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ region: "alpha", enabled: true }),
      }),
    );
    expect(directorGets).toBe(2);
    expect(
      container.querySelector<HTMLInputElement>(
        'input[aria-label="Asteroid Diversion in alpha"]',
      )?.checked,
    ).toBe(true);

    act(() => {
      root?.unmount();
    });
    container.remove();
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
