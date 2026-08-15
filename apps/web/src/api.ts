import {
  parseAutomationControlResponse,
  parseDescriptorsResponse,
  parseDevicesResponse,
  parseEntityIndexResponse,
  parseFiniteExecutionHistoryResponse,
  parseGalaxySceneResponse,
  parseSystemSceneResponse,
  parseHealthResponse,
  parseOperationResponse,
  parseOverviewResponse,
  parseSnapshotResponse,
  parseTriggerListResponse,
  parseTriggerResponse,
  parseWorkflowActivityResponse,
  parseWorkflowDetailResponse,
  parseWorkflowResponse,
} from "./protocol";
import type { AutomationControlAction, TriggerRequest } from "./protocol";

export function daemonUrl(path: string, origin?: string): string {
  const configuredOrigin = (
    import.meta as unknown as {
      env: { VITE_REPLICANT_DAEMON_ORIGIN?: string };
    }
  ).env.VITE_REPLICANT_DAEMON_ORIGIN;
  return `${(origin ?? configuredOrigin)?.replace(/\/+$/, "") ?? ""}${path}`;
}

async function get(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(daemonUrl(path), { signal });
  if (!response.ok)
    throw new Error(`replicantd returned ${String(response.status)}`);
  return response.json() as Promise<unknown>;
}

async function post(path: string, body?: unknown): Promise<unknown> {
  return send("POST", path, body);
}

async function send(
  method: "POST" | "PUT" | "DELETE",
  path: string,
  body?: unknown,
): Promise<unknown> {
  const response = await fetch(daemonUrl(path), {
    method,
    headers:
      body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.status === 204) return null;
  const value = (await response.json()) as unknown;
  if (!response.ok) {
    const payload = (value as { payload?: { message?: unknown } }).payload;
    throw new Error(
      typeof payload?.message === "string"
        ? payload.message
        : `replicantd returned ${String(response.status)}`,
    );
  }
  return value;
}

export const daemonApi = {
  async controlAutomation(
    action: AutomationControlAction,
    workflowIds: string[] = [],
    confirmed = false,
  ) {
    return parseAutomationControlResponse(
      await post("/api/automation/control", {
        action,
        workflow_ids: workflowIds,
        confirmed,
      }),
    ).payload;
  },
  async health(signal?: AbortSignal) {
    return parseHealthResponse(await get("/api/health", signal)).payload;
  },
  async snapshot(signal?: AbortSignal) {
    return parseSnapshotResponse(await get("/api/snapshot", signal)).payload;
  },
  async overview(signal?: AbortSignal) {
    return parseOverviewResponse(await get("/api/overview", signal)).payload;
  },
  async devices(signal?: AbortSignal) {
    return parseDevicesResponse(await get("/api/devices", signal)).payload;
  },
  async entities(signal?: AbortSignal) {
    return parseEntityIndexResponse(await get("/api/entities", signal)).payload;
  },
  async galaxyScene(signal?: AbortSignal) {
    return parseGalaxySceneResponse(await get("/api/galaxy-scene", signal))
      .payload;
  },
  async systemScene(system: string, signal?: AbortSignal) {
    return parseSystemSceneResponse(
      await get(`/api/system-scene/${encodeURIComponent(system)}`, signal),
    ).payload;
  },
  async descriptors(signal?: AbortSignal) {
    return parseDescriptorsResponse(await get("/api/descriptors", signal))
      .payload;
  },
  async workflow(id: string, signal?: AbortSignal) {
    return parseWorkflowDetailResponse(
      await get(`/api/workflows/${encodeURIComponent(id)}`, signal),
    ).payload;
  },
  async workflowActivity(id: string, signal?: AbortSignal) {
    return parseWorkflowActivityResponse(
      await get(`/api/workflows/${encodeURIComponent(id)}/activity`, signal),
    ).payload;
  },
  async history(signal?: AbortSignal) {
    return parseFiniteExecutionHistoryResponse(
      await get("/api/history", signal),
    ).payload;
  },
  async triggers(signal?: AbortSignal) {
    return parseTriggerListResponse(await get("/api/triggers", signal)).payload;
  },
  async createTrigger(request: TriggerRequest) {
    return parseTriggerResponse(await post("/api/triggers", request)).payload;
  },
  async updateTrigger(id: string, revision: number, request: TriggerRequest) {
    return parseTriggerResponse(
      await send("PUT", `/api/triggers/${encodeURIComponent(id)}`, {
        expected_revision: revision,
        ...request,
      }),
    ).payload;
  },
  async deleteTrigger(id: string) {
    await send("DELETE", `/api/triggers/${encodeURIComponent(id)}`);
  },
  async fireTrigger(id: string) {
    return parseTriggerResponse(
      await post(`/api/triggers/${encodeURIComponent(id)}/fire`),
    ).payload;
  },
  async startWorkflow(kind: string, parameters: Record<string, unknown>) {
    return parseWorkflowResponse(
      await post("/api/workflows", { kind, parameters }),
    ).payload;
  },
  async runOperation(
    operationClass: "report" | "action",
    kind: string,
    parameters: Record<string, unknown>,
  ) {
    return parseOperationResponse(
      await post(
        `/api/${operationClass === "report" ? "reports" : "actions"}/${encodeURIComponent(kind)}`,
        { parameters },
      ),
    ).payload;
  },
  async controlWorkflow(id: string, action: "pause" | "resume" | "cancel") {
    return parseWorkflowResponse(
      await post(`/api/workflows/${encodeURIComponent(id)}/${action}`),
    ).payload;
  },
};
