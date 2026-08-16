import {
  parseAutofactoryResponse,
  parseAutomationControlResponse,
  parseBootstrapResponse,
  parseCargoResponse,
  parseDescriptorsResponse,
  parseDevicesResponse,
  parseInventoryResponse,
  parseLeaderboardsResponse,
  parseMessagesResponse,
  parseMiningResponse,
  parseNetworkResponse,
  parseRelayResponse,
  parseEntityIndexResponse,
  parseEventsResponse,
  parseFiniteExecutionHistoryResponse,
  parseGalaxySceneResponse,
  parseSystemSceneResponse,
  parseHealthResponse,
  parseOperationResponse,
  parseOverviewResponse,
  parseReportsResponse,
  parseSettingsResponse,
  parseSnapshotResponse,
  parseStandingResponse,
  parseSurveyResponse,
  parseTradeResponse,
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

/** Milliseconds before an unanswered daemon request is abandoned. */
const REQUEST_TIMEOUT_MS = 30_000;

/**
 * Shared secret for talking to an authenticated daemon *directly*.
 *
 * Not needed for the packaged stack: there the browser calls same-origin paths
 * and nginx adds the credential server-side, so the token never reaches the
 * page. Set this only when pointing a development build straight at a remote
 * daemon that bypasses that proxy.
 */
export function daemonToken(): string | undefined {
  const configured = (
    import.meta as unknown as {
      env: { VITE_REPLICANT_DAEMON_TOKEN?: string };
    }
  ).env.VITE_REPLICANT_DAEMON_TOKEN;
  return configured === undefined || configured === "" ? undefined : configured;
}

function authHeaders(base?: Record<string, string>): Record<string, string> {
  const token = daemonToken();
  return token === undefined
    ? (base ?? {})
    : { ...base, Authorization: `Bearer ${token}` };
}

/**
 * Aborts a request that outlives the caller's signal or the timeout.
 *
 * Without a timeout a wedged daemon left requests pending forever instead of
 * surfacing an error and letting the reconnect path run.
 */
function withTimeout(signal?: AbortSignal): {
  signal: AbortSignal;
  done: () => void;
} {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new Error("replicantd did not respond in time"));
  }, REQUEST_TIMEOUT_MS);
  const abort = () => {
    controller.abort(signal?.reason as unknown);
  };
  if (signal?.aborted) abort();
  else signal?.addEventListener("abort", abort);
  return {
    signal: controller.signal,
    done: () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    },
  };
}

async function get(path: string, signal?: AbortSignal): Promise<unknown> {
  const request = withTimeout(signal);
  try {
    const response = await fetch(daemonUrl(path), {
      signal: request.signal,
      headers: authHeaders(),
    });
    if (!response.ok)
      throw new Error(`replicantd returned ${String(response.status)}`);
    return (await response.json()) as unknown;
  } finally {
    request.done();
  }
}

async function post(path: string, body?: unknown): Promise<unknown> {
  return send("POST", path, body);
}

async function send(
  method: "POST" | "PUT" | "DELETE",
  path: string,
  body?: unknown,
): Promise<unknown> {
  const request = withTimeout();
  let response: Response;
  try {
    response = await fetch(daemonUrl(path), {
      method,
      signal: request.signal,
      headers: authHeaders(
        body === undefined ? undefined : { "Content-Type": "application/json" },
      ),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } finally {
    request.done();
  }
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
  async inventory(signal?: AbortSignal) {
    return parseInventoryResponse(await get("/api/inventory", signal)).payload;
  },
  async autofactories(signal?: AbortSignal) {
    return parseAutofactoryResponse(await get("/api/autofactories", signal))
      .payload;
  },
  async cargo(signal?: AbortSignal) {
    return parseCargoResponse(await get("/api/cargo", signal)).payload;
  },
  async survey(signal?: AbortSignal) {
    return parseSurveyResponse(await get("/api/missions/survey", signal))
      .payload;
  },
  async mining(signal?: AbortSignal) {
    return parseMiningResponse(await get("/api/missions/mining", signal))
      .payload;
  },
  async relay(signal?: AbortSignal) {
    return parseRelayResponse(await get("/api/missions/relay", signal)).payload;
  },
  async bootstrap(signal?: AbortSignal) {
    return parseBootstrapResponse(await get("/api/missions/bootstrap", signal))
      .payload;
  },
  async events(signal?: AbortSignal) {
    return parseEventsResponse(await get("/api/events", signal)).payload;
  },
  async trade(signal?: AbortSignal) {
    return parseTradeResponse(await get("/api/trade", signal)).payload;
  },
  async reports(signal?: AbortSignal) {
    return parseReportsResponse(await get("/api/reports", signal)).payload;
  },
  async messages(relay?: string, signal?: AbortSignal) {
    const query = relay ? `?relay=${encodeURIComponent(relay)}` : "";
    return parseMessagesResponse(await get(`/api/messages${query}`, signal))
      .payload;
  },
  async network(signal?: AbortSignal) {
    return parseNetworkResponse(await get("/api/network", signal)).payload;
  },
  async standing(signal?: AbortSignal) {
    return parseStandingResponse(await get("/api/standing", signal)).payload;
  },
  async leaderboards(board?: string, signal?: AbortSignal) {
    const query = board ? `?board=${encodeURIComponent(board)}` : "";
    return parseLeaderboardsResponse(
      await get(`/api/leaderboards${query}`, signal),
    ).payload;
  },
  async entities(signal?: AbortSignal) {
    return parseEntityIndexResponse(await get("/api/entities", signal)).payload;
  },
  async settings(signal?: AbortSignal) {
    return parseSettingsResponse(await get("/api/settings", signal)).payload;
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
