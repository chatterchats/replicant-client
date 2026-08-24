import {
  parseActivityResponse,
  parseAutofactoryResponse,
  parseBillFinderResponse,
  parseBlueprintsResponse,
  parseBobnetResponse,
  parseAutomationControlResponse,
  parseDirectorResponse,
  parseBootstrapResponse,
  parseCargoResponse,
  parseDescriptorsResponse,
  parseDeviceLogsResponse,
  parseDevicesResponse,
  parseDirectoryReplicantResponse,
  parseDirectoryResponse,
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
  parseSimulationsResponse,
  parseSnapshotResponse,
  parseStandingResponse,
  parseSurveyResponse,
  parseTradeResponse,
  parseTutorialsResponse,
  parseTriggerListResponse,
  parseTriggerResponse,
  parseWorkflowActivityResponse,
  parseWorkflowDetailResponse,
  parseWorkflowResponse,
} from "./protocol";
import type {
  AutomationControlAction,
  BillFinderRequest,
  DirectorGoalKind,
  DirectorMode,
  TriggerRequest,
} from "./protocol";

export function daemonUrl(path: string, origin?: string): string {
  const configuredOrigin = (
    import.meta as unknown as {
      env: { VITE_REPLICANT_DAEMON_ORIGIN?: string };
    }
  ).env.VITE_REPLICANT_DAEMON_ORIGIN;
  return `${(origin ?? configuredOrigin)?.replace(/\/+$/, "") ?? ""}${path}`;
}

/** Milliseconds before an ordinary unanswered daemon request is abandoned. */
const REQUEST_TIMEOUT_MS = 30_000;
/** Reports may intentionally refresh a bounded set of upstream systems. */
const REPORT_TIMEOUT_MS = 110_000;

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
function withTimeout(
  signal?: AbortSignal,
  timeoutMs = REQUEST_TIMEOUT_MS,
): {
  signal: AbortSignal;
  done: () => void;
} {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new Error("replicantd did not respond in time"));
  }, timeoutMs);
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

async function post(
  path: string,
  body?: unknown,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<unknown> {
  return send("POST", path, body, timeoutMs);
}

async function send(
  method: "POST" | "PUT" | "DELETE",
  path: string,
  body?: unknown,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<unknown> {
  const request = withTimeout(undefined, timeoutMs);
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
  async director(signal?: AbortSignal) {
    return parseDirectorResponse(await get("/api/director", signal)).payload;
  },
  async reconcileDirector() {
    return parseDirectorResponse(
      await post("/api/director/reconcile", undefined, 60_000),
    ).payload;
  },
  async setDirectorMode(mode: DirectorMode) {
    return parseDirectorResponse(
      await send("PUT", "/api/director/mode", { mode }),
    ).payload;
  },
  async setDirectorGoal(kind: DirectorGoalKind, enabled: boolean) {
    return parseDirectorResponse(
      await send("PUT", `/api/director/goals/${encodeURIComponent(kind)}`, {
        enabled,
      }),
    ).payload;
  },
  async assignDirectorReplicant(
    code: string,
    region: string | null,
    roleAffinity: string | null = null,
  ) {
    return parseDirectorResponse(
      await send(
        "PUT",
        `/api/director/replicants/${encodeURIComponent(code)}/region`,
        { region, role_affinity: roleAffinity },
      ),
    ).payload;
  },
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
  async activity(
    options: {
      device?: string;
      name?: string;
      amiOnly?: boolean;
      limit?: number;
    } = {},
    signal?: AbortSignal,
  ) {
    const params = new URLSearchParams();
    if (options.device) params.set("device", options.device);
    if (options.name) params.set("name", options.name);
    if (options.amiOnly) params.set("ami_only", "true");
    if (options.limit !== undefined) params.set("limit", String(options.limit));
    const query = params.size > 0 ? `?${params.toString()}` : "";
    return parseActivityResponse(await get(`/api/activity${query}`, signal))
      .payload;
  },
  async deviceLogs(code: string, signal?: AbortSignal) {
    return parseDeviceLogsResponse(
      await get(
        `/api/devices/${encodeURIComponent(code)}/logs?limit=100`,
        signal,
      ),
    ).payload;
  },
  async simulations(signal?: AbortSignal) {
    return parseSimulationsResponse(await get("/api/simulations", signal))
      .payload;
  },
  async blueprints(signal?: AbortSignal) {
    return parseBlueprintsResponse(await get("/api/blueprints", signal))
      .payload;
  },
  async directory(name?: string, signal?: AbortSignal) {
    const query = name ? `?name=${encodeURIComponent(name)}` : "";
    return parseDirectoryResponse(await get(`/api/directory${query}`, signal))
      .payload;
  },
  async directoryReplicant(code: string, signal?: AbortSignal) {
    return parseDirectoryReplicantResponse(
      await get(`/api/directory/${encodeURIComponent(code)}`, signal),
    ).payload;
  },
  async tutorials(slug?: string, signal?: AbortSignal) {
    const query = slug ? `?slug=${encodeURIComponent(slug)}` : "";
    return parseTutorialsResponse(await get(`/api/tutorials${query}`, signal))
      .payload;
  },
  async trade(signal?: AbortSignal) {
    return parseTradeResponse(await get("/api/trade", signal)).payload;
  },
  async findBill(request: BillFinderRequest) {
    return parseBillFinderResponse(await post("/api/trade/bill/find", request))
      .payload;
  },
  async reports(signal?: AbortSignal) {
    return parseReportsResponse(await get("/api/reports", signal)).payload;
  },
  async messages(signal?: AbortSignal) {
    return parseMessagesResponse(await get("/api/messages", signal)).payload;
  },
  async markMessagesRead(options: { ids?: number[]; markAll?: boolean }) {
    return parseMessagesResponse(
      await post("/api/messages/read", {
        ids: options.ids ?? [],
        mark_all: options.markAll ?? false,
      }),
    ).payload;
  },
  async bobnet(
    options: { source?: string; includeNpcs?: boolean; cursor?: number } = {},
    signal?: AbortSignal,
  ) {
    const query = new URLSearchParams();
    if (options.source) query.set("source", options.source);
    if (typeof options.includeNpcs === "boolean")
      query.set("include_npcs", String(options.includeNpcs));
    if (typeof options.cursor === "number")
      query.set("cursor", String(options.cursor));
    query.set("limit", "100");
    const suffix = query.size ? `?${query.toString()}` : "";
    return parseBobnetResponse(await get(`/api/bobnet${suffix}`, signal))
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
        operationClass === "report" ? REPORT_TIMEOUT_MS : REQUEST_TIMEOUT_MS,
      ),
    ).payload;
  },
  async cancelAction(id: string) {
    await post(`/api/action-executions/${encodeURIComponent(id)}/cancel`);
  },
  async controlWorkflow(id: string, action: "pause" | "resume" | "cancel") {
    return parseWorkflowResponse(
      await post(`/api/workflows/${encodeURIComponent(id)}/${action}`),
    ).payload;
  },
};
