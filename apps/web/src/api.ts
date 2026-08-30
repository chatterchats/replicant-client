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
  parseEntityInspectorResponse,
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
import {
  configureWebTelemetryTransport,
  recordWebEvent,
  type WebTelemetryFields,
} from "./telemetry";
import {
  resourceTimingFields,
  type ResourceTimingFields,
} from "./resourceTiming";
import { recordQueryRequest, recordQuerySuccess } from "./queryTelemetry";

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
/** A serial full-system traversal can legitimately exceed report generation. */
const LOCATION_REFRESH_TIMEOUT_MS = 10 * 60_000;

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
function responseHeader(response: Response, name: string): string | null {
  // Real Fetch responses always have Headers. Keep telemetry optional for
  // lightweight test/native adapters that provide only status/body fields.
  return (
    (response as Partial<Pick<Response, "headers">>).headers?.get(name) ?? null
  );
}

/**
 * Convert an nginx timing header from seconds to rounded milliseconds.
 *
 * Nginx joins values for each upstream attempt with commas when a request is
 * retried. Summing those attempts preserves the complete upstream duration;
 * selecting only the last value would hide time spent on failed attempts.
 * Any missing, placeholder, or malformed attempt makes the field unavailable
 * rather than reporting a misleading partial duration.
 */
export function proxyTimingMs(
  response: Response,
  header: string,
): number | null {
  const value = responseHeader(response, header);
  if (value === null) return null;
  const attempts = value.split(",").map((attempt) => attempt.trim());
  if (
    attempts.length === 0 ||
    attempts.some(
      (attempt) =>
        attempt === "" ||
        attempt === "-" ||
        !/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(attempt),
    )
  )
    return null;
  const seconds = attempts.reduce((total, attempt) => {
    const parsed = Number(attempt);
    return Number.isFinite(parsed) && parsed >= 0 ? total + parsed : Number.NaN;
  }, 0);
  return Number.isFinite(seconds) ? Math.round(seconds * 1_000) : null;
}

/** Remove query and fragment data before a path is placed in telemetry. */
function safeDaemonPath(path: string): string {
  const pathname = path.split(/[?#]/, 1)[0] ?? "";
  return pathname === "" ? "/" : pathname;
}

export function normalizeDaemonRoute(path: string): string {
  const pathname = safeDaemonPath(path).replace(/\/+/g, "/");
  const dynamicRoutes: ReadonlyArray<[RegExp, string]> = [
    [/^\/api\/refreshes\/[^/]+\/approve$/, "/api/refreshes/:run_id/approve"],
    [/^\/api\/refreshes\/[^/]+\/cancel$/, "/api/refreshes/:run_id/cancel"],
    [/^\/api\/refreshes\/[^/]+$/, "/api/refreshes/:run_id"],
    [/^\/api\/devices\/[^/]+\/logs$/, "/api/devices/:code/logs"],
    [/^\/api\/directory\/[^/]+$/, "/api/directory/:code"],
    [/^\/api\/entities\/[^/]+\/[^/]+$/, "/api/entities/:kind/:id"],
    [/^\/api\/system-scene\/[^/]+$/, "/api/system-scene/:system"],
    [/^\/api\/locations\/refresh\/[^/]+$/, "/api/locations/refresh/:system"],
    [/^\/api\/reports\/[^/]+$/, "/api/reports/:kind"],
    [/^\/api\/actions\/[^/]+$/, "/api/actions/:kind"],
    [
      /^\/api\/action-executions\/[^/]+\/cancel$/,
      "/api/action-executions/:id/cancel",
    ],
    [/^\/api\/triggers\/[^/]+\/fire$/, "/api/triggers/:id/fire"],
    [/^\/api\/triggers\/[^/]+$/, "/api/triggers/:id"],
    [/^\/api\/director\/goals\/[^/]+$/, "/api/director/goals/:kind"],
    [
      /^\/api\/director\/mining-policies\/[^/]+$/,
      "/api/director/mining-policies/:region",
    ],
    [
      /^\/api\/director\/replicants\/[^/]+\/region$/,
      "/api/director/replicants/:code/region",
    ],
    [/^\/api\/workflows\/[^/]+\/activity$/, "/api/workflows/:id/activity"],
    [/^\/api\/workflows\/[^/]+\/pause$/, "/api/workflows/:id/pause"],
    [/^\/api\/workflows\/[^/]+\/resume$/, "/api/workflows/:id/resume"],
    [/^\/api\/workflows\/[^/]+\/cancel$/, "/api/workflows/:id/cancel"],
    [/^\/api\/workflows\/[^/]+$/, "/api/workflows/:id"],
  ];
  return (
    dynamicRoutes.find(([pattern]) => pattern.test(pathname))?.[1] ?? pathname
  );
}

function safeErrorReason(reason: unknown): string {
  const message = reason instanceof Error ? reason.message : String(reason);
  return (message.split(/[?#]/, 1)[0] ?? "").slice(0, 500);
}

function daemonResponseFields(
  response: Response,
  method: string,
  path: string,
  elapsedMs: number,
  headersMs: number,
  bodyMs: number,
  browserTiming: ResourceTimingFields,
): WebTelemetryFields {
  const contentLength = Number.parseInt(
    responseHeader(response, "Content-Length") ?? "",
    10,
  );
  // Content-Length is authoritative when exposed. Resource Timing provides a
  // useful browser-side fallback for responses served with chunked encoding.
  const measurableBytes =
    Number.isFinite(contentLength) && contentLength >= 0
      ? Math.round(contentLength)
      : (browserTiming.encoded_bytes ??
        browserTiming.transfer_bytes ??
        browserTiming.decoded_bytes);
  return {
    method,
    path: safeDaemonPath(path),
    route: normalizeDaemonRoute(path),
    status: response.status,
    elapsed_ms: Math.round(elapsedMs),
    headers_ms: Math.round(headersMs),
    body_ms: Math.round(bodyMs),
    browser_body_parse_ms:
      browserTiming.browser_response_end_ms === null
        ? null
        : Math.round(
            Math.max(0, elapsedMs - browserTiming.browser_response_end_ms),
          ),
    ...browserTiming,
    bytes: measurableBytes,
    proxy_request_id: responseHeader(response, "X-Replicant-Request-Id"),
    // nginx exposes its request-time snapshot when response headers are
    // emitted. It is not the finalized access-log duration, so consumers
    // must treat it as a boundary observation rather than full request time.
    proxy_request_ms: proxyTimingMs(response, "X-Replicant-Request-Time"),
    proxy_connect_ms: proxyTimingMs(
      response,
      "X-Replicant-Upstream-Connect-Time",
    ),
    proxy_header_ms: proxyTimingMs(
      response,
      "X-Replicant-Upstream-Header-Time",
    ),
    // Ordinary response headers are emitted before nginx has seen the final
    // upstream body byte, so the finalized response duration belongs only to
    // the correlated access log. A browser-visible value is therefore null
    // unless another proxy supplies this header at a safe boundary.
    proxy_response_ms: proxyTimingMs(
      response,
      "X-Replicant-Upstream-Response-Time",
    ),
    daemon_handler_ms: proxyTimingMs(response, "X-Replicant-Handler-Time"),
  };
}

function recordDaemonResponse(
  response: Response,
  method: string,
  path: string,
  started: number,
  completed: number,
  headersMs: number,
  bodyMs: number,
) {
  const fields = daemonResponseFields(
    response,
    method,
    path,
    completed - started,
    headersMs,
    bodyMs,
    resourceTimingFields(daemonUrl(path), started, completed),
  );
  const level = !response.ok || completed - started >= 5_000 ? "warn" : "info";
  recordWebEvent(
    level,
    "frontend.daemon_http",
    response.ok ? "daemon request completed" : "daemon request was rejected",
    fields,
  );
  if (response.ok) {
    recordQuerySuccess(
      normalizeDaemonRoute(path),
      typeof fields.bytes === "number" ? fields.bytes : undefined,
      completed - started,
    );
  }
}

function recordDaemonFailure(
  method: string,
  path: string,
  started: number,
  reason: unknown,
  signal: AbortSignal,
) {
  const timeoutReason = signal.reason as unknown;
  const timedOut =
    timeoutReason instanceof Error &&
    timeoutReason.message === "replicantd did not respond in time";
  if (signal.aborted && !timedOut) return;
  recordWebEvent(
    timedOut ? "warn" : "error",
    "frontend.daemon_http_failed",
    timedOut ? "daemon request timed out" : "daemon request failed",
    {
      method,
      path: safeDaemonPath(path),
      route: normalizeDaemonRoute(path),
      elapsed_ms: Math.round(performance.now() - started),
      timed_out: timedOut,
      aborted: signal.aborted,
      error: safeErrorReason(reason),
    },
  );
}

function responseErrorMessage(value: unknown, status: number): string {
  if (value && typeof value === "object" && "payload" in value) {
    const payload: unknown = value.payload;
    if (
      payload &&
      typeof payload === "object" &&
      "message" in payload &&
      typeof payload.message === "string"
    )
      return payload.message;
  }
  return `replicantd returned ${String(status)}`;
}

const webMode = (import.meta as unknown as { env: { MODE?: string } }).env.MODE;
if (webMode !== "test") {
  configureWebTelemetryTransport(
    daemonUrl("/api/frontend/telemetry"),
    daemonToken(),
  );
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
  const started = performance.now();
  let response: Response | undefined;
  let headersAt = started;
  recordQueryRequest(normalizeDaemonRoute(path));
  let recorded = false;
  try {
    response = await fetch(daemonUrl(path), {
      signal: request.signal,
      headers: authHeaders(),
    });
    headersAt = performance.now();
    if (!response.ok) {
      recordDaemonResponse(
        response,
        "GET",
        path,
        started,
        headersAt,
        headersAt - started,
        0,
      );
      recorded = true;
      throw new Error(`replicantd returned ${String(response.status)}`);
    }
    const value = (await response.json()) as unknown;
    const completed = performance.now();
    recordDaemonResponse(
      response,
      "GET",
      path,
      started,
      completed,
      headersAt - started,
      completed - headersAt,
    );
    recorded = true;
    return value;
  } catch (reason: unknown) {
    if (response === undefined) {
      recordDaemonFailure("GET", path, started, reason, request.signal);
    } else if (!recorded) {
      const completed = performance.now();
      recordDaemonResponse(
        response,
        "GET",
        path,
        started,
        completed,
        headersAt - started,
        completed - headersAt,
      );
    }
    throw reason;
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
  const started = performance.now();
  let response: Response | undefined;
  let headersAt = started;
  let recorded = false;
  recordQueryRequest(normalizeDaemonRoute(path));
  try {
    response = await fetch(daemonUrl(path), {
      method,
      signal: request.signal,
      headers: authHeaders(
        body === undefined ? undefined : { "Content-Type": "application/json" },
      ),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    headersAt = performance.now();
    if (response.status === 204) {
      recordDaemonResponse(
        response,
        method,
        path,
        started,
        headersAt,
        headersAt - started,
        0,
      );
      recorded = true;
      return null;
    }
    const value = (await response.json()) as unknown;
    const completed = performance.now();
    recordDaemonResponse(
      response,
      method,
      path,
      started,
      completed,
      headersAt - started,
      completed - headersAt,
    );
    recorded = true;
    if (!response.ok) {
      throw new Error(responseErrorMessage(value, response.status));
    }
    return value;
  } catch (reason: unknown) {
    if (response === undefined) {
      recordDaemonFailure(method, path, started, reason, request.signal);
    } else if (!recorded) {
      const completed = performance.now();
      recordDaemonResponse(
        response,
        method,
        path,
        started,
        completed,
        headersAt - started,
        completed - headersAt,
      );
    }
    throw reason;
  } finally {
    request.done();
  }
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
  async setDirectorGoal(
    kind: DirectorGoalKind,
    region: string | null,
    enabled: boolean,
  ) {
    return parseDirectorResponse(
      await send("PUT", `/api/director/goals/${encodeURIComponent(kind)}`, {
        region,
        enabled,
      }),
    ).payload;
  },
  async setDirectorMiningPolicy(
    region: string,
    expandModerate: boolean,
    expandSparse: boolean,
  ) {
    return parseDirectorResponse(
      await send(
        "PUT",
        `/api/director/mining-policies/${encodeURIComponent(region)}`,
        {
          expand_moderate: expandModerate,
          expand_sparse: expandSparse,
        },
      ),
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
    const payload = parseDevicesResponse(
      await get("/api/devices", signal),
    ).payload;
    recordWebEvent(
      "info",
      "frontend.devices_loaded",
      "device projection loaded",
      { devices: payload.devices.length, revision: payload.metadata.revision },
    );
    return payload;
  },
  async entityInspector(kind: string, id: string, signal?: AbortSignal) {
    return parseEntityInspectorResponse(
      await get(
        `/api/entities/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`,
        signal,
      ),
    ).payload;
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
  async refreshMessages() {
    return parseMessagesResponse(await post("/api/messages/refresh")).payload;
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
    const payload = parseGalaxySceneResponse(
      await get("/api/galaxy-scene", signal),
    ).payload;
    recordWebEvent(
      "info",
      "frontend.galaxy_scene_parsed",
      "galaxy scene payload parsed",
      {
        revision: payload.revision,
        stars: payload.stars.length,
        relay_edges: payload.relay_edges.length,
        active_travel: payload.active_travel.length,
      },
    );
    return payload;
  },
  async systemScene(system: string, signal?: AbortSignal) {
    return parseSystemSceneResponse(
      await get(`/api/system-scene/${encodeURIComponent(system)}`, signal),
    ).payload;
  },
  async refreshLocations(system?: string) {
    const suffix = system === undefined ? "" : `/${encodeURIComponent(system)}`;
    await post(
      `/api/locations/refresh${suffix}`,
      undefined,
      LOCATION_REFRESH_TIMEOUT_MS,
    );
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
