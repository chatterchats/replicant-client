import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type Mock,
} from "vitest";

import { daemonApi, normalizeDaemonRoute, proxyTimingMs } from "./api";
import { flushWebTelemetry, configureWebTelemetryTransport } from "./telemetry";
import { resourceTimingFields } from "./resourceTiming";
type ResourceEntry = Record<string, unknown>;

function responseWithHeaders(headers: Record<string, string>): Response {
  return { headers: new Headers(headers) } as Response;
}
function daemonJsonResponse(
  payload: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers(headers),
    json: () => Promise.resolve(payload),
  } as Response;
}
const healthPayload = {
  protocol_version: 1,
  payload: { status: "healthy", daemon_version: "0.1.0", detail: null },
};
type TelemetryRequest = {
  method?: string;
  body?: BodyInit | null;
};

type TelemetryEnvelope = {
  events: Array<{
    level: string;
    event: string;
    fields: Record<string, unknown>;
  }>;
};

function telemetryEvents(fetchMock: Mock): TelemetryEnvelope["events"] {
  const call = fetchMock.mock.calls.find(
    ([url]) => String(url) === "/api/frontend/telemetry",
  );
  expect(call).toBeDefined();
  const request = call?.[1] as TelemetryRequest;
  expect(typeof request.body).toBe("string");
  if (typeof request.body !== "string")
    throw new Error("telemetry body is not text");
  return (JSON.parse(request.body) as TelemetryEnvelope).events;
}

describe("daemon telemetry timing helpers", () => {
  let fetchMock: Mock;
  let daemonResponse: Response;
  let now: Mock;

  beforeEach(() => {
    vi.useFakeTimers();
    daemonResponse = responseWithHeaders({});
    now = vi.fn().mockReturnValue(0);
    vi.stubGlobal("performance", {
      now,
      getEntriesByName: vi.fn(() => []),
    });
    fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      return Promise.resolve(
        url === "/api/frontend/telemetry"
          ? { ok: true, status: 204 }
          : daemonResponse,
      );
    });
    vi.stubGlobal("fetch", fetchMock);
    configureWebTelemetryTransport("/api/frontend/telemetry");
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("sums every valid upstream attempt in nginx timing headers", () => {
    const response = responseWithHeaders({
      "X-Replicant-Upstream-Connect-Time": "0.0126",
      "X-Replicant-Upstream-Header-Time": "0.0344, 0.0456",
    });

    expect(proxyTimingMs(response, "X-Replicant-Upstream-Connect-Time")).toBe(
      13,
    );
    expect(proxyTimingMs(response, "X-Replicant-Upstream-Header-Time")).toBe(
      80,
    );
  });

  it("returns null when an nginx timing attempt is absent or malformed", () => {
    const response = responseWithHeaders({
      absent: "",
      placeholder: "-",
      invalid: "not-a-duration",
      "invalid-retry": "0.010, nope",
      "negative-retry": "0.010, -0.002",
      "empty-retry": "0.010, ",
    });

    expect(proxyTimingMs(response, "absent")).toBeNull();
    expect(proxyTimingMs(response, "placeholder")).toBeNull();
    expect(proxyTimingMs(response, "invalid")).toBeNull();
    expect(proxyTimingMs(response, "invalid-retry")).toBeNull();
    expect(proxyTimingMs(response, "negative-retry")).toBeNull();
    expect(proxyTimingMs(response, "empty-retry")).toBeNull();
  });

  it("normalizes query-bearing daemon paths without retaining payloads", () => {
    expect(normalizeDaemonRoute("/api/entities?system=SOL&limit=10")).toBe(
      "/api/entities",
    );
    expect(normalizeDaemonRoute("/api/devices/123/logs?token=secret")).toBe(
      "/api/devices/:code/logs",
    );
    expect(normalizeDaemonRoute("/api/health")).toBe("/api/health");
    expect(normalizeDaemonRoute("/api/workflows/WF-ALPHA/activity")).toBe(
      "/api/workflows/:id/activity",
    );
  });

  it("correlates a resource entry into separate browser timing phases", () => {
    const entry: ResourceEntry = {
      name: "/api/health",
      startTime: 100,
      fetchStart: 110,
      requestStart: 120,
      responseStart: 150,
      responseEnd: 180,
      domainLookupStart: 101,
      domainLookupEnd: 106,
      connectStart: 120,
      connectEnd: 135,
      transferSize: 300,
      encodedBodySize: 240,
      decodedBodySize: 480,
    };
    vi.stubGlobal("performance", {
      getEntriesByName: vi.fn(() => [entry]),
    });

    expect(resourceTimingFields("/api/health", 100, 200)).toEqual({
      browser_fetch_start_ms: 10,
      browser_request_start_ms: 20,
      browser_response_start_ms: 50,
      browser_response_end_ms: 80,
      browser_queue_ms: 10,
      browser_request_ms: 30,
      browser_network_ms: 70,
      browser_transfer_ms: 30,
      browser_dns_ms: 5,
      browser_connect_ms: 15,
      browser_tls_ms: null,
      transfer_bytes: 300,
      encoded_bytes: 240,
      decoded_bytes: 480,
      connection_reused: false,
    });
  });

  it("claims duplicate identical-URL entries independently and leaves absent values null", () => {
    const first: ResourceEntry = {
      name: "/api/snapshot",
      startTime: 100,
      fetchStart: 102,
      requestStart: 104,
      responseStart: 110,
      responseEnd: 120,
    };
    const second: ResourceEntry = {
      name: "/api/snapshot",
      startTime: 100,
      fetchStart: 103,
      requestStart: 106,
      responseStart: 130,
      responseEnd: 150,
    };
    vi.stubGlobal("performance", {
      getEntriesByName: vi.fn(() => [first, second]),
    });

    expect(resourceTimingFields("/api/snapshot", 100, 160)).toMatchObject({
      browser_queue_ms: 2,
      browser_transfer_ms: 10,
    });
    expect(resourceTimingFields("/api/snapshot", 100, 160)).toMatchObject({
      browser_queue_ms: 3,
      browser_transfer_ms: 20,
    });
    expect(resourceTimingFields("/api/missing", 100, 160)).toEqual({
      browser_fetch_start_ms: null,
      browser_request_start_ms: null,
      browser_response_start_ms: null,
      browser_response_end_ms: null,
      browser_queue_ms: null,
      browser_request_ms: null,
      browser_network_ms: null,
      browser_transfer_ms: null,
      browser_dns_ms: null,
      browser_connect_ms: null,
      browser_tls_ms: null,
      transfer_bytes: null,
      encoded_bytes: null,
      decoded_bytes: null,
      connection_reused: null,
    });
  });
  it("records one correlated daemon event with proxy timings and request ID", async () => {
    const resourceEntry: ResourceEntry = {
      name: "/api/health",
      startTime: 100,
      fetchStart: 105,
      requestStart: 110,
      responseStart: 130,
      responseEnd: 150,
      domainLookupStart: 101,
      domainLookupEnd: 104,
      connectStart: 110,
      connectEnd: 120,
      transferSize: 42,
      encodedBodySize: 40,
      decodedBodySize: 80,
    };
    vi.stubGlobal("performance", {
      now,
      getEntriesByName: vi.fn(() => [resourceEntry]),
    });
    daemonResponse = daemonJsonResponse(healthPayload, 200, {
      "Content-Length": "42",
      "X-Replicant-Request-Id": "req-abc",
      "X-Replicant-Request-Time": "0.008",
      "X-Replicant-Upstream-Connect-Time": "0.010, 0.003",
      "X-Replicant-Upstream-Header-Time": "0.020, 0.005",
      "X-Replicant-Handler-Time": "0.004",
    });
    now
      .mockReturnValueOnce(100)
      .mockReturnValueOnce(120)
      .mockReturnValueOnce(180);

    await expect(daemonApi.health()).resolves.toMatchObject({
      status: "healthy",
    });
    await flushWebTelemetry();

    const events = telemetryEvents(fetchMock).filter(
      ({ event }) => event === "frontend.daemon_http",
    );
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      level: "info",
      event: "frontend.daemon_http",
      fields: {
        method: "GET",
        path: "/api/health",
        route: "/api/health",
        status: 200,
        elapsed_ms: 80,
        headers_ms: 20,
        browser_body_parse_ms: 30,
        browser_fetch_start_ms: 5,
        browser_request_start_ms: 10,
        browser_response_start_ms: 30,
        browser_response_end_ms: 50,
        browser_queue_ms: 5,
        browser_request_ms: 20,
        browser_network_ms: 45,
        browser_transfer_ms: 20,
        bytes: 42,
        proxy_request_id: "req-abc",
        proxy_request_ms: 8,
        proxy_connect_ms: 13,
        proxy_header_ms: 25,
        proxy_response_ms: null,
        daemon_handler_ms: 4,
      },
    });
  });

  it("marks a request at the five-second boundary as slow", async () => {
    daemonResponse = daemonJsonResponse(healthPayload);
    now
      .mockReturnValueOnce(1_000)
      .mockReturnValueOnce(1_100)
      .mockReturnValueOnce(6_000);

    await daemonApi.health();
    await flushWebTelemetry();

    expect(
      telemetryEvents(fetchMock).filter(
        ({ event }) => event === "frontend.daemon_http",
      ),
    ).toEqual([
      expect.objectContaining({
        level: "warn",
        event: "frontend.daemon_http",
      }),
    ]);
  });

  it("does not duplicate the daemon event when a non-ok response is rejected", async () => {
    daemonResponse = daemonJsonResponse(undefined, 503, {
      "X-Replicant-Request-Id": "req-failed",
    });

    await expect(daemonApi.health()).rejects.toThrow("replicantd returned 503");
    await flushWebTelemetry();

    const events = telemetryEvents(fetchMock);
    const daemonEvents = events.filter(
      ({ event }) => event === "frontend.daemon_http",
    );
    expect(daemonEvents).toHaveLength(1);
    expect(daemonEvents[0]?.fields).toMatchObject({
      proxy_request_id: "req-failed",
      status: 503,
    });
  });
});
