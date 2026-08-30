import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type Mock,
} from "vitest";

import {
  configureWebTelemetryTransport,
  flushWebTelemetry,
  recordWebEvent,
} from "./telemetry";

type TelemetryRequest = {
  method?: string;
  body?: BodyInit | null;
};

function requestBody(request: TelemetryRequest) {
  expect(typeof request.body).toBe("string");
  if (typeof request.body !== "string")
    throw new Error("telemetry body is not text");
  return JSON.parse(request.body) as {
    events: Array<{ event: string; message: string }>;
  };
}

describe("web telemetry transport", () => {
  let fetchMock: Mock;

  beforeEach(() => {
    vi.useFakeTimers();
    fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 204 });
    vi.stubGlobal("fetch", fetchMock);
    configureWebTelemetryTransport("/api/frontend/telemetry");
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("sends one queued daemon event without duplicating it", async () => {
    recordWebEvent("info", "frontend.daemon_http", "daemon request completed", {
      method: "GET",
      path: "/api/health",
    });

    await flushWebTelemetry();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const body = requestBody(fetchMock.mock.calls[0]?.[1] as TelemetryRequest);
    expect(body.events).toHaveLength(1);
    expect(body.events[0]).toMatchObject({
      event: "frontend.daemon_http",
      message: "daemon request completed",
    });

    // A transport flush must not manufacture another daemon event or resend
    // the same event once the batch has completed successfully.
    await flushWebTelemetry();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("does not recurse when the telemetry POST itself is flushed", async () => {
    recordWebEvent(
      "info",
      "frontend.page_loaded",
      "initial document load completed",
    );

    await flushWebTelemetry();

    const [url, init] = fetchMock.mock.calls[0] as [string, TelemetryRequest];
    expect(url).toBe("/api/frontend/telemetry");
    expect(init.method).toBe("POST");
    const body = requestBody(init);
    expect(body.events).toHaveLength(1);
    expect(body.events[0]?.event).toBe("frontend.page_loaded");

    // If transport requests were fed back into browser request telemetry,
    // another queued event would be observable by a second flush.
    await flushWebTelemetry();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
