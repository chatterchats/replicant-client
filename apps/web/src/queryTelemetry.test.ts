import { beforeEach, describe, expect, it, vi } from "vitest";

import { recordWebEvent } from "./telemetry";
import {
  queryTelemetrySummary,
  recordQueryEvent,
  resetQueryTelemetryForTests,
} from "./queryTelemetry";

vi.mock("./telemetry", () => ({ recordWebEvent: vi.fn() }));

describe("query telemetry", () => {
  beforeEach(() => {
    resetQueryTelemetryForTests();
    vi.mocked(recordWebEvent).mockClear();
  });

  it("counts heavy projection requests and transferred bytes", () => {
    recordQueryEvent("request", { query: "/api/entities" });
    recordQueryEvent("request", { query: "/api/galaxy-scene" });
    recordQueryEvent("request", { query: "/api/devices" });
    recordQueryEvent("request_success", { bytes_received: 1_490_000 });

    expect(queryTelemetrySummary()).toMatchObject({
      requests_started: 3,
      bytes_received: 1_490_000,
      entities_fetches: 1,
      galaxy_scene_fetches: 1,
      devices_fetches: 1,
    });
  });

  it("records coalescing lifecycle counters and exact debug event names", () => {
    recordQueryEvent("joined_request", { query: "entities" });
    recordQueryEvent("coalesced_invalidation", { query: "entities" });
    recordQueryEvent("auto_refetch", { query: "entities" });
    recordQueryEvent("cache_hit", { query: "entities" });
    recordQueryEvent("cancelled_request", { query: "entities" });
    recordQueryEvent("stale_discarded", { query: "entities" });

    expect(queryTelemetrySummary()).toMatchObject({
      automatic_refetches: 1,
      requests_joined: 1,
      requests_cancelled: 1,
      cache_hits: 1,
      coalesced_invalidations: 1,
      stale_discarded: 1,
    });
    expect(vi.mocked(recordWebEvent).mock.calls.map((call) => call[1])).toEqual(
      [
        "frontend.query_joined",
        "frontend.query_coalesced",
        "frontend.query_cache_hit",
        "frontend.query_cancelled",
        "frontend.query_stale_discarded",
      ],
    );
  });
});
