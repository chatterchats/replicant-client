import { afterEach, describe, expect, it, vi } from "vitest";

import { resourceTimingFields } from "./resourceTiming";

type ResourceEntry = Partial<PerformanceResourceTiming> & { name: string };

function installEntries(entries: ResourceEntry[]) {
  vi.stubGlobal("performance", {
    getEntriesByName: vi.fn(() => entries),
  });
}

describe("resource timing extraction", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("extracts browser milestones, phases, TLS, and transfer sizes", () => {
    installEntries([
      {
        name: "http://localhost/api/health",
        startTime: 100,
        fetchStart: 110,
        requestStart: 120,
        responseStart: 150,
        responseEnd: 180,
        domainLookupStart: 101,
        domainLookupEnd: 106,
        connectStart: 120,
        secureConnectionStart: 125,
        connectEnd: 135,
        transferSize: 300,
        encodedBodySize: 240,
        decodedBodySize: 480,
      },
    ]);

    expect(
      resourceTimingFields("http://localhost/api/health", 100, 200),
    ).toEqual({
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
      browser_tls_ms: 10,
      transfer_bytes: 300,
      encoded_bytes: 240,
      decoded_bytes: 480,
      connection_reused: false,
    });
  });

  it("keeps omitted and zero sentinel fields unavailable", () => {
    installEntries([
      {
        name: "http://localhost/api/health",
        startTime: 100,
        fetchStart: 110,
        requestStart: 120,
        responseStart: 150,
        responseEnd: 180,
        domainLookupStart: 0,
        domainLookupEnd: 0,
        connectStart: 0,
        connectEnd: 0,
        secureConnectionStart: 0,
        transferSize: 0,
      },
    ]);

    expect(
      resourceTimingFields("http://localhost/api/health", 100, 200),
    ).toMatchObject({
      browser_dns_ms: null,
      browser_connect_ms: null,
      browser_tls_ms: null,
      connection_reused: null,
      transfer_bytes: 0,
      encoded_bytes: null,
      decoded_bytes: null,
    });
  });

  it("correlates repeated resource names only inside each initiating window", () => {
    const old = {
      name: "http://localhost/api/snapshot",
      startTime: 80,
      responseEnd: 90,
      fetchStart: 80,
      requestStart: 81,
      responseStart: 85,
    };
    const first = {
      name: "http://localhost/api/snapshot",
      startTime: 100,
      responseEnd: 120,
      fetchStart: 102,
      requestStart: 104,
      responseStart: 110,
    };
    const second = {
      name: "http://localhost/api/snapshot",
      startTime: 140,
      responseEnd: 160,
      fetchStart: 143,
      requestStart: 146,
      responseStart: 150,
    };
    const future = {
      name: "http://localhost/api/snapshot",
      startTime: 180,
      responseEnd: 200,
    };
    installEntries([old, first, second, future]);

    expect(
      resourceTimingFields("http://localhost/api/snapshot", 100, 130),
    ).toMatchObject({
      browser_queue_ms: 2,
      browser_transfer_ms: 10,
    });
    expect(
      resourceTimingFields("http://localhost/api/snapshot", 140, 170),
    ).toMatchObject({
      browser_queue_ms: 3,
      browser_transfer_ms: 10,
    });
    expect(
      resourceTimingFields("http://localhost/api/snapshot", 170, 175),
    ).toEqual({
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
});
