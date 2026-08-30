export type ResourceTimingFields = {
  browser_fetch_start_ms: number | null;
  browser_request_start_ms: number | null;
  browser_response_start_ms: number | null;
  browser_response_end_ms: number | null;
  browser_queue_ms: number | null;
  browser_request_ms: number | null;
  browser_network_ms: number | null;
  browser_transfer_ms: number | null;
  browser_dns_ms: number | null;
  browser_connect_ms: number | null;
  transfer_bytes: number | null;
  encoded_bytes: number | null;
  decoded_bytes: number | null;
  connection_reused: boolean | null;
};

const NO_RESOURCE_TIMING: ResourceTimingFields = {
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
  transfer_bytes: null,
  encoded_bytes: null,
  decoded_bytes: null,
  connection_reused: null,
};

// A ResourceTiming entry is not keyed by a fetch/request id. Keep claimed
// entries by identity so concurrent fetches of the same URL cannot emit the
// same browser timing twice.
const claimedEntries: WeakSet<object> = new WeakSet();
const TIMING_TOLERANCE_MS = 100;

function finite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function duration(start: unknown, end: unknown): number | null {
  if (!finite(start) || !finite(end)) return null;
  return Math.round(Math.max(0, end - start));
}

function size(value: unknown): number | null {
  return finite(value) && value >= 0 ? Math.round(value) : null;
}

function resourceEntries(url: string): PerformanceResourceTiming[] {
  if (typeof performance === "undefined" || typeof url !== "string") return [];
  let resourceUrl = url;
  try {
    if (typeof location !== "undefined")
      resourceUrl = new URL(url, location.href).href;
    return performance.getEntriesByName(
      resourceUrl,
      "resource",
    ) as PerformanceResourceTiming[];
  } catch {
    // Resource Timing is optional and can be blocked by browser privacy policy.
    return [];
  }
}

/**
 * Claims the Resource Timing entry belonging to one completed daemon fetch.
 *
 * Entries are selected by URL and the fetch's monotonic start/end window. The
 * earliest completed unclaimed entry wins, which preserves completion order
 * for concurrent requests that use the same URL without double-claiming.
 */
export function resourceTimingFields(
  url: string,
  fetchStartedAt: number,
  fetchCompletedAt: number,
): ResourceTimingFields {
  if (!finite(fetchStartedAt) || !finite(fetchCompletedAt))
    return { ...NO_RESOURCE_TIMING };

  const candidates = resourceEntries(url)
    .filter((entry) => {
      if (claimedEntries.has(entry)) return false;
      if (!finite(entry.startTime)) return false;
      if (entry.startTime < fetchStartedAt - TIMING_TOLERANCE_MS) return false;
      if (entry.startTime > fetchCompletedAt + TIMING_TOLERANCE_MS)
        return false;
      return (
        !finite(entry.responseEnd) ||
        entry.responseEnd <= fetchCompletedAt + TIMING_TOLERANCE_MS
      );
    })
    .sort((left, right) => {
      // Start time is the strongest correlation signal for concurrent
      // identical URLs. Completion order can differ when the server responds
      // out of order, so use responseEnd only as a tie breaker.
      const leftDistance = Math.abs(left.startTime - fetchStartedAt);
      const rightDistance = Math.abs(right.startTime - fetchStartedAt);
      if (leftDistance !== rightDistance) return leftDistance - rightDistance;
      const leftEnd = finite(left.responseEnd) ? left.responseEnd : Infinity;
      const rightEnd = finite(right.responseEnd) ? right.responseEnd : Infinity;
      return leftEnd - rightEnd;
    });

  const entry = candidates[0];
  if (entry === undefined) return { ...NO_RESOURCE_TIMING };
  claimedEntries.add(entry);

  return {
    browser_fetch_start_ms: duration(fetchStartedAt, entry.fetchStart),
    browser_request_start_ms: duration(fetchStartedAt, entry.requestStart),
    browser_response_start_ms: duration(fetchStartedAt, entry.responseStart),
    browser_response_end_ms: duration(fetchStartedAt, entry.responseEnd),
    // Standard Resource Timing exposes this pre-request interval, not the
    // narrower DevTools-only "stalled" phase. DNS/connect fields below let an
    // operator identify measured contributors without guessing.
    browser_queue_ms: duration(entry.fetchStart, entry.requestStart),
    browser_request_ms: duration(entry.requestStart, entry.responseStart),
    browser_network_ms: duration(entry.fetchStart, entry.responseEnd),
    browser_transfer_ms: duration(entry.responseStart, entry.responseEnd),
    browser_dns_ms: duration(entry.domainLookupStart, entry.domainLookupEnd),
    browser_connect_ms: duration(entry.connectStart, entry.connectEnd),
    transfer_bytes: size(entry.transferSize),
    encoded_bytes: size(entry.encodedBodySize),
    decoded_bytes: size(entry.decodedBodySize),
    // A zero-length connection phase is evidence of reuse only when the
    // browser exposed a non-zero connection timestamp. Cached/opaque entries
    // commonly report all connection timestamps as zero, so leave those null.
    connection_reused:
      finite(entry.connectStart) &&
      finite(entry.connectEnd) &&
      entry.connectStart > 0
        ? entry.connectEnd === entry.connectStart
        : null,
  };
}
