import { recordWebEvent } from "./telemetry";

export type QueryTelemetryKind =
  | "request"
  | "request_success"
  | "joined_request"
  | "cache_hit"
  | "coalesced_invalidation"
  | "cancelled_request";

export interface QueryTelemetryFields {
  /** A stable query/endpoint name, never a URL query or request payload. */
  query?: string;
  bytes_received?: number;
  elapsed_ms?: number;
}

export interface QueryTelemetrySummary {
  requests: number;
  joined_requests: number;
  cache_hits: number;
  coalesced_invalidations: number;
  cancelled_requests: number;
  bytes_received: number;
}

const SUMMARY_INTERVAL_MS = 60_000;
const MAX_COUNTER = 1_000_000;
const MAX_BYTES = 1_000_000_000_000;

const summary: QueryTelemetrySummary = {
  requests: 0,
  joined_requests: 0,
  cache_hits: 0,
  coalesced_invalidations: 0,
  cancelled_requests: 0,
  bytes_received: 0,
};

let queryTelemetryInstalled = false;
let summaryTimer: ReturnType<typeof setInterval> | undefined;

function boundedName(value: string | undefined): string | undefined {
  if (!value) return undefined;
  // Keep diagnostics useful while preventing accidental query strings, IDs, or
  // other payload-like values from becoming telemetry fields.
  const name = value.trim().replace(/[^A-Za-z0-9._:/,-]+/g, "_");
  return name.length > 80 ? name.slice(0, 80) : name || undefined;
}

function boundedNumber(
  value: number | undefined,
  maximum: number,
): number | undefined {
  if (value === undefined || !Number.isFinite(value) || value < 0)
    return undefined;
  return Math.min(Math.round(value), maximum);
}

function incrementCounter(
  name: keyof Omit<QueryTelemetrySummary, "bytes_received">,
) {
  summary[name] = Math.min(summary[name] + 1, MAX_COUNTER);
}

function emitDebug(kind: QueryTelemetryKind, fields: QueryTelemetryFields) {
  const query = boundedName(fields.query);
  const bytes = boundedNumber(fields.bytes_received, MAX_BYTES);
  const elapsed = boundedNumber(fields.elapsed_ms, MAX_COUNTER);
  const safeFields = {
    ...(query === undefined ? {} : { query }),
    ...(bytes === undefined ? {} : { bytes_received: bytes }),
    ...(elapsed === undefined ? {} : { elapsed_ms: elapsed }),
  };
  const eventByKind: Record<QueryTelemetryKind, string> = {
    request: "frontend.query_request",
    request_success: "frontend.query_request_success",
    joined_request: "frontend.query_joined",
    cache_hit: "frontend.query_cache_hit",
    coalesced_invalidation: "frontend.query_invalidation_coalesced",
    cancelled_request: "frontend.query_cancelled",
  };
  const messageByKind: Record<QueryTelemetryKind, string> = {
    request: "frontend query request started",
    request_success: "frontend query request completed",
    joined_request: "frontend query joined an active request",
    cache_hit: "frontend query served from cache",
    coalesced_invalidation: "frontend query invalidation coalesced",
    cancelled_request: "frontend query request cancelled",
  };
  recordWebEvent("debug", eventByKind[kind], messageByKind[kind], safeFields);
}

/** Record one query lifecycle diagnostic without retaining request data. */
export function recordQueryEvent(
  kind: QueryTelemetryKind,
  fields: QueryTelemetryFields = {},
): void {
  switch (kind) {
    case "request":
      incrementCounter("requests");
      break;
    case "request_success": {
      const bytes = boundedNumber(fields.bytes_received, MAX_BYTES);
      if (bytes !== undefined)
        summary.bytes_received = Math.min(
          summary.bytes_received + bytes,
          MAX_BYTES,
        );
      break;
    }
    case "joined_request":
      incrementCounter("joined_requests");
      break;
    case "cache_hit":
      incrementCounter("cache_hits");
      break;
    case "coalesced_invalidation":
      incrementCounter("coalesced_invalidations");
      break;
    case "cancelled_request":
      incrementCounter("cancelled_requests");
      break;
  }
  emitDebug(kind, fields);
}

export function recordQueryRequest(query?: string): void {
  recordQueryEvent("request", { query });
}

export function recordQuerySuccess(
  query?: string,
  bytesReceived?: number,
  elapsedMs?: number,
): void {
  recordQueryEvent("request_success", {
    query,
    bytes_received: bytesReceived,
    elapsed_ms: elapsedMs,
  });
}

export function recordQueryJoined(query?: string): void {
  recordQueryEvent("joined_request", { query });
}

export function recordQueryCacheHit(query?: string): void {
  recordQueryEvent("cache_hit", { query });
}

export function recordQueryInvalidationCoalesced(query?: string): void {
  recordQueryEvent("coalesced_invalidation", { query });
}

export function recordQueryCancellation(query?: string): void {
  recordQueryEvent("cancelled_request", { query });
}

export function queryTelemetrySummary(): QueryTelemetrySummary {
  return { ...summary };
}

/** Emit the cumulative, session-bounded query counters at INFO level. */
export function flushQuerySummary(): void {
  recordWebEvent("info", "frontend.query_summary", "frontend query summary", {
    requests: summary.requests,
    joined_requests: summary.joined_requests,
    cache_hits: summary.cache_hits,
    coalesced_invalidations: summary.coalesced_invalidations,
    cancelled_requests: summary.cancelled_requests,
    bytes_received: summary.bytes_received,
  });
}

/** Install low-frequency summary delivery for the browser session. */
export function installQueryTelemetry(): void {
  if (queryTelemetryInstalled || typeof window === "undefined") return;
  queryTelemetryInstalled = true;
  summaryTimer = window.setInterval(flushQuerySummary, SUMMARY_INTERVAL_MS);
  window.addEventListener("pagehide", () => {
    flushQuerySummary();
  });
}

/** Test-only lifecycle reset; does not reset session counters. */
export function uninstallQueryTelemetryForTests(): void {
  if (summaryTimer !== undefined) clearInterval(summaryTimer);
  summaryTimer = undefined;
  queryTelemetryInstalled = false;
}
