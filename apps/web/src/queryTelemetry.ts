import { recordWebEvent } from "./telemetry";

export type QueryTelemetryKind =
  | "request"
  | "request_success"
  | "auto_refetch"
  | "joined_request"
  | "cache_hit"
  | "coalesced_invalidation"
  | "cancelled_request"
  | "stale_discarded";

export interface QueryTelemetryFields {
  /**
   * A stable query/endpoint name used only for local counter classification.
   * Query names and response bodies are never included in telemetry payloads.
   */
  query?: string;
  bytes_received?: number;
}

export interface QueryTelemetrySummary {
  requests_started: number;
  automatic_refetches: number;
  requests_joined: number;
  requests_cancelled: number;
  bytes_received: number;
  entities_fetches: number;
  galaxy_scene_fetches: number;
  devices_fetches: number;
  cache_hits: number;
  coalesced_invalidations: number;
  stale_discarded: number;
}

const SUMMARY_INTERVAL_MS = 60_000;
const MAX_COUNTER = 1_000_000;
const MAX_BYTES = 1_000_000_000_000;

const summary: QueryTelemetrySummary = {
  requests_started: 0,
  automatic_refetches: 0,
  requests_joined: 0,
  requests_cancelled: 0,
  bytes_received: 0,
  entities_fetches: 0,
  galaxy_scene_fetches: 0,
  devices_fetches: 0,
  cache_hits: 0,
  coalesced_invalidations: 0,
  stale_discarded: 0,
};

let queryTelemetryInstalled = false;
let summaryTimer: ReturnType<typeof setInterval> | undefined;
let pagehideHandler: (() => void) | undefined;
let pagehideTarget: Window | undefined;

const lifecycleByKind: Partial<
  Record<QueryTelemetryKind, { event: string; message: string }>
> = {
  joined_request: {
    event: "frontend.query_joined",
    message: "frontend query joined an active request",
  },
  cache_hit: {
    event: "frontend.query_cache_hit",
    message: "frontend query served from cache",
  },
  coalesced_invalidation: {
    event: "frontend.query_coalesced",
    message: "frontend query invalidation coalesced",
  },
  cancelled_request: {
    event: "frontend.query_cancelled",
    message: "frontend query request cancelled",
  },
  stale_discarded: {
    event: "frontend.query_stale_discarded",
    message: "frontend query response discarded as stale",
  },
};

function emitLifecycleEvent(kind: QueryTelemetryKind): void {
  const lifecycle = lifecycleByKind[kind];
  if (lifecycle === undefined) return;
  recordWebEvent("debug", lifecycle.event, lifecycle.message);
}

type CountedFetch =
  "entities_fetches" | "galaxy_scene_fetches" | "devices_fetches";

function fetchCounter(query: string | undefined): CountedFetch | undefined {
  if (query === undefined) return undefined;
  const name = query.trim();
  // The API supplies canonical routes while the cache uses short stable keys.
  // Accept both forms without retaining the key or route in the event.
  const route =
    (name.startsWith("/api/") ? name.slice("/api/".length) : name)
      .split(/[?#]/, 1)[0]
      ?.replace(/\/+$/, "") ?? "";
  switch (route) {
    case "entities":
      return "entities_fetches";
    case "galaxy-scene":
    case "galaxy_scene":
    case "galaxyScene":
      return "galaxy_scene_fetches";
    case "devices":
      return "devices_fetches";
    default:
      return undefined;
  }
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
  name: Exclude<keyof QueryTelemetrySummary, "bytes_received">,
) {
  summary[name] = Math.min(summary[name] + 1, MAX_COUNTER);
}

/** Record one query lifecycle transition and aggregate it for the session. */
export function recordQueryEvent(
  kind: QueryTelemetryKind,
  fields: QueryTelemetryFields = {},
): void {
  switch (kind) {
    case "request":
      incrementCounter("requests_started");
      {
        const counter = fetchCounter(fields.query);
        if (counter !== undefined) incrementCounter(counter);
      }
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
    case "auto_refetch":
      incrementCounter("automatic_refetches");
      break;
    case "joined_request":
      incrementCounter("requests_joined");
      break;
    case "cache_hit":
      incrementCounter("cache_hits");
      break;
    case "coalesced_invalidation":
      incrementCounter("coalesced_invalidations");
      break;
    case "cancelled_request":
      incrementCounter("requests_cancelled");
      break;
    case "stale_discarded":
      incrementCounter("stale_discarded");
      break;
  }
  emitLifecycleEvent(kind);
}

export function queryTelemetrySummary(): QueryTelemetrySummary {
  return { ...summary };
}

/** Emit the cumulative, session-bounded query counters at INFO level. */
export function flushQuerySummary(): void {
  recordWebEvent("info", "frontend.query_summary", "frontend query summary", {
    requests_started: summary.requests_started,
    requests_joined: summary.requests_joined,
    automatic_refetches: summary.automatic_refetches,
    requests_cancelled: summary.requests_cancelled,
    bytes_received: summary.bytes_received,
    entities_fetches: summary.entities_fetches,
    galaxy_scene_fetches: summary.galaxy_scene_fetches,
    devices_fetches: summary.devices_fetches,
    cache_hits: summary.cache_hits,
    coalesced_invalidations: summary.coalesced_invalidations,
    stale_discarded: summary.stale_discarded,
  });
}

/** Install low-frequency summary delivery for the browser session. */
export function installQueryTelemetry(): void {
  if (queryTelemetryInstalled || typeof window === "undefined") return;
  queryTelemetryInstalled = true;
  summaryTimer = window.setInterval(flushQuerySummary, SUMMARY_INTERVAL_MS);
  pagehideHandler = () => {
    flushQuerySummary();
  };
  pagehideTarget = window;
  pagehideTarget.addEventListener("pagehide", pagehideHandler);
}

/** Test-only lifecycle reset; it does not reset cumulative session counters. */
export function uninstallQueryTelemetryForTests(): void {
  if (summaryTimer !== undefined) clearInterval(summaryTimer);
  if (pagehideHandler !== undefined && pagehideTarget !== undefined)
    pagehideTarget.removeEventListener("pagehide", pagehideHandler);
  summaryTimer = undefined;
  pagehideHandler = undefined;
  pagehideTarget = undefined;
  queryTelemetryInstalled = false;
}

/** Reset counters and browser hooks for isolated telemetry tests. */
export function resetQueryTelemetryForTests(): void {
  uninstallQueryTelemetryForTests();
  summary.requests_started = 0;
  summary.automatic_refetches = 0;
  summary.requests_joined = 0;
  summary.requests_cancelled = 0;
  summary.bytes_received = 0;
  summary.entities_fetches = 0;
  summary.galaxy_scene_fetches = 0;
  summary.devices_fetches = 0;
  summary.cache_hits = 0;
  summary.coalesced_invalidations = 0;
  summary.stale_discarded = 0;
}
