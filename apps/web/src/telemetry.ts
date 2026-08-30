export type WebTelemetryLevel = "debug" | "info" | "warn" | "error";
export type WebTelemetryField = string | number | boolean | null;
export type WebTelemetryFields = Record<string, WebTelemetryField>;

interface WebTelemetryEvent {
  session_id: string;
  observed_at_ms: number;
  level: WebTelemetryLevel;
  event: string;
  message: string;
  page: string | null;
  fields: WebTelemetryFields;
}

const MAX_QUEUE = 500;
const MAX_BATCH = 50;
const FLUSH_DELAY_MS = 2_000;
const RETRY_DELAY_MS = 5_000;
const TELEMETRY_TIMEOUT_MS = 5_000;

const sessionId =
  typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `web-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

let endpoint: string | undefined;
let token: string | undefined;
let queue: WebTelemetryEvent[] = [];
let timer: ReturnType<typeof setTimeout> | undefined;
let flushing = false;
let browserTelemetryInstalled = false;

function currentPage(): string | null {
  if (typeof window === "undefined") return null;
  return window.location.hash || window.location.pathname || null;
}

function scheduleFlush(delayMs = FLUSH_DELAY_MS) {
  if (endpoint === undefined || timer !== undefined || flushing) return;
  timer = setTimeout(() => {
    timer = undefined;
    void flushWebTelemetry();
  }, delayMs);
}

export function configureWebTelemetryTransport(
  url: string,
  authToken?: string,
) {
  endpoint = url;
  token = authToken;
  if (queue.length > 0) scheduleFlush(0);
}

export function recordWebEvent(
  level: WebTelemetryLevel,
  event: string,
  message: string,
  fields: WebTelemetryFields = {},
) {
  queue.push({
    session_id: sessionId,
    observed_at_ms: Date.now(),
    level,
    event,
    message,
    page: currentPage(),
    fields,
  });
  if (queue.length > MAX_QUEUE) queue = queue.slice(-MAX_QUEUE);
  scheduleFlush(queue.length >= MAX_BATCH ? 0 : FLUSH_DELAY_MS);
}

export async function flushWebTelemetry(): Promise<void> {
  if (endpoint === undefined || flushing || queue.length === 0) return;
  flushing = true;
  let retry = false;
  const batch = queue.splice(0, MAX_BATCH);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TELEMETRY_TIMEOUT_MS);
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (token !== undefined) headers.Authorization = `Bearer ${token}`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify({ events: batch }),
      keepalive: true,
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`telemetry returned ${response.status}`);
  } catch {
    // Diagnostics must never make the UI less reliable. Retain only the newest
    // bounded window and retry later when the daemon/proxy is reachable again.
    queue = [...batch, ...queue].slice(-MAX_QUEUE);
    retry = true;
  } finally {
    clearTimeout(timeout);
    flushing = false;
    if (queue.length > 0 && timer === undefined)
      scheduleFlush(retry ? RETRY_DELAY_MS : FLUSH_DELAY_MS);
  }
}

function navigationTimingFields(): WebTelemetryFields {
  if (typeof performance === "undefined") return {};
  const entry = performance.getEntriesByType("navigation")[0];
  if (
    typeof PerformanceNavigationTiming === "undefined" ||
    !(entry instanceof PerformanceNavigationTiming)
  )
    return {};
  return {
    dns_ms: Math.round(entry.domainLookupEnd - entry.domainLookupStart),
    connect_ms: Math.round(entry.connectEnd - entry.connectStart),
    response_ms: Math.round(entry.responseEnd - entry.responseStart),
    dom_content_loaded_ms: Math.round(entry.domContentLoadedEventEnd),
    load_ms: Math.round(entry.loadEventEnd || performance.now()),
    transfer_bytes: entry.transferSize,
    decoded_bytes: entry.decodedBodySize,
  };
}

export function installBrowserTelemetry() {
  if (browserTelemetryInstalled || typeof window === "undefined") return;
  browserTelemetryInstalled = true;

  recordWebEvent("info", "frontend.session_started", "web session started", {
    user_agent: navigator.userAgent.slice(0, 240),
  });

  const recordInitialLoad = () => {
    recordWebEvent(
      "info",
      "frontend.page_loaded",
      "initial document load completed",
      navigationTimingFields(),
    );
  };
  if (document.readyState === "complete") queueMicrotask(recordInitialLoad);
  else window.addEventListener("load", recordInitialLoad, { once: true });

  window.addEventListener("error", (event) => {
    recordWebEvent("error", "frontend.window_error", "uncaught browser error", {
      message: event.message.slice(0, 500),
      filename: event.filename.slice(0, 300),
      line: event.lineno,
      column: event.colno,
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    const rawReason = event.reason as unknown;
    const reason =
      rawReason instanceof Error ? rawReason.message : String(rawReason);
    recordWebEvent(
      "error",
      "frontend.unhandled_rejection",
      "unhandled browser promise rejection",
      { reason: reason.slice(0, 500) },
    );
  });

  if (
    typeof PerformanceObserver !== "undefined" &&
    PerformanceObserver.supportedEntryTypes.includes("longtask")
  ) {
    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (entry.duration < 100) continue;
        recordWebEvent(
          "warn",
          "frontend.long_task",
          "browser main thread was blocked",
          { duration_ms: Math.round(entry.duration) },
        );
      }
    });
    observer.observe({ entryTypes: ["longtask"] });
  }

  const flushIfHidden = () => {
    if (document.visibilityState === "hidden") void flushWebTelemetry();
  };
  document.addEventListener("visibilitychange", flushIfHidden);
  window.addEventListener("pagehide", () => {
    void flushWebTelemetry();
  });
}
