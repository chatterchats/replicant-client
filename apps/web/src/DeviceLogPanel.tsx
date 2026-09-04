import { useCallback, useEffect, useRef, useState } from "react";

import { daemonApi } from "./api";
import type { DeviceLogsSnapshot } from "./protocol";
import { recordQueryEvent } from "./queryTelemetry";
import { recordWebEvent } from "./telemetry";

type DeviceLogQueryState = {
  data: DeviceLogsSnapshot | undefined;
  status: "loading" | "error" | "empty" | "loaded";
  error: string | null;
  refreshing: boolean;
};

function useDeviceLogs(device: string) {
  const [state, setState] = useState<DeviceLogQueryState>({
    data: undefined,
    status: "loading",
    error: null,
    refreshing: false,
  });
  const requestRef = useRef<{
    controller: AbortController;
  } | null>(null);
  const abortCurrent = useCallback(() => {
    const current = requestRef.current;
    if (current === null) return;
    requestRef.current = null;
    current.controller.abort();
    recordQueryEvent("cancelled_request", { query: "device-logs" });
  }, []);
  const fetchLogs = useCallback(
    (retainData: boolean) => {
      abortCurrent();
      const controller = new AbortController();
      requestRef.current = { controller };
      setState((current) => ({
        data: retainData ? current.data : undefined,
        status:
          retainData && current.data !== undefined ? current.status : "loading",
        error: null,
        refreshing: retainData && current.data !== undefined,
      }));
      const requestStarted = performance.now();
      return daemonApi
        .deviceLogs(device, controller.signal)
        .then((value) => {
          if (
            controller.signal.aborted ||
            requestRef.current?.controller !== controller
          )
            return;
          recordWebEvent(
            "debug",
            "frontend.domain_query",
            "frontend domain projection loaded",
            {
              slice: "device_logs",
              elapsed_ms: Math.round(performance.now() - requestStarted),
              revision: value.metadata.revision,
            },
          );
          setState({
            data: value,
            status: value.events.length === 0 ? "empty" : "loaded",
            error: null,
            refreshing: false,
          });
        })
        .catch((error: unknown) => {
          if (
            controller.signal.aborted ||
            requestRef.current?.controller !== controller
          )
            return;
          recordWebEvent(
            "error",
            "frontend.domain_query_failed",
            "frontend domain projection failed",
            {
              slice: "device_logs",
              elapsed_ms: Math.round(performance.now() - requestStarted),
              error: String(error).slice(0, 500),
            },
          );
          setState((current) => ({
            ...current,
            status: current.data === undefined ? "error" : current.status,
            error: String(error),
            refreshing: false,
          }));
        })
        .finally(() => {
          if (requestRef.current?.controller === controller)
            requestRef.current = null;
        });
    },
    [abortCurrent, device],
  );
  useEffect(() => {
    void fetchLogs(false);
    return abortCurrent;
  }, [abortCurrent, fetchLogs]);
  const refresh = useCallback(() => fetchLogs(true), [fetchLogs]);
  return { ...state, refresh };
}

function DeviceLogDialog({
  device,
  onClose,
}: {
  device: string;
  onClose: () => void;
}) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const { data, status, error, refreshing, refresh } = useDeviceLogs(device);
  useEffect(() => {
    closeRef.current?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [onClose]);
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <section
        aria-labelledby="device-log-title"
        aria-modal="true"
        className="confirm-dialog device-log-dialog"
        onClick={(event) => {
          event.stopPropagation();
        }}
        role="dialog"
      >
        <header className="inspector-section-heading">
          <h2 id="device-log-title">Device log · {device}</h2>
          <div>
            <button disabled={refreshing} onClick={() => void refresh()}>
              Refresh
            </button>
            <button ref={closeRef} onClick={onClose}>
              Close
            </button>
          </div>
        </header>
        {status === "loading" && !data ? (
          <p>Loading logs…</p>
        ) : error && !data ? (
          <p className="inline-warning">{error}</p>
        ) : !data?.events.length ? (
          <p>No device log entries.</p>
        ) : (
          <div className="activity-list compact">
            {[...data.events]
              .sort((left, right) => {
                const leftTime = left.created_at
                  ? Date.parse(left.created_at)
                  : 0;
                const rightTime = right.created_at
                  ? Date.parse(right.created_at)
                  : 0;
                return rightTime - leftTime;
              })
              .slice(0, 20)
              .map((event, index) => (
                <article className="activity-item" key={event.id ?? index}>
                  {event.created_at ? (
                    <small>{new Date(event.created_at).toLocaleString()}</small>
                  ) : null}
                  <strong>{event.event_type ?? "device event"}</strong>
                  <p>{event.message ?? JSON.stringify(event.payload)}</p>
                </article>
              ))}
          </div>
        )}
      </section>
    </div>
  );
}

export function DeviceLogButton({ device }: { device: string }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        onClick={() => {
          setOpen(true);
        }}
      >
        Device log
      </button>
      {open ? (
        <DeviceLogDialog
          device={device}
          onClose={() => {
            setOpen(false);
          }}
        />
      ) : null}
    </>
  );
}
