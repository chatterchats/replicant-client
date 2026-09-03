import { useEffect, useRef, useState } from "react";

import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";

function DeviceLogDialog({
  device,
  onClose,
}: {
  device: string;
  onClose: () => void;
}) {
  const closeRef = useRef<HTMLButtonElement>(null);
  const { data, status, error, refreshing, refresh } = useDomainQuery({
    slice: "activity",
    fetcher: (signal) => daemonApi.deviceLogs(device, signal),
    isEmpty: (snapshot) => snapshot.events.length === 0,
  });
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
