import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";

export function DeviceLogPanel({ device }: { device: string }) {
  const { data, status, error, refreshing, refresh } = useDomainQuery({
    slice: "activity",
    fetcher: (signal) => daemonApi.deviceLogs(device, signal),
    isEmpty: (snapshot) => snapshot.events.length === 0,
  });
  return (
    <section className="inspector-section" aria-label="Device log">
      <header className="inspector-section-heading">
        <strong>Device log</strong>
        <button disabled={refreshing} onClick={() => void refresh()}>
          Refresh
        </button>
      </header>
      {status === "loading" && !data ? (
        <p>Loading logs…</p>
      ) : error && !data ? (
        <p className="inline-warning">{error}</p>
      ) : !data?.events.length ? (
        <p>No device log entries.</p>
      ) : (
        <div className="activity-list compact">
          {data.events.slice(0, 20).map((event, index) => (
            <article className="activity-item" key={event.id ?? index}>
              <small>
                {event.created_at
                  ? new Date(event.created_at).toLocaleString()
                  : "Unknown time"}
              </small>
              <strong>{event.event_type ?? "device event"}</strong>
              <p>{event.message ?? JSON.stringify(event.payload)}</p>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
