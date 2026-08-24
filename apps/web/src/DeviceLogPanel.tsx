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
  );
}
