import { useMemo, useState } from "react";

import { ActivityEventDetails } from "./ActivityEventDetails";
import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";
import type { EntityRef } from "./protocol";

export function ActivityPage({
  onSelectEntity,
}: {
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [device, setDevice] = useState("");
  const [name, setName] = useState("");
  const [amiOnly, setAmiOnly] = useState(false);
  const query = useDomainQuery({
    slice: "activity",
    fetcher: (signal) =>
      daemonApi.activity({ device, name, amiOnly, limit: 300 }, signal),
    isEmpty: (snapshot) => snapshot.events.length === 0,
  });
  const { data, status, error, refreshing, refresh } = query;
  const eventTypes = useMemo(() => {
    const values = new Set(data?.events.map((event) => event.name) ?? []);
    if (name) values.add(name);
    return [...values].sort((left, right) => left.localeCompare(right));
  }, [data?.events, name]);
  if (!data && status === "loading")
    return (
      <article className="page loading-state">
        Loading account activity…
      </article>
    );
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Activity unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Activity</h1>
          <p className="lede">
            Durable account events and AMI fleet digests, separate from galaxy
            location events.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="inventory-controls" aria-label="Activity filters">
        <label>
          Device
          <input
            value={device}
            onChange={(event) => {
              setDevice(event.target.value);
            }}
            placeholder="Device code"
          />
        </label>
        <label>
          Event type
          <select
            value={name}
            onChange={(event) => {
              setName(event.target.value);
            }}
          >
            <option value="">All event types</option>
            {eventTypes.map((eventType) => (
              <option key={eventType} value={eventType}>
                {eventType}
              </option>
            ))}
          </select>
        </label>
        <label>
          <input
            type="checkbox"
            checked={amiOnly}
            onChange={(event) => {
              setAmiOnly(event.target.checked);
            }}
          />{" "}
          AMI digests only
        </label>
        <button onClick={() => void refresh()}>Apply</button>
      </section>
      {!data?.events.length ? (
        <section className="empty-state">No matching account events.</section>
      ) : (
        <div className="inventory-table-wrap activity-table-wrap">
          <table className="inventory-table activity-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Event</th>
                <th>Subject</th>
                <th>Location</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {data.events.map((event) => {
                const subject = event.device ?? event.replicant;
                return (
                  <tr key={event.id}>
                    <td>{new Date(event.occurred_at).toLocaleString()}</td>
                    <td>
                      <strong>{event.name}</strong>
                      {event.ami_digest && <small>AMI digest</small>}
                    </td>
                    <td>
                      {subject ? (
                        <button
                          onClick={() => {
                            onSelectEntity(subject);
                          }}
                        >
                          {subject.id}
                        </button>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td>{event.location ?? event.system ?? "—"}</td>
                    <td className="activity-details-cell">
                      <details>
                        <summary>
                          {event.ami_digest ? "View report" : "View payload"}
                        </summary>
                        <ActivityEventDetails event={event} />
                      </details>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <p className="table-summary">
        Cursor {data?.cursor ?? "—"} · revision {data?.metadata.revision ?? "—"}
      </p>
    </article>
  );
}
