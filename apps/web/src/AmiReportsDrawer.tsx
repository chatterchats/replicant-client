import { useMemo, useState } from "react";

import { ActivityEventDetails } from "./ActivityEventDetails";
import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";
import type { EntityRef } from "./protocol";

export function AmiReportsDrawer({
  onSelectEntity,
}: {
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [eventType, setEventType] = useState("");
  const query = useDomainQuery({
    slice: "activity",
    fetcher: (signal) =>
      daemonApi.activity({ amiOnly: true, limit: 50 }, signal),
    isEmpty: (snapshot) => snapshot.events.length === 0,
  });
  const eventTypes = useMemo(
    () =>
      [...new Set((query.data?.events ?? []).map((event) => event.name))].sort(
        (left, right) => left.localeCompare(right),
      ),
    [query.data?.events],
  );
  const events = (query.data?.events ?? []).filter(
    (event) => !eventType || event.name === eventType,
  );

  return (
    <div className="ami-report-drawer">
      <div className="ami-report-toolbar">
        <label>
          Report type
          <select
            value={eventType}
            onChange={(event) => setEventType(event.target.value)}
          >
            <option value="">All AMI reports</option>
            {eventTypes.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <button
          disabled={query.refreshing}
          onClick={() => void query.refresh()}
        >
          Refresh
        </button>
      </div>
      {query.error && <p className="inline-warning">{query.error}</p>}
      {!query.data && query.status === "loading" ? (
        <p className="empty-state">Loading AMI reports…</p>
      ) : !events.length ? (
        <p className="empty-state">No AMI reports match this view.</p>
      ) : (
        <div className="ami-report-list">
          {events.map((event) => (
            <details className="ami-report-card" key={event.id}>
              <summary>
                <span>
                  <strong>{event.name}</strong>
                  <small>
                    {event.device?.id ??
                      event.location ??
                      event.system ??
                      "fleet"}
                  </small>
                </span>
                <time dateTime={event.occurred_at}>
                  {new Date(event.occurred_at).toLocaleString()}
                </time>
              </summary>
              <div className="ami-report-card-body">
                {event.device && (
                  <button
                    className="subtle-link"
                    onClick={() => onSelectEntity(event.device!)}
                  >
                    Inspect {event.device.id}
                  </button>
                )}
                <ActivityEventDetails event={event} compact />
              </div>
            </details>
          ))}
        </div>
      )}
    </div>
  );
}
