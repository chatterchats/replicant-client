/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  DescriptorCatalog,
  EventSummary,
  EventsSnapshot,
} from "./protocol";

const empty = (snapshot: EventsSnapshot) => snapshot.events.length === 0;

export const eventCommands = (descriptors: DescriptorCatalog) =>
  descriptorCommands(descriptors).filter((command) =>
    /event|fulfill/i.test(
      `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
    ),
  );

export function EventsPage(props: {
  descriptors: DescriptorCatalog;
  onSelectEvent: (event: EventSummary) => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "events",
    fetcher: (signal) => daemonApi.events(signal),
    isEmpty: empty,
  });
  return <EventsContent {...query} {...props} />;
}

export function EventsContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  descriptors,
  onSelectEvent,
  onOpenGalaxy,
  onOpenSystem,
  onRunCommand,
}: {
  data?: EventsSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  descriptors: DescriptorCatalog;
  onSelectEvent: (event: EventSummary) => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [search, setSearch] = useState("");
  const operations = useMemo(() => eventCommands(descriptors), [descriptors]);
  const events = (data?.events ?? []).filter((event) =>
    [
      event.designation,
      event.title,
      event.event_type,
      event.category,
      event.status,
      event.system,
      event.location,
    ].some((value) => value?.toLowerCase().includes(search.toLowerCase())),
  );
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Events…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Events unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Events</h1>
          <p className="lede">
            Discovered location events, confirmed progress, and rewards.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="asset-operations" aria-label="Event operations">
        <h2>Operations</h2>
        {operations.length ? (
          operations.map((command) => (
            <button
              key={`${command.operationClass}:${command.descriptor.kind}`}
              onClick={() => {
                onRunCommand(command);
              }}
            >
              {command.descriptor.display_name}
            </button>
          ))
        ) : (
          <p>No event operation is registered.</p>
        )}
      </section>
      <label className="inventory-search">
        Search events
        <input
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value);
          }}
          placeholder="Title, designation, type, or location"
        />
      </label>
      {!data?.events.length ? (
        <section className="empty-state">No discovered events.</section>
      ) : !events.length ? (
        <section className="empty-state">No events match the search.</section>
      ) : (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Event</th>
                <th>Status</th>
                <th>Location</th>
                <th>Criteria / progress</th>
                <th>Rewards</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <tr key={event.designation}>
                  <td>
                    <strong>{event.title}</strong>
                    <small>{event.designation}</small>
                    {event.description && <small>{event.description}</small>}
                  </td>
                  <td>
                    <span className="status-chip">
                      {event.status ?? "unknown"}
                    </span>
                    <small>
                      {[event.event_type, event.category, event.tier]
                        .filter((value) => value !== null)
                        .join(" · ") || "Unclassified"}
                    </small>
                  </td>
                  <td>
                    <strong>{event.system}</strong>
                    <small>{event.location}</small>
                  </td>
                  <td>
                    {event.criteria.map((criterion) => (
                      <details key={criterion.name}>
                        <summary>
                          {criterion.name} ·{" "}
                          {criterion.complete ? "complete" : "active"}
                        </summary>
                        {criterion.requirements.map((requirement) => (
                          <small
                            key={`${requirement.kind}:${requirement.item}`}
                          >
                            {requirement.item}: {requirement.completed}/
                            {requirement.required} ({requirement.remaining}{" "}
                            remaining)
                          </small>
                        ))}
                      </details>
                    ))}
                  </td>
                  <td>
                    {[...event.rewards.resources, ...event.rewards.devices].map(
                      (reward) => (
                        <small key={reward.item}>
                          {reward.quantity} {reward.item}
                        </small>
                      ),
                    )}
                    {event.rewards.xp !== null && (
                      <small>{event.rewards.xp} XP</small>
                    )}
                    {event.rewards.civilisation_points !== null && (
                      <small>
                        {event.rewards.civilisation_points} civilisation points
                      </small>
                    )}
                    {event.rewards.completion_achievement && (
                      <small>{event.rewards.completion_achievement}</small>
                    )}
                  </td>
                  <td>
                    <button
                      onClick={() => {
                        onSelectEvent(event);
                      }}
                    >
                      Inspect
                    </button>
                    <button
                      onClick={() => {
                        onOpenGalaxy(event.system);
                      }}
                    >
                      Galaxy
                    </button>
                    <button
                      onClick={() => {
                        onOpenSystem(event.system);
                      }}
                    >
                      System
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <p className="table-summary">Revision {data?.metadata.revision ?? "—"}</p>
    </article>
  );
}
