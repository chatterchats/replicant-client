/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { DescriptorCatalog, EntityRef, RelaySnapshot } from "./protocol";

const empty = (snapshot: RelaySnapshot) =>
  snapshot.relays.length === 0 &&
  snapshot.staged_relays.length === 0 &&
  snapshot.expansions.length === 0;

export const relayCommands = (descriptors: DescriptorCatalog) =>
  applicableDescriptorCommands(descriptors, "system").filter((command) =>
    /relay/i.test(
      `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
    ),
  );

export function RelayPage(props: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenGalaxy: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "missions",
    fetcher: (signal) => daemonApi.relay(signal),
    isEmpty: empty,
  });
  return <RelayContent {...query} {...props} />;
}

export function RelayContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  descriptors,
  onSelectEntity,
  onOpenGalaxy,
  onSelectWorkflow,
  onRunCommand,
}: {
  data?: RelaySnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenGalaxy: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [search, setSearch] = useState("");
  const operations = useMemo(() => relayCommands(descriptors), [descriptors]);
  const staged = new Set(
    data?.staged_relays.map((device) => device.entity.id) ?? [],
  );
  const rows = [...(data?.relays ?? []), ...(data?.staged_relays ?? [])].filter(
    (device) =>
      !search ||
      [
        device.entity.id,
        device.device_type,
        device.system,
        device.location,
        device.owner,
        device.owner_name,
        ...device.tags,
      ].some((value) => value?.toLowerCase().includes(search.toLowerCase())),
  );
  const edgeSystems = new Set(
    data?.relay_edges.flatMap((edge) => [edge.from, edge.to]) ?? [],
  );
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Relay…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Relay unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Relay</h1>
          <p className="lede">
            Deployed network coverage and durable expansion progress.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="asset-operations" aria-label="Relay operations">
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
          <p>No relay expansion workflow is registered.</p>
        )}
      </section>
      <section className="inventory-summary" aria-label="Relay coverage">
        <div>
          <strong>{data?.relays.length ?? 0}</strong>
          <span>deployed relays</span>
        </div>
        <div>
          <strong>{data?.connected_systems ?? 0}</strong>
          <span>covered systems</span>
        </div>
        <div>
          <strong>{data?.relay_edges.length ?? 0}</strong>
          <span>network edges</span>
        </div>
        <div>
          <strong>
            {Math.max(0, (data?.connected_systems ?? 0) - edgeSystems.size)}
          </strong>
          <span>isolated systems</span>
        </div>
      </section>
      {!!data?.expansions.length && (
        <section>
          <h2>Active expansion</h2>
          <div className="inventory-table-wrap">
            <table className="inventory-table">
              <thead>
                <tr>
                  <th>Workflow</th>
                  <th>Route</th>
                  <th>Progress</th>
                  <th>Relays</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {data.expansions.map((expansion) => (
                  <tr key={expansion.workflow.id}>
                    <td>
                      {expansion.workflow.kind}
                      <small className="status-chip">{expansion.phase}</small>
                    </td>
                    <td>
                      {expansion.hub} → {expansion.targets.join(", ")}
                      <small>replicant {expansion.replicant}</small>
                    </td>
                    <td>
                      {expansion.completed_stops} /{" "}
                      {expansion.total_stops ?? "—"}
                      <small>{expansion.next_system ?? "Return to hub"}</small>
                    </td>
                    <td>{expansion.pending_relays ?? "—"} pending</td>
                    <td>
                      <button
                        onClick={() => {
                          onSelectWorkflow(expansion.workflow.id);
                        }}
                      >
                        Open workflow
                      </button>
                      <button
                        onClick={() => {
                          onOpenGalaxy(
                            expansion.next_system ??
                              expansion.targets[0] ??
                              expansion.hub,
                          );
                        }}
                      >
                        Show on Galaxy
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
      <label className="inventory-search">
        Search relays
        <input
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value);
          }}
          placeholder="Code, system, owner, or tag"
        />
      </label>
      {!data?.relays.length && !data?.staged_relays.length ? (
        <section className="empty-state">
          No owned relay devices discovered.
        </section>
      ) : !rows.length ? (
        <section className="empty-state">No relays match the search.</section>
      ) : (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Relay</th>
                <th>State</th>
                <th>System / location</th>
                <th>Owner</th>
                <th>Tags</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((device) => (
                <tr key={device.entity.id}>
                  <td>
                    <button
                      className="link-button"
                      onClick={() => {
                        onSelectEntity(device.entity);
                      }}
                    >
                      {device.entity.id}
                    </button>
                    <small>{device.device_type ?? "relay"}</small>
                  </td>
                  <td>
                    <span className="status-chip">
                      {staged.has(device.entity.id)
                        ? "staged"
                        : (device.status ?? "deployed")}
                    </span>
                  </td>
                  <td>
                    {device.system ? (
                      <button
                        className="link-button"
                        onClick={() => {
                          if (device.system) onOpenGalaxy(device.system);
                        }}
                      >
                        {device.system}
                      </button>
                    ) : (
                      "Unknown system"
                    )}
                    <small>{device.location ?? "Unknown location"}</small>
                  </td>
                  <td>{device.owner_name ?? device.owner ?? "Account"}</td>
                  <td>{device.tags.join(", ") || "—"}</td>
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
