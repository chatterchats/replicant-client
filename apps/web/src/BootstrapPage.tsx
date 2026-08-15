/* eslint-disable react-refresh/only-export-components */
import { useMemo } from "react";

import { daemonApi } from "./api";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { BootstrapSnapshot, DescriptorCatalog } from "./protocol";

const empty = (snapshot: BootstrapSnapshot) => snapshot.missions.length === 0;

export const bootstrapCommands = (descriptors: DescriptorCatalog) =>
  applicableDescriptorCommands(descriptors, "system").filter((command) =>
    /bootstrap/i.test(
      `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
    ),
  );

export function BootstrapPage(props: {
  descriptors: DescriptorCatalog;
  onOpenGalaxy: (system: string) => void;
  onOpenHistory: () => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "missions",
    fetcher: (signal) => daemonApi.bootstrap(signal),
    isEmpty: empty,
  });
  return <BootstrapContent {...query} {...props} />;
}

export function BootstrapContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  descriptors,
  onOpenGalaxy,
  onOpenHistory,
  onRunCommand,
}: {
  data?: BootstrapSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  descriptors: DescriptorCatalog;
  onOpenGalaxy: (system: string) => void;
  onOpenHistory: () => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const operations = useMemo(
    () => bootstrapCommands(descriptors),
    [descriptors],
  );
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Bootstrap…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Bootstrap unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Bootstrap</h1>
          <p className="lede">
            Regional ark staging, delivery, and deployment checkpoints.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="asset-operations" aria-label="Bootstrap operations">
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
          <p>No Bootstrap action is registered.</p>
        )}
        <p>
          These actions resume an existing mission file. Create new regional
          plans with the Bootstrap CLI.
        </p>
      </section>
      {!data?.missions.length ? (
        <section className="empty-state">
          No Bootstrap action results have been recorded yet.
        </section>
      ) : (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Mission</th>
                <th>Target</th>
                <th>Phase</th>
                <th>Ark progress</th>
                <th>Regional progress</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {data.missions.map((mission) => (
                <tr key={mission.mission_id}>
                  <td>
                    <strong>{mission.mission_id}</strong>
                    <small>
                      {mission.region} · source {mission.source_hub}
                    </small>
                  </td>
                  <td>
                    <button
                      className="link-button"
                      onClick={() => {
                        onOpenGalaxy(mission.target_system);
                      }}
                    >
                      {mission.target_system}
                    </button>
                    <small>{mission.target_location}</small>
                  </td>
                  <td>
                    <span className="status-chip">{mission.phase}</span>
                    <small>
                      {mission.completed ? "Completed" : "Active / resumable"}
                    </small>
                  </td>
                  <td>
                    {mission.loaded_devices} loaded / {mission.reserved_devices}{" "}
                    reserved
                  </td>
                  <td>
                    {mission.capital_system ?? "No capital yet"}
                    <small>{mission.selected_sites} selected sites</small>
                    {!!mission.warnings.length && (
                      <small>{mission.warnings.length} warnings</small>
                    )}
                  </td>
                  <td>
                    <button onClick={onOpenHistory}>Open history</button>
                    <button
                      onClick={() => {
                        onOpenGalaxy(mission.target_system);
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
      )}
      <p className="table-summary">Revision {data?.metadata.revision ?? "—"}</p>
    </article>
  );
}
