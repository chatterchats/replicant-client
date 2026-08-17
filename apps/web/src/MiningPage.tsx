/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  DescriptorCatalog,
  EntityRef,
  MiningInstallationStatus,
  MiningSnapshot,
} from "./protocol";

const empty = (snapshot: MiningSnapshot) =>
  snapshot.installations.length === 0 && snapshot.workflows.length === 0;

export const miningCommands = (descriptors: DescriptorCatalog) =>
  applicableDescriptorCommands(descriptors, "system").filter((command) =>
    /mining/i.test(
      `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
    ),
  );

export function MiningPage(props: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenGalaxy: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "missions",
    fetcher: (signal) => daemonApi.mining(signal),
    isEmpty: empty,
  });
  return <MiningContent {...query} {...props} />;
}

export function MiningContent({
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
  data?: MiningSnapshot;
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
  const [system, setSystem] = useState("");
  const [completeness, setCompleteness] = useState<
    "" | MiningInstallationStatus
  >("");
  const operations = useMemo(() => miningCommands(descriptors), [descriptors]);
  const systems = [
    ...new Set(data?.installations.flatMap((row) => row.system ?? []) ?? []),
  ];
  const rows =
    data?.installations.filter(
      (row) =>
        (!system || row.system === system) &&
        (!completeness || row.status === completeness),
    ) ?? [];
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Mining…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Mining unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Mining</h1>
          <p className="lede">
            Managed mining installations and expansion work.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="asset-operations" aria-label="Mining operations">
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
          <p>No mining expansion operation is registered.</p>
        )}
      </section>
      {!!data?.workflows.length && (
        <section className="asset-jobs">
          <h2>Active expansion workflows</h2>
          {data.workflows.map((workflow) => (
            <button
              key={workflow.id}
              onClick={() => {
                onSelectWorkflow(workflow.id);
              }}
            >
              {workflow.kind} · {workflow.status}
            </button>
          ))}
        </section>
      )}
      <section className="inventory-controls" aria-label="Mining filters">
        <label>
          System
          <select
            value={system}
            onChange={(event) => {
              setSystem(event.target.value);
            }}
          >
            <option value="">All systems</option>
            {systems.map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
        <label>
          Completeness
          <select
            value={completeness}
            onChange={(event) => {
              setCompleteness(
                event.target.value as "" | MiningInstallationStatus,
              );
            }}
          >
            <option value="">All</option>
            <option value="complete">Complete</option>
            <option value="partial">Partial</option>
          </select>
        </label>
      </section>
      {!data?.installations.length ? (
        <section className="empty-state">
          No managed mining installations discovered.
        </section>
      ) : !rows.length ? (
        <section className="empty-state">
          No installations match the filters.
        </section>
      ) : (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>System / location</th>
                <th>Controller</th>
                <th>Miners</th>
                <th>Survey</th>
                <th>Maintenance</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const rowSystem = row.system;
                const controller = row.controller;
                return (
                  <tr key={row.id}>
                    <td>
                      {rowSystem ? (
                        <button
                          className="link-button"
                          onClick={() => {
                            onOpenGalaxy(rowSystem);
                          }}
                        >
                          {rowSystem}
                        </button>
                      ) : (
                        "Unknown system"
                      )}
                      <small>{row.location ?? "Unknown location"}</small>
                    </td>
                    <td>
                      {controller ? (
                        <button
                          className="link-button"
                          onClick={() => {
                            onSelectEntity(controller.entity);
                          }}
                        >
                          {controller.entity.id}
                        </button>
                      ) : (
                        "Missing"
                      )}
                    </td>
                    <td>{row.miners.length} / 4 adopted</td>
                    <td>
                      {row.survey_controller?.entity.id ?? "No controller"} ·{" "}
                      {row.survey_drones.length} / 2 drones
                    </td>
                    <td>{row.maintenance_device?.entity.id ?? "Missing"}</td>
                    <td>
                      <span className="status-chip">{row.status}</span>
                      {!!row.missing.length && (
                        <small>{row.missing.join(", ")}</small>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <p className="table-summary">Revision {data?.metadata.revision ?? "—"}</p>
    </article>
  );
}
