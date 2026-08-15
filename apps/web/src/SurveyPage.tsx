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
  SurveySnapshot,
  WorkflowStatus,
} from "./protocol";

const empty = (snapshot: SurveySnapshot) => snapshot.missions.length === 0;

export const surveyCommands = (descriptors: DescriptorCatalog) =>
  applicableDescriptorCommands(descriptors, "system").filter((command) =>
    /survey/i.test(
      `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
    ),
  );

export function SurveyPage(props: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenGalaxy: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "missions",
    fetcher: (signal) => daemonApi.survey(signal),
    isEmpty: empty,
  });
  return <SurveyContent {...query} {...props} />;
}

export function SurveyContent({
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
  data?: SurveySnapshot;
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
  const [controlError, setControlError] = useState<string>();
  const [busy, setBusy] = useState<string>();
  const operations = useMemo(() => surveyCommands(descriptors), [descriptors]);
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Survey…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Survey unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  const control = (id: string, action: "pause" | "resume" | "cancel") => {
    if (action === "cancel" && !window.confirm("Cancel this Survey mission?"))
      return;
    setBusy(id);
    setControlError(undefined);
    void daemonApi
      .controlWorkflow(id, action)
      .then(refresh)
      .catch((reason: unknown) => {
        setControlError(String(reason));
      })
      .finally(() => {
        setBusy(undefined);
      });
  };

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Survey</h1>
          <p className="lede">
            Durable route progress and assigned survey fleets.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      {controlError && <p className="inline-warning">{controlError}</p>}
      <section className="asset-operations" aria-label="Survey operations">
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
          <p>No Survey workflow is registered.</p>
        )}
      </section>
      {!data?.missions.length ? (
        <section className="empty-state">No active Survey missions.</section>
      ) : (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Mission</th>
                <th>Fleet</th>
                <th>Route</th>
                <th>Progress</th>
                <th>Controls</th>
              </tr>
            </thead>
            <tbody>
              {data.missions.map((mission) => {
                const vessel = data.fleet.find(
                  (device) => device.entity.id === mission.vessel,
                );
                return (
                  <tr key={mission.workflow.id}>
                    <td>
                      <button
                        className="link-button"
                        onClick={() => {
                          onSelectEntity({
                            kind: "workflow",
                            id: mission.workflow.id,
                          });
                        }}
                      >
                        {mission.workflow.kind}
                      </button>
                      <small className="status-chip">{mission.phase}</small>
                    </td>
                    <td>
                      <strong>{mission.vessel}</strong>
                      <small>
                        {mission.controller ?? "No controller"} ·{" "}
                        {mission.drones.length} drones
                      </small>
                    </td>
                    <td>
                      <button
                        className="link-button"
                        onClick={() => {
                          onOpenGalaxy(mission.next_system ?? mission.center);
                        }}
                      >
                        {mission.next_system ??
                          vessel?.system ??
                          mission.center}
                      </button>
                      <small>center {mission.center}</small>
                    </td>
                    <td>
                      {mission.completed_systems} /{" "}
                      {mission.total_systems || "—"}
                    </td>
                    <td>
                      <button
                        onClick={() => {
                          onSelectWorkflow(mission.workflow.id);
                        }}
                      >
                        Open workflow
                      </button>
                      {mission.workflow.status === "paused" ? (
                        <button
                          disabled={busy === mission.workflow.id}
                          onClick={() => {
                            control(mission.workflow.id, "resume");
                          }}
                        >
                          Resume
                        </button>
                      ) : canPause(mission.workflow.status) ? (
                        <button
                          disabled={busy === mission.workflow.id}
                          onClick={() => {
                            control(mission.workflow.id, "pause");
                          }}
                        >
                          Pause
                        </button>
                      ) : null}
                      <button
                        disabled={busy === mission.workflow.id}
                        onClick={() => {
                          control(mission.workflow.id, "cancel");
                        }}
                      >
                        Cancel
                      </button>
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

function canPause(status: WorkflowStatus) {
  return (
    status === "running" || status === "waiting" || status === "reconciling"
  );
}
