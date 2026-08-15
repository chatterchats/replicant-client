import { useMemo } from "react";

import { daemonApi } from "./api";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  AutofactorySnapshot,
  DescriptorCatalog,
  DeviceSummary,
  EntityRef,
} from "./protocol";

const empty = (snapshot: AutofactorySnapshot) =>
  snapshot.factories.length === 0;

export function AutofactoryPage(props: {
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "autofactories",
    fetcher: (signal) => daemonApi.autofactories(signal),
    isEmpty: empty,
  });
  return <AutofactoryContent {...query} {...props} />;
}

export function AutofactoryContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  descriptors,
  onSelectDevice,
  onOpenSystem,
  onSelectWorkflow,
  onRunCommand,
}: {
  data?: AutofactorySnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const operations = useMemo(
    () =>
      applicableDescriptorCommands(descriptors, "device").filter((command) =>
        /print|manufactur|factory/i.test(
          `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
        ),
      ),
    [descriptors],
  );
  if (!data && status === "loading")
    return (
      <article className="page loading-state">Loading Autofactories…</article>
    );
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Autofactories unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  const utilization = data?.utilization;
  const currentJobs =
    data?.factories.flatMap((factory) =>
      factory.current_job ? [{ factory, job: factory.current_job }] : [],
    ) ?? [];
  const queuedJobs =
    data?.factories.flatMap((factory) =>
      factory.queued_jobs.map((job) => ({
        code: factory.device.entity.id,
        job,
      })),
    ) ?? [];
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Assets</p>
          <h1>Autofactory</h1>
          <p className="lede">
            Live manufacturing availability and print queues.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error ? <p className="inline-warning">Refresh failed: {error}</p> : null}
      {status === "empty" ? (
        <section className="empty-state">
          No managed Autofactories are available.
        </section>
      ) : (
        <>
          <section
            className="inventory-summary"
            aria-label="Manufacturing summary"
          >
            <div>
              <strong>{utilization?.total ?? 0}</strong>
              <span>Factories</span>
            </div>
            <div>
              <strong>{utilization?.available ?? 0}</strong>
              <span>Available</span>
            </div>
            <div>
              <strong>{utilization?.busy ?? 0}</strong>
              <span>Busy</span>
            </div>
            <div>
              <strong>{utilization?.queued_units ?? 0}</strong>
              <span>Queued units</span>
            </div>
            <div>
              <strong>
                {Math.round(utilization?.utilization_percent ?? 0)}%
              </strong>
              <span>Utilization</span>
            </div>
          </section>
          <section
            className="asset-operations"
            aria-label="Manufacturing operations"
          >
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
              <p>
                No manufacturing operation is registered in the descriptor
                catalogue.
              </p>
            )}
          </section>
          <section>
            <h2>Factories</h2>
            <div className="inventory-table-wrap">
              <table className="inventory-table">
                <thead>
                  <tr>
                    <th>Factory</th>
                    <th>Owner</th>
                    <th>Position</th>
                    <th>Status</th>
                    <th>Queue</th>
                    <th>Workflow</th>
                  </tr>
                </thead>
                <tbody>
                  {data?.factories.map((factory) => {
                    const device = factory.device;
                    return (
                      <tr key={device.entity.id}>
                        <td>
                          <button
                            className="link-button"
                            onClick={() => {
                              onSelectDevice(device);
                            }}
                          >
                            {device.entity.id}
                          </button>
                        </td>
                        <td>
                          {device.owner_name ?? device.owner ?? "Account"}
                        </td>
                        <td>
                          {device.system ? (
                            <button
                              className="link-button"
                              onClick={() => {
                                if (device.system) onOpenSystem(device.system);
                              }}
                            >
                              {device.location ?? device.system}
                            </button>
                          ) : (
                            (device.location ?? "Unknown")
                          )}
                        </td>
                        <td>
                          <span
                            className={`status-chip ${factory.availability}`}
                          >
                            {factory.availability}
                          </span>
                          <small>{device.status ?? "unknown"}</small>
                        </td>
                        <td>
                          {factory.queued_units} /{" "}
                          {factory.queue_capacity ?? "—"}
                        </td>
                        <td>
                          {device.claim ? (
                            <button
                              className="link-button"
                              onClick={() => {
                                if (device.claim)
                                  onSelectWorkflow(device.claim.workflow_id);
                              }}
                            >
                              {device.claim.workflow_kind}
                            </button>
                          ) : (
                            "—"
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
          <section className="asset-jobs">
            <div>
              <h2>Current jobs</h2>
              {currentJobs.length ? (
                <ul>
                  {currentJobs.map(({ factory, job }) => (
                    <li key={factory.device.entity.id}>
                      <button
                        className="link-button"
                        onClick={() => {
                          onSelectDevice(factory.device);
                        }}
                      >
                        {factory.device.entity.id}
                      </button>
                      <span>{job.device_type}</span>
                      <strong>
                        {job.eta_seconds === null
                          ? "ETA unavailable"
                          : `${String(Math.ceil(job.eta_seconds / 60))} min`}
                      </strong>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="empty-state">No active print jobs.</p>
              )}
            </div>
            <div>
              <h2>Queued jobs</h2>
              {queuedJobs.length ? (
                <ul>
                  {queuedJobs.map(({ code, job }, index) => (
                    <li key={`${code}:${String(index)}`}>
                      <span>{code}</span>
                      <span>{job.device_type}</span>
                      <strong>×{job.quantity}</strong>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="empty-state">No queued print jobs.</p>
              )}
            </div>
          </section>
        </>
      )}
    </article>
  );
}
