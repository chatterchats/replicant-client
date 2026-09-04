/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  CargoCarrierSummary,
  CargoSnapshot,
  DescriptorCatalog,
  DeviceSummary,
  EntityRef,
} from "./protocol";

const empty = (snapshot: CargoSnapshot) => snapshot.carriers.length === 0;

export function filterCargo(
  carriers: CargoCarrierSummary[],
  search: string,
  activity: string,
) {
  const query = search.trim().toLowerCase();
  return carriers.filter(({ device, resources }) => {
    const active = device.travel_destination !== null || device.claim !== null;
    if (activity === "active" && !active) return false;
    if (activity === "idle" && active) return false;
    return (
      !query ||
      [
        device.entity.id,
        device.device_type,
        device.owner,
        device.owner_name,
        device.system,
        device.location,
        ...resources.map((resource) => resource.resource),
      ].some((value) => value?.toLowerCase().includes(query))
    );
  });
}

function Capacity({
  used,
  total,
}: {
  used: number | null;
  total: number | null;
}) {
  if (total === null || total <= 0) return <span>—</span>;
  const value = Math.min(Math.max(used ?? 0, 0), total);
  return (
    <span className="capacity-meter">
      <meter min={0} max={total} value={value} />{" "}
      <small>
        {used ?? 0} / {total}
      </small>
    </span>
  );
}

export function CargoPage(props: {
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "cargo",
    fetcher: (signal) => daemonApi.cargo(signal),
    isEmpty: empty,
  });
  return <CargoContent {...query} {...props} />;
}

export function CargoContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  descriptors,
  onSelectDevice,
  onSelectEntity,
  onOpenSystem,
  onSelectWorkflow,
  onRunCommand,
}: {
  data?: CargoSnapshot;
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
  const [search, setSearch] = useState("");
  const [activity, setActivity] = useState("");
  const carriers = useMemo(
    () => filterCargo(data?.carriers ?? [], search, activity),
    [activity, data?.carriers, search],
  );
  const operations = useMemo(
    () =>
      applicableDescriptorCommands(descriptors, "device").filter((command) =>
        /cargo|transport|deliver/i.test(
          `${command.descriptor.kind} ${command.descriptor.category} ${command.descriptor.display_name}`,
        ),
      ),
    [descriptors],
  );
  if (!data && status === "loading")
    return (
      <article className="page loading-state">Loading cargo carriers…</article>
    );
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Cargo unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Assets</p>
          <h1>Cargo</h1>
          <p className="lede">
            Carrier capacity, payloads, travel, and transport claims.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error ? <p className="inline-warning">Refresh failed: {error}</p> : null}
      {status === "empty" ? (
        <section className="empty-state">
          No cargo- or attachment-capable devices are available.
        </section>
      ) : (
        <>
          <section className="inventory-summary" aria-label="Cargo summary">
            <div>
              <strong>{data?.carriers.length ?? 0}</strong>
              <span>Carriers</span>
            </div>
            <div>
              <strong>
                {data?.cargo_used ?? 0} / {data?.cargo_capacity ?? 0}
              </strong>
              <span>Cargo capacity</span>
            </div>
            <div>
              <strong>
                {data?.attachment_used ?? 0} / {data?.attachment_capacity ?? 0}
              </strong>
              <span>Attachment capacity</span>
            </div>
          </section>
          <section
            className="asset-operations"
            aria-label="Transport operations"
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
                No transport operation is registered in the descriptor
                catalogue.
              </p>
            )}
          </section>
          <section className="device-filters" aria-label="Cargo filters">
            <label className="device-search">
              <span>Search</span>
              <input
                type="search"
                placeholder="Carrier, resource, location, owner"
                value={search}
                onChange={(event) => {
                  setSearch(event.target.value);
                }}
              />
            </label>
            <label>
              <span>Activity</span>
              <select
                value={activity}
                onChange={(event) => {
                  setActivity(event.target.value);
                }}
              >
                <option value="">All carriers</option>
                <option value="active">Active transport</option>
                <option value="idle">Idle</option>
              </select>
            </label>
          </section>
          <p className="table-summary">
            Showing {carriers.length} of {data?.carriers.length ?? 0} carriers ·
            revision {data?.metadata.revision ?? "—"}
          </p>
          {carriers.length === 0 ? (
            <section className="empty-state">
              No carriers match the current filters.
            </section>
          ) : (
            <div className="inventory-table-wrap">
              <table className="inventory-table cargo-table">
                <thead>
                  <tr>
                    <th>Carrier</th>
                    <th>Owner / position</th>
                    <th>Cargo</th>
                    <th>Attachments</th>
                    <th>Contents</th>
                    <th>Transport</th>
                  </tr>
                </thead>
                <tbody>
                  {carriers.map(({ device, resources, attachment_used }) => (
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
                        <small>
                          {device.device_type ?? "unknown"} ·{" "}
                          {device.status ?? "unknown"}
                        </small>
                      </td>
                      <td>
                        {device.owner_name ?? device.owner ?? "Account"}
                        <small>
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
                        </small>
                      </td>
                      <td>
                        <Capacity
                          used={device.cargo_used}
                          total={device.cargo_capacity}
                        />
                      </td>
                      <td>
                        <Capacity
                          used={attachment_used}
                          total={device.attach_capacity}
                        />
                        <small>
                          {device.attached_devices.length} attached ·{" "}
                          {device.stowed_devices.length} stowed
                        </small>
                      </td>
                      <td>
                        {resources.length ? (
                          <ul className="inventory-contents">
                            {resources.map((resource) => (
                              <li key={resource.resource}>
                                <button
                                  className="link-button"
                                  onClick={() => {
                                    onSelectEntity({
                                      kind: "inventory",
                                      id: resource.resource,
                                    });
                                  }}
                                >
                                  {resource.resource}
                                </button>
                                <strong>{resource.quantity}</strong>
                              </li>
                            ))}
                          </ul>
                        ) : (
                          "Empty"
                        )}
                      </td>
                      <td>
                        {device.travel_destination ? (
                          <span>Traveling to {device.travel_destination}</span>
                        ) : (
                          "Idle"
                        )}
                        {device.claim ? (
                          <button
                            className="link-button workflow-link"
                            onClick={() => {
                              if (device.claim)
                                onSelectWorkflow(device.claim.workflow_id);
                            }}
                          >
                            {device.claim.workflow_kind}
                          </button>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </article>
  );
}
