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
  DeviceSummary,
  DevicesSnapshot,
  EntityRef,
} from "./protocol";

export type DeviceSortKey =
  | "code"
  | "type"
  | "status"
  | "owner"
  | "system"
  | "location"
  | "capacity"
  | "claim";

export interface DeviceFilters {
  search: string;
  status: string;
  type: string;
  system: string;
  location: string;
  ownership: string;
  claim: "" | "claimed" | "unclaimed";
}

const emptyFilters: DeviceFilters = {
  search: "",
  status: "",
  type: "",
  system: "",
  location: "",
  ownership: "",
  claim: "",
};

export function filterAndSortDevices(
  devices: DeviceSummary[],
  filters: DeviceFilters,
  sort: DeviceSortKey,
  descending = false,
): DeviceSummary[] {
  const search = filters.search.trim().toLowerCase();
  const filtered = devices.filter((device) => {
    const matchesSearch = [
      device.entity.id,
      device.device_type,
      device.status,
      device.owner,
      device.system,
      device.location,
      ...device.tags,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase()
      .includes(search);
    return (
      matchesSearch &&
      (!filters.status || device.status === filters.status) &&
      (!filters.type || device.device_type === filters.type) &&
      (!filters.system || device.system === filters.system) &&
      (!filters.location || device.location === filters.location) &&
      (!filters.ownership || device.ownership === filters.ownership) &&
      (!filters.claim ||
        (device.claim ? "claimed" : "unclaimed") === filters.claim)
    );
  });
  const value = (device: DeviceSummary): string | number => {
    switch (sort) {
      case "code":
        return device.entity.id;
      case "type":
        return device.device_type ?? "";
      case "status":
        return device.status ?? "";
      case "owner":
        return device.owner ?? "";
      case "system":
        return device.system ?? "";
      case "location":
        return device.location ?? "";
      case "capacity":
        return device.operational_capacity_percent ?? -1;
      case "claim":
        return device.claim?.workflow_id ?? "";
    }
  };
  return filtered.sort((left, right) => {
    const a = value(left);
    const b = value(right);
    const order =
      typeof a === "number" && typeof b === "number"
        ? a - b
        : String(a).localeCompare(String(b), undefined, { numeric: true });
    return descending ? -order : order;
  });
}

const unique = (
  devices: DeviceSummary[],
  field: "status" | "type" | "system" | "location" | "ownership",
) => {
  const deviceField = field === "type" ? "device_type" : field;
  return [
    ...new Set(
      devices
        .map((device) => device[deviceField])
        .filter((value): value is string => typeof value === "string"),
    ),
  ].sort();
};

const devicesEmpty = (snapshot: DevicesSnapshot) =>
  snapshot.devices.length === 0;

export function DeviceSelection({
  device,
  onSelectDevice,
}: {
  device: DeviceSummary;
  onSelectDevice: (device: DeviceSummary) => void;
}) {
  return (
    <button
      className="entity-link"
      onClick={() => {
        onSelectDevice(device);
      }}
    >
      {device.entity.id}
    </button>
  );
}

export function DevicesPage({
  descriptors,
  onSelectDevice,
  onSelectEntity,
  onOpenSystem,
  onSelectWorkflow,
  onRunCommand,
}: {
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (workflowId: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "devices",
    fetcher: (signal) => daemonApi.devices(signal),
    isEmpty: devicesEmpty,
  });
  return (
    <DevicesContent
      {...query}
      descriptors={descriptors}
      onSelectDevice={onSelectDevice}
      onSelectEntity={onSelectEntity}
      onOpenSystem={onOpenSystem}
      onSelectWorkflow={onSelectWorkflow}
      onRunCommand={onRunCommand}
    />
  );
}

export function DevicesContent({
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
  data?: DevicesSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (workflowId: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [filters, setFilters] = useState(emptyFilters);
  const [sort, setSort] = useState<DeviceSortKey>("code");
  const [descending, setDescending] = useState(false);
  const rows = useMemo(
    () => filterAndSortDevices(data?.devices ?? [], filters, sort, descending),
    [data, descending, filters, sort],
  );
  const actions = useMemo(
    () => applicableDescriptorCommands(descriptors, "device"),
    [descriptors],
  );
  const chooseSort = (next: DeviceSortKey) => {
    setDescending(next === sort ? !descending : false);
    setSort(next);
  };
  const sortButton = (label: string, key: DeviceSortKey) => (
    <button
      className="table-sort"
      onClick={() => {
        chooseSort(key);
      }}
    >
      {label} {sort === key ? (descending ? "↓" : "↑") : ""}
    </button>
  );
  const update = (field: keyof DeviceFilters, value: string) => {
    setFilters((current) => ({ ...current, [field]: value }));
  };

  if (status === "loading" && !data)
    return (
      <article className="page loading-state">Loading device fleet…</article>
    );
  if (status === "error" && !data)
    return (
      <article className="page error-state">
        <h1>Devices unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page devices-page">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Assets</p>
          <h1>Devices</h1>
          <p className="lede">
            Managed fleet state, relationships, capacity, and workflow
            ownership.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>

      {error ? <p className="inline-warning">Refresh failed: {error}</p> : null}
      {status === "empty" ? (
        <section className="empty-state">
          No managed devices are currently available.
        </section>
      ) : (
        <>
          <section className="device-filters" aria-label="Device filters">
            <label className="device-search">
              <span>Search</span>
              <input
                type="search"
                placeholder="Code, type, tag, location, owner"
                value={filters.search}
                onChange={(event) => {
                  update("search", event.target.value);
                }}
              />
            </label>
            {(
              [
                ["Status", "status"],
                ["Type", "type"],
                ["System", "system"],
                ["Location", "location"],
                ["Ownership", "ownership"],
              ] as const
            ).map(([label, field]) => (
              <label key={field}>
                <span>{label}</span>
                <select
                  value={filters[field]}
                  onChange={(event) => {
                    update(field, event.target.value);
                  }}
                >
                  <option value="">All</option>
                  {unique(data?.devices ?? [], field).map((value) => (
                    <option key={value}>{value}</option>
                  ))}
                </select>
              </label>
            ))}
            <label>
              <span>Claim</span>
              <select
                value={filters.claim}
                onChange={(event) => {
                  update("claim", event.target.value);
                }}
              >
                <option value="">All</option>
                <option value="claimed">Claimed</option>
                <option value="unclaimed">Unclaimed</option>
              </select>
            </label>
          </section>

          <p className="table-summary">
            Showing {rows.length} of {data?.devices.length ?? 0} devices ·
            revision {data?.metadata.revision ?? "—"}
          </p>
          {rows.length === 0 ? (
            <section className="empty-state">
              No devices match the current filters.
            </section>
          ) : (
            <div className="device-table-wrap">
              <table className="device-table">
                <thead>
                  <tr>
                    <th>{sortButton("Code", "code")}</th>
                    <th>{sortButton("Type / status", "type")}</th>
                    <th>{sortButton("Owner", "owner")}</th>
                    <th>{sortButton("Position", "system")}</th>
                    <th>Relationship</th>
                    <th>{sortButton("Capacity", "capacity")}</th>
                    <th>{sortButton("Claim", "claim")}</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((device) => (
                    <tr key={device.entity.id}>
                      <td>
                        <DeviceSelection
                          device={device}
                          onSelectDevice={onSelectDevice}
                        />
                        {device.tags.length ? (
                          <small>{device.tags.join(" · ")}</small>
                        ) : null}
                      </td>
                      <td>
                        <strong>{device.device_type ?? "Unknown type"}</strong>
                        <span
                          className={`status-chip ${device.status ?? "unknown"}`}
                        >
                          {device.status ?? "unknown"}
                        </span>
                      </td>
                      <td>{device.owner ?? device.ownership}</td>
                      <td>
                        {device.system ? (
                          <button
                            className="entity-link"
                            onClick={() => {
                              if (device.system) onOpenSystem(device.system);
                            }}
                          >
                            {device.system}
                          </button>
                        ) : (
                          "—"
                        )}
                        {device.location ? (
                          <button
                            className="subtle-link"
                            onClick={() => {
                              if (device.location)
                                onSelectEntity({
                                  kind: "location",
                                  id: device.location,
                                });
                            }}
                          >
                            {device.location}
                          </button>
                        ) : null}
                      </td>
                      <td>
                        {device.attached_to
                          ? `Attached to ${device.attached_to}`
                          : device.stowed_in
                            ? `Stowed in ${device.stowed_in}`
                            : device.controller
                              ? `Controlled by ${device.controller}`
                              : "Independent"}
                      </td>
                      <td>
                        {device.operational_capacity_percent === null
                          ? "—"
                          : `${device.operational_capacity_percent.toFixed(0)}%`}
                        {device.cargo_capacity === null ? null : (
                          <small>
                            Cargo {device.cargo_used ?? 0}/
                            {device.cargo_capacity}
                          </small>
                        )}
                      </td>
                      <td>
                        {device.claim ? (
                          <button
                            className="claim-link"
                            onClick={() => {
                              if (device.claim)
                                onSelectWorkflow(device.claim.workflow_id);
                            }}
                          >
                            {device.claim.workflow_kind}
                            <small>{device.claim.workflow_id}</small>
                          </button>
                        ) : (
                          <span className="muted">Unclaimed</span>
                        )}
                      </td>
                      <td>
                        <details className="row-actions">
                          <summary>Actions</summary>
                          <div>
                            {actions.length ? (
                              actions.map((command) => (
                                <button
                                  key={`${command.operationClass}:${command.descriptor.kind}`}
                                  onClick={() => {
                                    onSelectDevice(device);
                                    onRunCommand(command);
                                  }}
                                >
                                  {command.descriptor.display_name}
                                </button>
                              ))
                            ) : (
                              <small>No registered device actions.</small>
                            )}
                          </div>
                        </details>
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
