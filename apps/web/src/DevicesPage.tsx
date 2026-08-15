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
  "code" | "type" | "status" | "owner" | "system" | "capacity";

export type DeviceCategory =
  | "vessel"
  | "mining"
  | "survey"
  | "ftl_comms"
  | "maintenance"
  | "transport"
  | "carrier"
  | "manufacturing"
  | "other";

export const DEVICE_CATEGORIES: readonly {
  id: DeviceCategory;
  label: string;
}[] = [
  { id: "vessel", label: "Vessel" },
  { id: "mining", label: "Mining" },
  { id: "survey", label: "Survey" },
  { id: "ftl_comms", label: "FTL Comms" },
  { id: "maintenance", label: "Maintenance" },
  { id: "transport", label: "Transport" },
  { id: "carrier", label: "Carrier" },
  { id: "manufacturing", label: "Manufacturing" },
  { id: "other", label: "Other" },
];

const categoryTypes: Record<Exclude<DeviceCategory, "other">, string[]> = {
  vessel: [
    "heaven_vessel",
    "racing_vessel",
    "cargo_vessel",
    "empty_replicant_matrix",
    "replicant_interface",
    "matrix_container",
  ],
  mining: [
    "ami_mining_controller",
    "mining_drone",
    "exotic_matter_injector",
    "exotic_particle_trap",
    "mass_driver",
    "thermal_lance",
  ],
  survey: [
    "ami_survey_controller",
    "survey_drone",
    "sensor_array",
    "seismic_monitor",
  ],
  ftl_comms: [
    "ftl_beacon",
    "ftl_relay",
    "ftl_slingshot",
    "system_hub",
    "comm_satellite",
    "mesh_relay",
    "signal_booster",
    "galactic_observatory",
    "electrodynamic_tether",
  ],
  maintenance: [
    "maintenance_drone",
    "fleet_tender",
    "system_ward",
    "defence_grid",
    "orbital_defence_platform",
    "point_defence_array",
    "shield_generator",
  ],
  transport: [
    "ami_transport_controller",
    "ami_trade_controller",
    "transport_drone",
    "transport_hauler",
    "cargo_freighter",
    "cargo_lifter",
  ],
  carrier: [
    "surge_carrier",
    "mobile_fleet",
    "surge_plate",
    "surge_platform",
    "fusion_barge",
  ],
  manufacturing: [
    "autofactory",
    "structural_fabricator",
    "atmo_processor",
    "filtration_array",
    "hydroponic_bay",
    "nutrient_synthesizer",
    "orbital_farm",
    "solar_collector",
    "power_cell_array",
    "compute_core",
  ],
};

const categoryByType = new Map(
  Object.entries(categoryTypes).flatMap(([category, types]) =>
    types.map((type) => [type, category as DeviceCategory] as const),
  ),
);

export interface DeviceFilters {
  search: string;
  status: string;
  type: string;
  system: string;
  owner: string;
}

export interface DeviceTreeRow {
  device: DeviceSummary;
  depth: number;
  relationship: "attached" | "stowed" | null;
}

export interface DeviceGroup {
  category: DeviceCategory;
  label: string;
  rows: DeviceTreeRow[];
}

export interface SystemOption {
  system: string;
  count: number;
}

const emptyFilters: DeviceFilters = {
  search: "",
  status: "",
  type: "",
  system: "",
  owner: "",
};

export function deviceCategory(deviceType: string | null): DeviceCategory {
  return deviceType
    ? (categoryByType.get(deviceType.toLowerCase()) ?? "other")
    : "other";
}

export function normalizedDeviceStatus(status: string | null): string {
  const normalized = status?.trim().toLowerCase() ?? "unknown";
  if (normalized.startsWith("mining")) return "mining";
  if (normalized.startsWith("repairing")) return "repairing";
  return normalized;
}

export function systemOptions(devices: DeviceSummary[]): SystemOption[] {
  const counts = new Map<string, number>();
  for (const device of devices) {
    if (device.system)
      counts.set(device.system, (counts.get(device.system) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([system, count]) => ({ system, count }))
    .sort(
      (left, right) =>
        right.count - left.count || left.system.localeCompare(right.system),
    );
}

function deviceValue(
  device: DeviceSummary,
  sort: DeviceSortKey,
): string | number {
  switch (sort) {
    case "code":
      return device.entity.id;
    case "type":
      return device.device_type ?? "";
    case "status":
      return normalizedDeviceStatus(device.status);
    case "owner":
      return device.owner ?? "";
    case "system":
      return device.system ?? "";
    case "capacity":
      return device.operational_capacity_percent ?? -1;
  }
}

export function filterAndSortDevices(
  devices: DeviceSummary[],
  filters: DeviceFilters,
  sort: DeviceSortKey,
  descending = false,
): DeviceSummary[] {
  const search = filters.search.trim().toLowerCase();
  return devices
    .filter((device) => {
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
        (!filters.status ||
          normalizedDeviceStatus(device.status) === filters.status) &&
        (!filters.type || device.device_type === filters.type) &&
        (!filters.system || device.system === filters.system) &&
        (!filters.owner || device.owner === filters.owner)
      );
    })
    .sort((left, right) => {
      const a = deviceValue(left, sort);
      const b = deviceValue(right, sort);
      const order =
        typeof a === "number" && typeof b === "number"
          ? a - b
          : String(a).localeCompare(String(b), undefined, { numeric: true });
      return descending ? -order : order;
    });
}

function physicalParent(
  device: DeviceSummary,
  devices: Map<string, DeviceSummary>,
) {
  if (device.stowed_in && devices.has(device.stowed_in))
    return { code: device.stowed_in, relationship: "stowed" as const };
  if (device.attached_to && devices.has(device.attached_to))
    return { code: device.attached_to, relationship: "attached" as const };
  return null;
}

export function groupDevices(devices: DeviceSummary[]): DeviceGroup[] {
  const byCode = new Map(devices.map((device) => [device.entity.id, device]));
  const order = new Map(
    devices.map((device, index) => [device.entity.id, index]),
  );
  const children = new Map<string, DeviceSummary[]>();
  for (const device of devices) {
    const parent = physicalParent(device, byCode);
    if (parent)
      children.set(parent.code, [...(children.get(parent.code) ?? []), device]);
  }
  for (const values of children.values())
    values.sort(
      (a, b) => (order.get(a.entity.id) ?? 0) - (order.get(b.entity.id) ?? 0),
    );

  const grouped = new Map<DeviceCategory, DeviceTreeRow[]>();
  const visited = new Set<string>();
  const append = (
    device: DeviceSummary,
    category: DeviceCategory,
    depth: number,
    relationship: DeviceTreeRow["relationship"],
  ) => {
    if (visited.has(device.entity.id)) return;
    visited.add(device.entity.id);
    grouped.set(category, [
      ...(grouped.get(category) ?? []),
      { device, depth, relationship },
    ]);
    for (const child of children.get(device.entity.id) ?? []) {
      const parent = physicalParent(child, byCode);
      append(child, category, depth + 1, parent?.relationship ?? null);
    }
  };

  for (const device of devices) {
    if (!physicalParent(device, byCode))
      append(device, deviceCategory(device.device_type), 0, null);
  }
  for (const device of devices)
    append(device, deviceCategory(device.device_type), 0, null);

  return DEVICE_CATEGORIES.flatMap(({ id, label }) => {
    const rows = grouped.get(id) ?? [];
    return rows.length ? [{ category: id, label, rows }] : [];
  });
}

const uniqueStrings = (values: Array<string | null>) =>
  [...new Set(values.filter((value): value is string => Boolean(value)))].sort(
    (a, b) => a.localeCompare(b, undefined, { numeric: true }),
  );

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

function SystemFilter({
  devices,
  value,
  onChange,
}: {
  devices: DeviceSummary[];
  value: string;
  onChange: (value: string) => void;
}) {
  const options = systemOptions(devices);
  return (
    <details className="system-filter">
      <summary>
        <span>System</span>
        <strong>{value || "All systems"}</strong>
      </summary>
      <div>
        <button
          className={!value ? "selected" : ""}
          onClick={(event) => {
            onChange("");
            event.currentTarget.closest("details")?.removeAttribute("open");
          }}
        >
          <strong>All systems</strong>
          <small>{devices.length} devices</small>
        </button>
        {options.map((option) => (
          <button
            className={value === option.system ? "selected" : ""}
            key={option.system}
            onClick={(event) => {
              onChange(option.system);
              event.currentTarget.closest("details")?.removeAttribute("open");
            }}
          >
            <strong>{option.system}</strong>
            <small>
              {option.count} {option.count === 1 ? "device" : "devices"}
            </small>
          </button>
        ))}
      </div>
    </details>
  );
}

export function DevicesPage({
  descriptors,
  onSelectDevice,
  onSelectEntity,
  onOpenSystem,
  onRunCommand,
}: {
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
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
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [filters, setFilters] = useState(emptyFilters);
  const [sort, setSort] = useState<DeviceSortKey>("code");
  const [descending, setDescending] = useState(false);
  const [collapsed, setCollapsed] = useState<Set<DeviceCategory>>(
    () => new Set(["ftl_comms"]),
  );
  const allDevices = useMemo(() => data?.devices ?? [], [data?.devices]);
  const rows = useMemo(
    () => filterAndSortDevices(allDevices, filters, sort, descending),
    [allDevices, descending, filters, sort],
  );
  const groups = useMemo(() => groupDevices(rows), [rows]);
  const actions = useMemo(
    () => applicableDescriptorCommands(descriptors, "device"),
    [descriptors],
  );
  const statuses = useMemo(
    () =>
      uniqueStrings(
        allDevices.map((device) => normalizedDeviceStatus(device.status)),
      ),
    [allDevices],
  );
  const types = useMemo(
    () => uniqueStrings(allDevices.map((device) => device.device_type)),
    [allDevices],
  );
  const owners = useMemo(
    () => uniqueStrings(allDevices.map((device) => device.owner)),
    [allDevices],
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
  const toggleGroup = (category: DeviceCategory) => {
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(category)) next.delete(category);
      else next.add(category);
      return next;
    });
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
            Managed fleet state, physical hierarchy, capacity, and ownership.
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
                placeholder="Code, type, tag, system, owner"
                value={filters.search}
                onChange={(event) => {
                  update("search", event.target.value);
                }}
              />
            </label>
            <label>
              <span>Status</span>
              <select
                value={filters.status}
                onChange={(event) => {
                  update("status", event.target.value);
                }}
              >
                <option value="">All statuses</option>
                {statuses.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
            <label>
              <span>Type</span>
              <select
                value={filters.type}
                onChange={(event) => {
                  update("type", event.target.value);
                }}
              >
                <option value="">All types</option>
                {types.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
            <SystemFilter
              devices={allDevices}
              value={filters.system}
              onChange={(value) => {
                update("system", value);
              }}
            />
            <label>
              <span>Ownership</span>
              <select
                value={filters.owner}
                onChange={(event) => {
                  update("owner", event.target.value);
                }}
              >
                <option value="">All replicants</option>
                {owners.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
          </section>

          <div className="fleet-summary">
            <p className="table-summary">
              Showing {rows.length} of {allDevices.length} devices · revision{" "}
              {data?.metadata.revision ?? "—"}
            </p>
            <div>
              <button
                onClick={() => {
                  setCollapsed(new Set());
                }}
              >
                Expand all
              </button>
              <button
                onClick={() => {
                  setCollapsed(new Set(groups.map((group) => group.category)));
                }}
              >
                Collapse all
              </button>
            </div>
          </div>
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
                    <th>{sortButton("Capacity", "capacity")}</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                {groups.map((group) => {
                  const isCollapsed = collapsed.has(group.category);
                  return (
                    <tbody key={group.category}>
                      <tr className="device-group-row">
                        <th colSpan={6}>
                          <button
                            aria-expanded={!isCollapsed}
                            onClick={() => {
                              toggleGroup(group.category);
                            }}
                          >
                            <span aria-hidden="true">
                              {isCollapsed ? "▸" : "▾"}
                            </span>
                            <strong>{group.label}</strong>
                            <small>{group.rows.length} devices</small>
                          </button>
                        </th>
                      </tr>
                      {isCollapsed
                        ? null
                        : group.rows.map(({ device, depth, relationship }) => (
                            <tr key={device.entity.id}>
                              <td>
                                <div
                                  className="device-tree-cell"
                                  style={{
                                    paddingLeft: `${String(depth * 18)}px`,
                                  }}
                                >
                                  {depth > 0 ? (
                                    <span
                                      className="tree-branch"
                                      aria-hidden="true"
                                    >
                                      ↳
                                    </span>
                                  ) : null}
                                  <div>
                                    <DeviceSelection
                                      device={device}
                                      onSelectDevice={onSelectDevice}
                                    />
                                    {relationship ? (
                                      <small className="relationship-label">
                                        {relationship}
                                      </small>
                                    ) : null}
                                    {device.tags.length ? (
                                      <small>{device.tags.join(" · ")}</small>
                                    ) : null}
                                  </div>
                                </div>
                              </td>
                              <td>
                                <strong>
                                  {device.device_type ?? "Unknown type"}
                                </strong>
                                <span
                                  className={`status-chip ${normalizedDeviceStatus(device.status)}`}
                                >
                                  {device.status ?? "unknown"}
                                </span>
                              </td>
                              <td>
                                {device.owner ?? (
                                  <span className="muted">Unassigned</span>
                                )}
                              </td>
                              <td>
                                {device.system ? (
                                  <button
                                    className="entity-link"
                                    onClick={() => {
                                      if (device.system)
                                        onOpenSystem(device.system);
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
                                      <small>
                                        No registered device actions.
                                      </small>
                                    )}
                                  </div>
                                </details>
                              </td>
                            </tr>
                          ))}
                    </tbody>
                  );
                })}
              </table>
            </div>
          )}
        </>
      )}
    </article>
  );
}
