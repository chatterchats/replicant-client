/* eslint-disable react-refresh/only-export-components */
import { useEffect, useMemo, useState } from "react";

import { daemonApi } from "./api";
import { ConfirmDialog, type ConfirmRequest } from "./ConfirmDialog";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { useDaemonState } from "./daemon";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  DescriptorCatalog,
  DeviceSummary,
  DevicesSnapshot,
  EntityRef,
  FiniteExecution,
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
  | "system"
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
  { id: "system", label: "System" },
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
  mining: ["ami_mining_controller", "mining_drone"],
  survey: ["ami_survey_controller", "survey_drone"],
  ftl_comms: ["ftl_beacon", "ftl_relay", "deep_space_relay_station"],
  maintenance: ["maintenance_drone"],
  transport: [
    "ami_transport_controller",
    "transport_drone",
    "transport_hauler",
    "cargo_freighter",
  ],
  carrier: [
    "surge_carrier",
    "mobile_fleet",
    "surge_plate",
    "surge_platform",
    "fusion_barge",
  ],
  manufacturing: ["autofactory"],
  system: ["system_hub", "system_ward"],
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
  region: string;
  system: string;
  owner: string;
}

export interface DeviceTreeRow {
  device: DeviceSummary;
  depth: number;
  relationship: "attached" | "controlled" | "stowed" | null;
}

export interface VisibleDeviceTreeRow extends DeviceTreeRow {
  hasChildren: boolean;
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
  region: "",
  system: "",
  owner: "",
};

export function deviceRefreshParameters(
  filters: DeviceFilters,
): Record<string, string> {
  return {
    ...(filters.owner ? { replicant_code: filters.owner } : {}),
    ...(filters.type ? { device_type: filters.type } : {}),
    ...(filters.system ? { location: filters.system } : {}),
  };
}

export const BULK_DEVICE_COMMANDS = [
  { id: "activate", label: "Activate", destructive: false },
  { id: "assemble", label: "Assemble", destructive: false },
  { id: "cancel", label: "Cancel", destructive: false },
  { id: "clear_queue", label: "Clear queue", destructive: false },
  { id: "deactivate", label: "Deactivate", destructive: false },
  { id: "decommission", label: "Decommission", destructive: true },
  { id: "deploy", label: "Deploy", destructive: false },
  { id: "compact", label: "Compact", destructive: false },
  { id: "unfurl", label: "Unfurl", destructive: false },
  { id: "launch", label: "Launch", destructive: false },
  { id: "recall", label: "Recall", destructive: false },
  { id: "scan", label: "Scan", destructive: false },
  { id: "search", label: "Search", destructive: false },
  { id: "system_scan", label: "System scan", destructive: false },
  { id: "travel", label: "Travel", destructive: false },
  { id: "withdraw", label: "Withdraw", destructive: false },
  { id: "retrieve", label: "Retrieve", destructive: false },
] as const;

export type BulkDeviceCommand = (typeof BULK_DEVICE_COMMANDS)[number]["id"];

export interface BulkDeviceEligibility {
  eligible: DeviceSummary[];
  incompatible: DeviceSummary[];
}

export interface BulkDeviceResultItem {
  kind: "succeeded" | "failed";
  device: string;
  operation_id: string | null;
  operation_status: string | null;
  error: string | null;
}

export function bulkDeviceEligibility(
  devices: DeviceSummary[],
  selected: ReadonlySet<string>,
  command: BulkDeviceCommand | "",
): BulkDeviceEligibility {
  const selectedDevices = devices.filter((device) =>
    selected.has(device.entity.id),
  );
  if (!command) return { eligible: [], incompatible: selectedDevices };
  const eligible = selectedDevices.filter((device) =>
    device.available_commands.includes(command),
  );
  const eligibleCodes = new Set(eligible.map((device) => device.entity.id));
  return {
    eligible,
    incompatible: selectedDevices.filter(
      (device) => !eligibleCodes.has(device.entity.id),
    ),
  };
}

export function bulkDeviceResultItems(result: unknown): BulkDeviceResultItem[] {
  if (!result || typeof result !== "object" || Array.isArray(result)) return [];
  const results = (result as { results?: unknown }).results;
  if (!Array.isArray(results)) return [];
  return results.flatMap((value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return [];
    const item = value as Record<string, unknown>;
    if (
      (item.kind !== "succeeded" && item.kind !== "failed") ||
      typeof item.device !== "string"
    )
      return [];
    return [
      {
        kind: item.kind,
        device: item.device,
        operation_id:
          typeof item.operation_id === "string" ? item.operation_id : null,
        operation_status:
          typeof item.operation_status === "string"
            ? item.operation_status
            : null,
        error: typeof item.error === "string" ? item.error : null,
      },
    ];
  });
}

export function bulkDeviceOperationParameters(
  command: BulkDeviceCommand,
  devices: DeviceSummary[],
  destination: string,
): Record<string, string> {
  return {
    devices: devices.map((device) => device.entity.id).join(","),
    command,
    ...(command === "travel" ? { destination: destination.trim() } : {}),
  };
}

function bulkCommandInfo(command: BulkDeviceCommand | "") {
  return BULK_DEVICE_COMMANDS.find((item) => item.id === command);
}

function confirmationItems(devices: DeviceSummary[]): string[] {
  const visible = devices.slice(0, 24).map((device) => device.entity.id);
  if (devices.length > visible.length)
    visible.push(`… and ${String(devices.length - visible.length)} more`);
  return visible;
}

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
      return device.owner_name ?? device.owner ?? "";
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
  const filtered = devices.filter((device) => {
    const matchesSearch = [
      device.entity.id,
      device.device_type,
      device.status,
      device.owner,
      device.owner_name,
      device.region,
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
      (!filters.region || device.region === filters.region) &&
      (!filters.system || device.system === filters.system) &&
      (!filters.owner || device.owner === filters.owner)
    );
  });
  const codes = new Set(filtered.map((device) => device.entity.id));
  const parents = new Set<string>();
  for (const device of filtered) {
    for (const parent of [
      device.stowed_in,
      device.attached_to,
      device.controller,
    ])
      if (parent && codes.has(parent)) parents.add(parent);
    if (
      [
        ...device.stowed_devices,
        ...device.attached_devices,
        ...device.controlled_devices,
      ].some((child) => codes.has(child))
    )
      parents.add(device.entity.id);
  }
  return filtered.sort((left, right) => {
    const parentOrder =
      Number(parents.has(right.entity.id)) -
      Number(parents.has(left.entity.id));
    if (parentOrder !== 0) return parentOrder;
    const a = deviceValue(left, sort);
    const b = deviceValue(right, sort);
    let order =
      typeof a === "number" && typeof b === "number"
        ? a - b
        : String(a).localeCompare(String(b), undefined, { numeric: true });
    if (order === 0 && sort === "type")
      order = (left.system ?? "").localeCompare(right.system ?? "");
    if (order === 0)
      order = left.entity.id.localeCompare(right.entity.id, undefined, {
        numeric: true,
      });
    return descending ? -order : order;
  });
}

function deviceParent(
  device: DeviceSummary,
  devices: Map<string, DeviceSummary>,
  controlledBy: Map<string, string>,
) {
  if (device.stowed_in && devices.has(device.stowed_in))
    return { code: device.stowed_in, relationship: "stowed" as const };
  if (device.attached_to && devices.has(device.attached_to))
    return { code: device.attached_to, relationship: "attached" as const };
  const controller = device.controller ?? controlledBy.get(device.entity.id);
  if (controller && devices.has(controller))
    return { code: controller, relationship: "controlled" as const };
  return null;
}

export function groupDevices(devices: DeviceSummary[]): DeviceGroup[] {
  const byCode = new Map(devices.map((device) => [device.entity.id, device]));
  const controlledBy = new Map<string, string>();
  for (const device of devices)
    for (const child of device.controlled_devices)
      controlledBy.set(child, device.entity.id);
  const order = new Map(
    devices.map((device, index) => [device.entity.id, index]),
  );
  const children = new Map<string, DeviceSummary[]>();
  for (const device of devices) {
    const parent = deviceParent(device, byCode, controlledBy);
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
      const parent = deviceParent(child, byCode, controlledBy);
      append(child, category, depth + 1, parent?.relationship ?? null);
    }
  };

  for (const device of devices) {
    if (!deviceParent(device, byCode, controlledBy))
      append(device, deviceCategory(device.device_type), 0, null);
  }
  for (const device of devices)
    append(device, deviceCategory(device.device_type), 0, null);

  return DEVICE_CATEGORIES.flatMap(({ id, label }) => {
    const rows = grouped.get(id) ?? [];
    return rows.length ? [{ category: id, label, rows }] : [];
  });
}

export function visibleDeviceRows(
  rows: DeviceTreeRow[],
  collapsed: ReadonlySet<string>,
): VisibleDeviceTreeRow[] {
  const visible: VisibleDeviceTreeRow[] = [];
  let hiddenBelowDepth: number | null = null;
  for (const [index, row] of rows.entries()) {
    if (hiddenBelowDepth !== null) {
      if (row.depth > hiddenBelowDepth) continue;
      hiddenBelowDepth = null;
    }
    const hasChildren = (rows[index + 1]?.depth ?? 0) > row.depth;
    visible.push({ ...row, hasChildren });
    if (hasChildren && collapsed.has(row.device.entity.id))
      hiddenBelowDepth = row.depth;
  }
  return visible;
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
    <div className="system-filter-field">
      <span>System</span>
      <details className="system-filter">
        <summary>{value || "All systems"}</summary>
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
    </div>
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
  const historyRevision = useDaemonState().invalidated.history ?? 0;
  return (
    <DevicesContent
      {...query}
      historyRevision={historyRevision}
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
  historyRevision = 0,
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
  historyRevision?: number;
  descriptors: DescriptorCatalog;
  onSelectDevice: (device: DeviceSummary) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [filters, setFilters] = useState(emptyFilters);
  const [sort, setSort] = useState<DeviceSortKey>("type");
  const [descending, setDescending] = useState(false);
  const [collapsed, setCollapsed] = useState<Set<DeviceCategory>>(
    () => new Set(["ftl_comms"]),
  );
  const [collapsedDevices, setCollapsedDevices] = useState<Set<string>>(
    () => new Set(),
  );
  const [selectedDevices, setSelectedDevices] = useState<Set<string>>(
    () => new Set(),
  );
  const [bulkCommand, setBulkCommand] = useState<BulkDeviceCommand | "">("");
  const [bulkDestination, setBulkDestination] = useState("");
  const [bulkRun, setBulkRun] = useState<{
    command: BulkDeviceCommand;
    deviceIds: string[];
    execution: FiniteExecution;
  } | null>(null);
  const [bulkError, setBulkError] = useState<string | null>(null);
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(
    null,
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
  const hasBulkLifecycleAction = useMemo(
    () =>
      descriptors.actions.some(
        (descriptor) => descriptor.kind === "device.lifecycle.bulk",
      ),
    [descriptors.actions],
  );
  const deviceRefreshAction = useMemo(
    () =>
      descriptors.actions.find(
        (descriptor) => descriptor.kind === "device.refresh",
      ),
    [descriptors.actions],
  );
  const currentRefreshParameters = useMemo(
    () => deviceRefreshParameters(filters),
    [filters],
  );
  const selectedCount = selectedDevices.size;
  const { eligible: bulkEligible, incompatible: bulkIncompatible } = useMemo(
    () => bulkDeviceEligibility(allDevices, selectedDevices, bulkCommand),
    [allDevices, bulkCommand, selectedDevices],
  );
  const bulkCommandOptions = useMemo(
    () =>
      BULK_DEVICE_COMMANDS.map((command) => ({
        ...command,
        eligible: bulkDeviceEligibility(allDevices, selectedDevices, command.id)
          .eligible.length,
      })).filter((command) => command.eligible > 0),
    [allDevices, selectedDevices],
  );
  const bulkInfo = bulkCommandInfo(bulkCommand);
  const bulkResults = bulkDeviceResultItems(bulkRun?.execution.result);
  const failedBulkResults = bulkResults.filter(
    (item) => item.kind === "failed",
  );
  const bulkExecutionId = bulkRun?.execution.id;
  const bulkExecutionStatus = bulkRun?.execution.status;
  const bulkRunning = bulkExecutionStatus === "running";
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
    () =>
      [
        ...new Map(
          allDevices.flatMap((device) =>
            device.owner
              ? [[device.owner, device.owner_name ?? device.owner] as const]
              : [],
          ),
        ),
      ].sort((left, right) => left[1].localeCompare(right[1])),
    [allDevices],
  );
  const regions = useMemo(
    () => uniqueStrings(allDevices.map((device) => device.region)),
    [allDevices],
  );
  useEffect(() => {
    const present = new Set(allDevices.map((device) => device.entity.id));
    setSelectedDevices((current) => {
      if ([...current].every((id) => present.has(id))) return current;
      return new Set([...current].filter((id) => present.has(id)));
    });
  }, [allDevices]);

  useEffect(() => {
    if (!bulkExecutionId || bulkExecutionStatus !== "running") return;
    const controller = new AbortController();
    void daemonApi
      .history(controller.signal)
      .then((history) => {
        const execution = history.find((item) => item.id === bulkExecutionId);
        if (!execution) return;
        setBulkRun((current) => {
          if (!current || current.execution.id !== execution.id) return current;
          return { ...current, execution };
        });
      })
      .catch((fetchError: unknown) => {
        if (!controller.signal.aborted) setBulkError(String(fetchError));
      });
    return () => {
      controller.abort();
    };
  }, [bulkExecutionId, bulkExecutionStatus, historyRevision]);

  const toggleDeviceSelection = (device: string, checked: boolean) => {
    setSelectedDevices((current) => {
      const next = new Set(current);
      if (checked) next.add(device);
      else next.delete(device);
      return next;
    });
  };
  const setFilteredSelection = (checked: boolean) => {
    setSelectedDevices((current) => {
      const next = new Set(current);
      for (const device of rows) {
        if (checked) next.add(device.entity.id);
        else next.delete(device.entity.id);
      }
      return next;
    });
  };
  const filteredSelected = rows.filter((device) =>
    selectedDevices.has(device.entity.id),
  ).length;
  const allFilteredSelected =
    rows.length > 0 && filteredSelected === rows.length;

  const submitBulkCommand = async (
    command: BulkDeviceCommand,
    targets: DeviceSummary[],
  ) => {
    setBulkError(null);
    try {
      const execution = await daemonApi.runOperation(
        "action",
        "device.lifecycle.bulk",
        bulkDeviceOperationParameters(command, targets, bulkDestination),
      );
      setBulkRun({
        command,
        deviceIds: targets.map((device) => device.entity.id),
        execution,
      });
    } catch (runError) {
      setBulkError(String(runError));
    }
  };

  const requestBulkCommand = () => {
    if (
      !bulkCommand ||
      bulkEligible.length === 0 ||
      bulkRunning ||
      (bulkCommand === "travel" && !bulkDestination.trim())
    )
      return;
    const info = bulkCommandInfo(bulkCommand);
    if (!info) return;
    const skipped = bulkIncompatible.length;
    setConfirmRequest({
      title: `${info.label} ${String(bulkEligible.length)} ${
        bulkEligible.length === 1 ? "device" : "devices"
      }?`,
      message:
        skipped > 0
          ? `${String(skipped)} selected ${
              skipped === 1 ? "device does" : "devices do"
            } not currently advertise this command and will be skipped.${
              bulkCommand === "travel"
                ? ` Destination: ${bulkDestination.trim()}.`
                : ""
            }`
          : `The command will be submitted through the managed device operation path for every selected compatible device.${
              bulkCommand === "travel"
                ? ` Destination: ${bulkDestination.trim()}.`
                : ""
            }`,
      items: confirmationItems(bulkEligible),
      confirmLabel: `${info.label} ${String(bulkEligible.length)}`,
      cancelLabel: "Cancel",
      requireTyped: info.destructive ? "DECOMMISSION" : undefined,
      destructive: info.destructive,
      onConfirm: () => {
        void submitBulkCommand(bulkCommand, bulkEligible);
      },
    });
  };

  const openDeviceRefresh = (initialParameters: Record<string, string>) => {
    if (!deviceRefreshAction) return;
    onRunCommand({
      descriptor: deviceRefreshAction,
      operationClass: "action",
      initialParameters,
    });
  };

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
        <div className="page-heading-actions">
          <button disabled={refreshing} onClick={() => void refresh()}>
            {refreshing ? "Reloading…" : "Reload view"}
          </button>
          <button
            disabled={!deviceRefreshAction}
            onClick={() => {
              openDeviceRefresh({});
            }}
          >
            Refresh devices…
          </button>
          <button
            disabled={
              !deviceRefreshAction ||
              Object.keys(currentRefreshParameters).length === 0
            }
            title="Uses the current Type, System, and Ownership filters"
            onClick={() => {
              openDeviceRefresh(currentRefreshParameters);
            }}
          >
            Refresh current filters…
          </button>
        </div>
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
              <span>Region</span>
              <select
                value={filters.region}
                onChange={(event) => {
                  update("region", event.target.value);
                }}
              >
                <option value="">All regions</option>
                {regions.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Ownership</span>
              <select
                value={filters.owner}
                onChange={(event) => {
                  update("owner", event.target.value);
                }}
              >
                <option value="">All replicants</option>
                {owners.map(([id, name]) => (
                  <option key={id} value={id}>
                    {name}
                  </option>
                ))}
              </select>
            </label>
          </section>

          <section
            className="device-bulk-actions"
            aria-label="Bulk device actions"
          >
            <div className="device-bulk-selection">
              <strong>{selectedCount} selected</strong>
              <span>{filteredSelected} in the current filtered view</span>
              <button
                disabled={rows.length === 0 || bulkRunning}
                onClick={() => {
                  setFilteredSelection(!allFilteredSelected);
                }}
              >
                {allFilteredSelected
                  ? "Deselect filtered"
                  : "Select all filtered"}
              </button>
              <button
                disabled={selectedCount === 0 || bulkRunning}
                onClick={() => {
                  setSelectedDevices(new Set<string>());
                }}
              >
                Clear selection
              </button>
            </div>
            <div className="device-bulk-command">
              <label>
                <span>Bulk command</span>
                <select
                  aria-label="Bulk device command"
                  disabled={
                    selectedCount === 0 ||
                    !hasBulkLifecycleAction ||
                    bulkRunning
                  }
                  value={bulkCommand}
                  onChange={(event) => {
                    setBulkCommand(
                      event.target.value as BulkDeviceCommand | "",
                    );
                  }}
                >
                  <option value="">Choose command…</option>
                  {bulkCommandOptions.map((command) => (
                    <option key={command.id} value={command.id}>
                      {command.label} ({command.eligible}/{selectedCount})
                    </option>
                  ))}
                </select>
              </label>
              {bulkCommand === "travel" ? (
                <label>
                  <span>Destination</span>
                  <input
                    aria-label="Bulk travel destination"
                    placeholder="System or location"
                    value={bulkDestination}
                    onChange={(event) => {
                      setBulkDestination(event.target.value);
                    }}
                  />
                </label>
              ) : null}
              <button
                className={bulkInfo?.destructive ? "danger" : "primary"}
                disabled={
                  !hasBulkLifecycleAction ||
                  !bulkCommand ||
                  bulkEligible.length === 0 ||
                  (bulkCommand === "travel" && !bulkDestination.trim()) ||
                  bulkRunning
                }
                onClick={requestBulkCommand}
              >
                {bulkRunning
                  ? "Bulk command running…"
                  : bulkInfo
                    ? `${bulkInfo.label} ${String(bulkEligible.length)}`
                    : "Run command"}
              </button>
            </div>
            {!hasBulkLifecycleAction ? (
              <p className="inline-warning">
                Bulk device control is unavailable from this daemon build.
              </p>
            ) : bulkCommand && bulkIncompatible.length > 0 ? (
              <p className="muted">
                {bulkIncompatible.length} selected{" "}
                {bulkIncompatible.length === 1 ? "device is" : "devices are"}{" "}
                incompatible and will be skipped.
              </p>
            ) : null}
          </section>

          {bulkRun ? (
            <section
              className={`device-bulk-status ${bulkRun.execution.status}`}
              aria-live="polite"
            >
              <div>
                <strong>
                  {bulkCommandInfo(bulkRun.command)?.label ?? bulkRun.command}
                </strong>
                <span>
                  {bulkRun.execution.status === "running"
                    ? `Running across ${String(bulkRun.deviceIds.length)} devices…`
                    : `${String(bulkRun.execution.summary.succeeded)} succeeded · ${String(
                        bulkRun.execution.summary.failed,
                      )} failed · ${String(bulkRun.execution.summary.skipped)} skipped`}
                </span>
              </div>
              {bulkRun.execution.error ? (
                <p className="inline-warning">{bulkRun.execution.error}</p>
              ) : null}
              {failedBulkResults.length > 0 ? (
                <details>
                  <summary>{failedBulkResults.length} failed devices</summary>
                  <ul>
                    {failedBulkResults.map((item) => (
                      <li key={item.device}>
                        <strong>{item.device}</strong>
                        <span>
                          {item.error ??
                            item.operation_status ??
                            "managed operation failed"}
                        </span>
                      </li>
                    ))}
                  </ul>
                </details>
              ) : null}
            </section>
          ) : null}
          {bulkError ? (
            <p className="inline-warning">
              Bulk action update failed: {bulkError}
            </p>
          ) : null}

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
                    <th className="device-select-column">
                      <input
                        aria-label="Select all filtered devices"
                        checked={allFilteredSelected}
                        disabled={rows.length === 0 || bulkRunning}
                        type="checkbox"
                        onChange={(event) => {
                          setFilteredSelection(event.target.checked);
                        }}
                      />
                    </th>
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
                  const branchIds = group.rows.flatMap((row, index) =>
                    (group.rows[index + 1]?.depth ?? 0) > row.depth
                      ? [row.device.entity.id]
                      : [],
                  );
                  const hasCollapsedBranches = branchIds.some((id) =>
                    collapsedDevices.has(id),
                  );
                  const visibleRows = visibleDeviceRows(
                    group.rows,
                    collapsedDevices,
                  );
                  return (
                    <tbody key={group.category}>
                      <tr className="device-group-row">
                        <th colSpan={7}>
                          <div>
                            <button
                              className="group-toggle"
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
                            <button
                              className="branch-toggle"
                              disabled={branchIds.length === 0}
                              onClick={() => {
                                setCollapsedDevices((current) => {
                                  const next = new Set(current);
                                  for (const id of branchIds)
                                    if (hasCollapsedBranches) next.delete(id);
                                    else next.add(id);
                                  return next;
                                });
                              }}
                            >
                              {hasCollapsedBranches
                                ? "Expand all"
                                : "Collapse all"}
                            </button>
                          </div>
                        </th>
                      </tr>
                      {isCollapsed
                        ? null
                        : visibleRows.map(
                            ({ device, depth, relationship, hasChildren }) => (
                              <tr
                                className={
                                  selectedDevices.has(device.entity.id)
                                    ? "selected-device-row"
                                    : undefined
                                }
                                key={device.entity.id}
                              >
                                <td className="device-select-column">
                                  <input
                                    aria-label={`Select device ${device.entity.id}`}
                                    checked={selectedDevices.has(
                                      device.entity.id,
                                    )}
                                    disabled={bulkRunning}
                                    type="checkbox"
                                    onChange={(event) => {
                                      toggleDeviceSelection(
                                        device.entity.id,
                                        event.target.checked,
                                      );
                                    }}
                                  />
                                </td>
                                <td>
                                  <div
                                    className="device-tree-cell"
                                    style={{
                                      paddingLeft: `${String(depth * 18)}px`,
                                    }}
                                  >
                                    {hasChildren ? (
                                      <button
                                        className="tree-toggle"
                                        aria-expanded={
                                          !collapsedDevices.has(
                                            device.entity.id,
                                          )
                                        }
                                        aria-label={`${collapsedDevices.has(device.entity.id) ? "Expand" : "Collapse"} ${device.entity.id}`}
                                        onClick={() => {
                                          setCollapsedDevices((current) => {
                                            const next = new Set(current);
                                            if (next.has(device.entity.id))
                                              next.delete(device.entity.id);
                                            else next.add(device.entity.id);
                                            return next;
                                          });
                                        }}
                                      >
                                        {collapsedDevices.has(device.entity.id)
                                          ? "▸"
                                          : "▾"}
                                      </button>
                                    ) : (
                                      <span className="tree-spacer" />
                                    )}
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
                                  {device.owner_name ?? device.owner ?? (
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
                            ),
                          )}
                    </tbody>
                  );
                })}
              </table>
            </div>
          )}
        </>
      )}
      <ConfirmDialog
        request={confirmRequest}
        onClose={() => {
          setConfirmRequest(null);
        }}
      />
    </article>
  );
}
