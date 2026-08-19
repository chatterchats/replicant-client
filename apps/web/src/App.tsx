import { useEffect, useMemo, useReducer, useState } from "react";

import {
  useActivity,
  useAutomationControl,
  useDaemonConnection,
  useDaemonHealth,
  useDaemonState,
  useEntities,
  useNotifications,
  useWorkflows,
} from "./daemon";
import { AutomationsPage } from "./AutomationsPage";
import { ActivityPage } from "./ActivityPage";
import { AmiReportsDrawer } from "./AmiReportsDrawer";
import { AutofactoryPage } from "./AutofactoryPage";
import { BlueprintsPage } from "./BlueprintsPage";
import { BobNetPage } from "./BobNetPage";
import { BootstrapPage } from "./BootstrapPage";
import { CargoPage } from "./CargoPage";
import { CloningPage } from "./CloningPage";
import { DeviceLogPanel } from "./DeviceLogPanel";
import { DirectoryPage } from "./DirectoryPage";
import {
  applicableDescriptorCommands,
  CommandPalette,
  type CommandContext,
  type DescriptorCommand,
} from "./CommandPalette";
import { GalaxyPage } from "./GalaxyPage";
import { HistoryPage } from "./HistoryPage";
import { InventoryPage } from "./InventoryPage";
import { LeaderboardsPage } from "./LeaderboardsPage";
import { MessagesPage } from "./MessagesPage";
import { MiningPage } from "./MiningPage";
import { ObservatoryPage } from "./ObservatoryPage";
import { OverviewPage } from "./OverviewPage";
import { DevicesPage } from "./DevicesPage";
import { EventsPage } from "./EventsPage";
import { RequirementsPage } from "./RequirementsPage";
import { RelayPage } from "./RelayPage";
import { ReportsPage } from "./ReportsPage";
import { SettingsPage } from "./SettingsPage";
import { SimulationsPage } from "./SimulationsPage";
import { AchievementsPage, ReputationPage } from "./StandingPage";
import { SystemPage } from "./SystemPage";
import { SurveyPage } from "./SurveyPage";
import { TradePage } from "./TradePage";
import { TutorialsPage } from "./TutorialsPage";
import { ConfirmDialog, type ConfirmRequest } from "./ConfirmDialog";
import { NotificationCenter, NotificationToasts } from "./Notifications";
import { absoluteTime, relativeTime } from "./time";
import { daemonApi } from "./api";
import type {
  DescriptorCatalog,
  CargoSnapshot,
  DeviceSummary,
  EntitySummary,
  EventSummary,
  FiniteExecution,
  GalaxyStar,
  InventorySnapshot,
  Notification,
  SystemMarker,
  WorkflowStatus,
  WorkflowSummary,
} from "./protocol";
import {
  initialShellState,
  routeFromHash,
  routeToHash,
  shellReducer,
  type SelectedEntity,
} from "./shellState";

const navigation = [
  ["Operations", ["Overview", "Galaxy", "System", "Observatory", "Cloning"]],
  ["Assets", ["Devices", "Inventory", "Autofactory", "Cargo", "Blueprints"]],
  [
    "Missions",
    [
      "Survey",
      "Mining",
      "Relay",
      "Galaxy Events",
      "Bootstrap",
      "Trade",
      "Simulations",
    ],
  ],
  ["Automation", ["Automations", "Requirements", "History"]],
  [
    "Intelligence",
    [
      "Activity",
      "Reports",
      "Messages",
      "BobNet",
      "Directory",
      "Achievements",
      "Species Reputation",
      "Leaderboards",
      "Tutorials",
    ],
  ],
] as const;

const navigationCommands = [
  ...navigation.flatMap(([, items]) => items),
  "Settings",
];

const NOTIFICATION_DISMISSALS_KEY = "replicant.notifications.dismissed.v1";
const MESSAGE_BADGE_REFRESH_MS = 60_000;

function canonicalPage(page: string) {
  if (page === "Network") return "Relay";
  if (page === "Standing" || page === "Reputation") return "Species Reputation";
  return page;
}

function loadDismissedNotificationIds(): Set<string> {
  try {
    const stored = window.localStorage.getItem(NOTIFICATION_DISMISSALS_KEY);
    if (!stored) return new Set();
    const parsed: unknown = JSON.parse(stored);
    return Array.isArray(parsed)
      ? new Set(
          parsed.filter((value): value is string => typeof value === "string"),
        )
      : new Set();
  } catch {
    return new Set();
  }
}

function persistDismissedNotificationIds(ids: Set<string>) {
  try {
    window.localStorage.setItem(
      NOTIFICATION_DISMISSALS_KEY,
      JSON.stringify([...ids]),
    );
  } catch {
    // Notification dismissal is a UI convenience; storage failure must never
    // break the application shell.
  }
}
const activeWorkflowStatuses: WorkflowStatus[] = [
  "queued",
  "running",
  "waiting",
  "paused",
  "reconciling",
];

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}

function textField(value: unknown, ...fields: string[]): string | null {
  const record = asRecord(value);
  for (const field of fields) {
    if (typeof record?.[field] === "string") return record[field];
  }
  return null;
}

function isEntitySummary(value: unknown): value is EntitySummary {
  const record = asRecord(value);
  return typeof record?.label === "string" && asRecord(record.entity) !== null;
}

function isDeviceSummary(value: unknown): value is DeviceSummary {
  const record = asRecord(value);
  return (
    asRecord(record?.entity)?.kind === "device" &&
    typeof record?.ownership === "string"
  );
}

function isWorkflowSummary(value: unknown): value is WorkflowSummary {
  const record = asRecord(value);
  return (
    typeof record?.id === "string" &&
    typeof record.kind === "string" &&
    typeof record.status === "string" &&
    typeof record.revision === "number"
  );
}

function isEventSummary(value: unknown): value is EventSummary {
  const record = asRecord(value);
  return (
    typeof record?.designation === "string" &&
    typeof record.title === "string" &&
    typeof record.system === "string" &&
    typeof record.location === "string"
  );
}

function isGalaxyStar(value: unknown): value is GalaxyStar {
  const record = asRecord(value);
  const position = asRecord(record?.position);
  return (
    typeof record?.id === "string" &&
    typeof position?.x === "number" &&
    typeof position.y === "number" &&
    typeof position.z === "number"
  );
}

function isSystemMarker(value: unknown): value is SystemMarker {
  const record = asRecord(value);
  return (
    typeof record?.id === "string" &&
    typeof record.label === "string" &&
    typeof record.location === "string" &&
    asRecord(record.entity) !== null
  );
}

function readableFieldName(value: string) {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function formatInspectorValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return value.toString();
  }
  if (typeof value === "symbol") return value.toString();
  if (typeof value === "function") return value.name || "function";
  return "—";
}

function StructuredDetails({ value }: { value: Record<string, unknown> }) {
  const entries = Object.entries(value).filter(
    ([, item]) => item !== null && item !== undefined,
  );
  if (!entries.length) return <p>No additional details.</p>;
  return (
    <dl>
      {entries.map(([key, item]) => (
        <div key={key} className="inspector-structured-row">
          <dt>{readableFieldName(key)}</dt>
          <dd>
            {Array.isArray(item)
              ? item
                  .map((entry) =>
                    typeof entry === "object" && entry !== null
                      ? JSON.stringify(entry)
                      : String(entry),
                  )
                  .join(", ") || "—"
              : typeof item === "object"
                ? Object.entries(item as Record<string, unknown>)
                    .map(
                      ([childKey, child]) =>
                        `${readableFieldName(childKey)}: ${String(child)}`,
                    )
                    .join(" · ")
                : formatInspectorValue(item)}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function aggregateInventory(
  inventory: InventorySnapshot | undefined,
  predicate: (row: InventorySnapshot["locations"][number]) => boolean,
) {
  const resources = new Map<string, number>();
  for (const row of inventory?.locations.filter(predicate) ?? []) {
    for (const item of row.resources) {
      resources.set(
        item.resource,
        (resources.get(item.resource) ?? 0) + item.quantity,
      );
    }
  }
  return [...resources.entries()]
    .map(([resource, quantity]) => ({ resource, quantity }))
    .sort(
      (left, right) =>
        right.quantity - left.quantity ||
        left.resource.localeCompare(right.resource),
    );
}

function commandFitsDevice(command: DescriptorCommand, device: DeviceSummary) {
  const kind = command.descriptor.kind;
  const deviceType = device.device_type?.toLowerCase() ?? "";
  const advertised = new Set(device.available_commands);
  const supports = (commandName: string) =>
    advertised.size === 0 || advertised.has(commandName);
  if (kind === "autofactory.print") {
    return deviceType === "autofactory" && supports("enqueue_print");
  }
  if (kind === "device.stow") return supports("stow");
  if (kind === "device.attach") return supports("attach");
  if (kind === "device.detach") return supports("detach");
  if (kind === "device.repair") return supports("repair");
  if (kind === "device.change_owner") return supports("change_owner");
  if (kind === "device.travel") return supports("travel");
  if (kind.startsWith("observatory."))
    return deviceType === "galactic_observatory";
  if (kind.startsWith("hub.")) return deviceType === "system_hub";
  if (kind.startsWith("trade.")) return deviceType.includes("trade_controller");
  if (kind.startsWith("simulation."))
    return deviceType.includes("replicant_interface");
  if (kind.startsWith("clone.")) return deviceType.includes("replicant_matrix");
  return true;
}

function specializeDeviceCommand(
  command: DescriptorCommand,
  device: DeviceSummary,
): DescriptorCommand {
  if (
    command.descriptor.kind !== "device.lifecycle" ||
    device.available_commands.length === 0
  ) {
    return command;
  }
  const supported = new Set(device.available_commands);
  const descriptor = {
    ...command.descriptor,
    parameters: command.descriptor.parameters.map((parameter) =>
      parameter.name === "command"
        ? {
            ...parameter,
            options: parameter.options.filter(
              (option) =>
                supported.has(option.value) ||
                (option.value === "retrieve" && device.stowed_in !== null),
            ),
          }
        : parameter,
    ),
  };
  return { ...command, descriptor };
}

function Inspector({
  entity,
  value,
  descriptors,
  entities,
  onClose,
  onClear,
  onOpenGalaxy,
  onOpenSystem,
  onOpenWorkflow,
  onRunCommand,
}: {
  entity: SelectedEntity;
  value: unknown;
  descriptors: DescriptorCatalog;
  entities: Record<string, EntitySummary>;
  onClose: () => void;
  onClear: () => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onOpenWorkflow: (workflowId: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [inventory, setInventory] = useState<InventorySnapshot>();
  const [cargo, setCargo] = useState<CargoSnapshot>();
  const [detailError, setDetailError] = useState<string | null>(null);
  const device = isDeviceSummary(value) ? value : undefined;
  const workflow = !device && isWorkflowSummary(value) ? value : undefined;
  const event =
    !device && !workflow && isEventSummary(value) ? value : undefined;
  const summary =
    !device && !workflow && !event && isEntitySummary(value)
      ? value
      : undefined;
  const galaxyStar = isGalaxyStar(value) ? value : undefined;
  const marker = isSystemMarker(value) ? value : undefined;
  const deviceActions = useMemo(
    () =>
      device
        ? applicableDescriptorCommands(descriptors, "device")
            .filter(
              (command) =>
                command.operationClass === "action" &&
                commandFitsDevice(command, device),
            )
            .map((command) => specializeDeviceCommand(command, device))
            .filter(
              (command) =>
                command.descriptor.kind !== "device.lifecycle" ||
                command.descriptor.parameters.some(
                  (parameter) =>
                    parameter.name === "command" &&
                    parameter.options.length > 0,
                ),
            )
        : [],
    [descriptors, device],
  );
  useEffect(() => {
    setInventory(undefined);
    setCargo(undefined);
    setDetailError(null);
    const controller = new AbortController();
    const tasks: Promise<void>[] = [];
    if (["system", "location", "resource"].includes(entity.kind)) {
      tasks.push(
        daemonApi
          .inventory(controller.signal)
          .then(setInventory)
          .catch((error: unknown) => {
            if (!controller.signal.aborted) setDetailError(String(error));
          }),
      );
    }
    if (entity.kind === "device") {
      tasks.push(
        daemonApi
          .cargo(controller.signal)
          .then(setCargo)
          .catch((error: unknown) => {
            if (!controller.signal.aborted) setDetailError(String(error));
          }),
      );
    }
    void Promise.all(tasks);
    return () => {
      controller.abort();
    };
  }, [entity.id, entity.kind]);

  const relatedDevice = (code: string) => {
    const related = entities[`device:${code}`];
    return related ? `${related.label} (${code})` : code;
  };
  const systemResources =
    entity.kind === "system"
      ? aggregateInventory(inventory, (row) => row.system === entity.id)
      : [];
  const locationResources =
    entity.kind === "location"
      ? aggregateInventory(inventory, (row) => row.location === entity.id)
      : [];
  const resourceDistribution =
    entity.kind === "resource"
      ? inventory?.resources.find((item) => item.resource === entity.id)
      : undefined;
  const carrier = device
    ? cargo?.carriers.find((item) => item.device.entity.id === device.entity.id)
    : undefined;
  const containedEntities =
    entity.kind === "system" || entity.kind === "location"
      ? Object.values(entities)
          .filter((item) =>
            entity.kind === "system"
              ? item.system === entity.id
              : item.location === entity.id,
          )
          .filter(
            (item) =>
              item.entity.kind === "device" || item.entity.kind === "replicant",
          )
          .sort((left, right) => left.label.localeCompare(right.label))
      : [];
  const targetSystem =
    entity.kind === "system"
      ? entity.id
      : (event?.system ?? device?.system ?? summary?.system ?? null);
  return (
    <aside className="inspector" aria-label="Selected entity inspector">
      <header className="drawer-header">
        <div>
          <small>{entity.kind}</small>
          <strong>
            {summary?.label ?? galaxyStar?.name ?? marker?.label ?? entity.id}
          </strong>
        </div>
        <button aria-label="Close inspector" onClick={onClose}>
          ×
        </button>
      </header>
      <div className="inspector-body">
        {device ? (
          <dl>
            <dt>Type</dt>
            <dd>{device.device_type ?? "Unknown"}</dd>
            <dt>Status</dt>
            <dd>{device.status ?? "Unknown"}</dd>
            <dt>Ownership</dt>
            <dd>{device.owner_name ?? device.owner ?? device.ownership}</dd>
            {device.system && (
              <>
                <dt>System</dt>
                <dd>{device.system}</dd>
              </>
            )}
            {device.location && (
              <>
                <dt>Location</dt>
                <dd>{device.location}</dd>
              </>
            )}
            {device.tags.length > 0 && (
              <>
                <dt>Tags</dt>
                <dd>{device.tags.join(", ")}</dd>
              </>
            )}
            {(device.attached_to || device.stowed_in || device.controller) && (
              <>
                <dt>Relationship</dt>
                <dd>
                  {device.attached_to
                    ? `Attached to ${device.attached_to}`
                    : device.stowed_in
                      ? `Stowed in ${device.stowed_in}`
                      : `Controlled by ${device.controller ?? "—"}`}
                </dd>
              </>
            )}
            {device.controlled_devices.length > 0 && (
              <>
                <dt>Controlled devices</dt>
                <dd>
                  {device.controlled_devices.map((code) => (
                    <div key={code}>{relatedDevice(code)}</div>
                  ))}
                </dd>
              </>
            )}
            {device.attached_devices.length > 0 && (
              <>
                <dt>Attached devices</dt>
                <dd>
                  {device.attached_devices.map((code) => (
                    <div key={code}>{relatedDevice(code)}</div>
                  ))}
                </dd>
              </>
            )}
            {device.stowed_devices.length > 0 && (
              <>
                <dt>Stowed devices</dt>
                <dd>
                  {device.stowed_devices.map((code) => (
                    <div key={code}>{relatedDevice(code)}</div>
                  ))}
                </dd>
              </>
            )}
            {device.operational_capacity_percent !== null && (
              <>
                <dt>Operational</dt>
                <dd>{device.operational_capacity_percent.toFixed(0)}%</dd>
              </>
            )}
            {device.cargo_capacity !== null && (
              <>
                <dt>Cargo</dt>
                <dd>
                  {device.cargo_used ?? 0} / {device.cargo_capacity}
                </dd>
              </>
            )}
            {device.attach_capacity !== null && (
              <>
                <dt>Attach points</dt>
                <dd>
                  {device.attached_devices.length} / {device.attach_capacity}
                  {device.attach_capacity > device.attached_devices.length
                    ? ` · ${String(device.attach_capacity - device.attached_devices.length)} free`
                    : ""}
                </dd>
              </>
            )}
            {device.active_directive && (
              <>
                <dt>Directive</dt>
                <dd>
                  {device.active_directive}
                  {device.directive_status
                    ? ` · ${device.directive_status}`
                    : ""}
                </dd>
              </>
            )}
            {device.travel_destination && (
              <>
                <dt>Traveling to</dt>
                <dd>{device.travel_destination}</dd>
              </>
            )}
            {carrier?.resources.length ? (
              <>
                <dt>Stowed cargo</dt>
                <dd>
                  {carrier.resources.map((item) => (
                    <div key={item.resource}>
                      {item.resource}: {item.quantity.toLocaleString()}
                    </div>
                  ))}
                </dd>
              </>
            ) : null}
          </dl>
        ) : workflow ? (
          <dl>
            <dt>Kind</dt>
            <dd>{workflow.kind}</dd>
            <dt>Status</dt>
            <dd>
              <span className="status-chip">{workflow.status}</span>
            </dd>
            {workflow.current_step && (
              <>
                <dt>Step</dt>
                <dd>{workflow.current_step}</dd>
              </>
            )}
            <dt>Revision</dt>
            <dd>{workflow.revision}</dd>
            <dt>Updated</dt>
            <dd>{new Date(workflow.updated_at_ms).toLocaleString()}</dd>
          </dl>
        ) : event ? (
          <dl>
            <dt>Title</dt>
            <dd>{event.title}</dd>
            <dt>Status</dt>
            <dd>
              <span className="status-chip">{event.status ?? "unknown"}</span>
            </dd>
            <dt>Type</dt>
            <dd>
              {[event.event_type, event.category, event.tier]
                .filter((value) => value !== null)
                .join(" · ") || "Unclassified"}
            </dd>
            <dt>System</dt>
            <dd>{event.system}</dd>
            <dt>Location</dt>
            <dd>{event.location}</dd>
            {event.description && (
              <>
                <dt>Description</dt>
                <dd>{event.description}</dd>
              </>
            )}
            {event.criteria.length > 0 && (
              <>
                <dt>Progress</dt>
                <dd>
                  {event.criteria.map((criterion) => (
                    <div key={criterion.name}>
                      {criterion.name} ·{" "}
                      {criterion.complete ? "complete" : "active"}
                    </div>
                  ))}
                </dd>
              </>
            )}
            {(event.rewards.resources.length > 0 ||
              event.rewards.devices.length > 0 ||
              event.rewards.xp !== null) && (
              <>
                <dt>Rewards</dt>
                <dd>
                  {[...event.rewards.resources, ...event.rewards.devices].map(
                    (reward) => (
                      <div key={reward.item}>
                        {reward.quantity} {reward.item}
                      </div>
                    ),
                  )}
                  {event.rewards.xp !== null && (
                    <div>{event.rewards.xp} XP</div>
                  )}
                </dd>
              </>
            )}
          </dl>
        ) : galaxyStar ? (
          <dl>
            <dt>Exploration</dt>
            <dd>{galaxyStar.exploration}</dd>
            <dt>Spectral type</dt>
            <dd>{galaxyStar.spectral_type ?? "Unknown"}</dd>
            <dt>Coordinates</dt>
            <dd>
              {galaxyStar.position.x.toFixed(2)},{" "}
              {galaxyStar.position.y.toFixed(2)},{" "}
              {galaxyStar.position.z.toFixed(2)} LY
            </dd>
            <dt>Infrastructure</dt>
            <dd>
              {[
                galaxyStar.has_hub ? "System Hub" : null,
                galaxyStar.has_relay ? "Relay" : null,
                galaxyStar.has_megastructure ? "Megastructure" : null,
              ]
                .filter(Boolean)
                .join(" · ") || "None known"}
            </dd>
            <dt>Life</dt>
            <dd>{galaxyStar.has_life ? "Detected" : "None known"}</dd>
          </dl>
        ) : marker ? (
          <dl>
            <dt>Kind</dt>
            <dd>{marker.kind}</dd>
            <dt>Location</dt>
            <dd>{marker.location}</dd>
            {marker.parent ? (
              <>
                <dt>Parent</dt>
                <dd>{marker.parent}</dd>
              </>
            ) : null}
            <dt>Map position</dt>
            <dd>
              {marker.position.x.toFixed(2)}, {marker.position.y.toFixed(2)}
            </dd>
            {marker.in_habitable_zone !== null ? (
              <>
                <dt>Habitable zone</dt>
                <dd>{marker.in_habitable_zone ? "Yes" : "No"}</dd>
              </>
            ) : null}
          </dl>
        ) : summary ? (
          <dl>
            {summary.secondary_label && (
              <>
                <dt>Type</dt>
                <dd>{summary.secondary_label}</dd>
              </>
            )}
            {summary.status && (
              <>
                <dt>Status</dt>
                <dd>{summary.status}</dd>
              </>
            )}
            {summary.system && (
              <>
                <dt>System</dt>
                <dd>{summary.system}</dd>
              </>
            )}
            {summary.location && (
              <>
                <dt>Location</dt>
                <dd>{summary.location}</dd>
              </>
            )}
          </dl>
        ) : value === undefined ? (
          <p>This entity is not present in the current daemon projection.</p>
        ) : (
          <StructuredDetails value={asRecord(value) ?? { value }} />
        )}
        {entity.kind === "system" && inventory ? (
          <section className="inspector-section">
            <h3>Combined system resources</h3>
            {systemResources.length ? (
              <ul className="inspector-resource-list">
                {systemResources.map((item) => (
                  <li key={item.resource}>
                    <span>{item.resource}</span>
                    <strong>{item.quantity.toLocaleString()}</strong>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="empty-state">No stored resources in this system.</p>
            )}
          </section>
        ) : null}
        {entity.kind === "location" && inventory ? (
          <section className="inspector-section">
            <h3>Location resources</h3>
            {locationResources.length ? (
              <ul className="inspector-resource-list">
                {locationResources.map((item) => (
                  <li key={item.resource}>
                    <span>{item.resource}</span>
                    <strong>{item.quantity.toLocaleString()}</strong>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="empty-state">
                No stored resources at this location.
              </p>
            )}
          </section>
        ) : null}
        {containedEntities.length ? (
          <section className="inspector-section">
            <h3>Present assets</h3>
            <ul className="inspector-entity-list">
              {containedEntities.slice(0, 50).map((item) => (
                <li key={`${item.entity.kind}:${item.entity.id}`}>
                  <span>
                    <strong>{item.label}</strong>
                    <small>{item.secondary_label ?? item.entity.kind}</small>
                  </span>
                  <span className="status-chip">
                    {item.status ?? "present"}
                  </span>
                </li>
              ))}
            </ul>
            {containedEntities.length > 50 ? (
              <small>
                Showing 50 of {containedEntities.length} present assets.
              </small>
            ) : null}
          </section>
        ) : null}
        {resourceDistribution ? (
          <section className="inspector-section">
            <h3>{resourceDistribution.resource}</h3>
            <p>{resourceDistribution.total_quantity.toLocaleString()} total</p>
            <ul className="inspector-resource-list">
              {resourceDistribution.distribution.map((item, index) => (
                <li
                  key={`${item.owner}:${item.location ?? "none"}:${String(index)}`}
                >
                  <span>{item.location ?? item.owner}</span>
                  <strong>{item.quantity.toLocaleString()}</strong>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        {detailError ? (
          <p className="inline-warning">Detail refresh failed: {detailError}</p>
        ) : null}
        {device && deviceActions.length ? (
          <section className="inspector-section">
            <h3>Actions</h3>
            <div className="inspector-command-grid">
              {deviceActions.map((command) => (
                <button
                  key={`${command.operationClass}:${command.descriptor.kind}`}
                  onClick={() => {
                    onRunCommand(command);
                  }}
                >
                  {command.descriptor.display_name}
                </button>
              ))}
            </div>
          </section>
        ) : null}
        {device ? <DeviceLogPanel device={device.entity.id} /> : null}
      </div>
      {targetSystem || entity.kind === "workflow" ? (
        <div className="inspector-actions">
          {targetSystem && (
            <>
              <button
                onClick={() => {
                  onOpenGalaxy(targetSystem);
                }}
              >
                Show on Galaxy
              </button>
              <button
                onClick={() => {
                  onOpenSystem(targetSystem);
                }}
              >
                Show on System
              </button>
            </>
          )}
          {entity.kind === "workflow" && (
            <button
              onClick={() => {
                onOpenWorkflow(entity.id);
              }}
            >
              Open in Automation
            </button>
          )}
        </div>
      ) : null}
      <button className="clear-selection" onClick={onClear}>
        Clear selection
      </button>
    </aside>
  );
}

export function App() {
  const [shell, dispatch] = useReducer(shellReducer, initialShellState, () => {
    const route = routeFromHash(window.location.hash, {
      page: initialShellState.page,
      entity: null,
    });
    return {
      ...initialShellState,
      page: canonicalPage(route.page),
      selectedEntity: route.entity,
      inspectorOpen: route.entity !== null,
    };
  });
  const [descriptors, setDescriptors] = useState<DescriptorCatalog>({
    reports: [],
    actions: [],
    workflows: [],
  });
  const [selectedGalaxyStar, setSelectedGalaxyStar] = useState<GalaxyStar>();
  const [selectedExecution, setSelectedExecution] = useState<FiniteExecution>();
  const [selectedSystem, setSelectedSystem] = useState<string>();
  const [selectedSystemMarker, setSelectedSystemMarker] =
    useState<SystemMarker>();
  const [selectedDevice, setSelectedDevice] = useState<DeviceSummary>();
  const [selectedEvent, setSelectedEvent] = useState<EventSummary>();
  const [galaxyCommand, setGalaxyCommand] = useState<DescriptorCommand>();
  const [selectedAutomationWorkflow, setSelectedAutomationWorkflow] =
    useState<string>();
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(
    null,
  );
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [dismissedNotificationIds, setDismissedNotificationIds] = useState(
    loadDismissedNotificationIds,
  );
  const [unreadMessageCount, setUnreadMessageCount] = useState(0);
  const [activityTab, setActivityTab] = useState<"workflow" | "ami">(
    "workflow",
  );
  // The sidebar is hidden at narrow widths; this opens it as a sheet so
  // navigation does not depend on the keyboard-only command palette.
  const [navOpen, setNavOpen] = useState(false);
  const [commandResult, setCommandResult] = useState<{
    message: string;
    actionLabel: string;
    onAction: () => void;
  } | null>(null);
  const {
    busy: automationBusy,
    error: automationError,
    control: controlAutomation,
  } = useAutomationControl();
  const daemon = useDaemonState();
  const health = useDaemonHealth();
  const { connection, syncing, revision } = useDaemonConnection();
  const entities = useEntities();
  const workflows = useWorkflows();
  const activity = useActivity();
  const rawNotifications = useNotifications();
  const notifications = useMemo(
    () =>
      rawNotifications.filter(
        (notification) => !dismissedNotificationIds.has(notification.id),
      ),
    [dismissedNotificationIds, rawNotifications],
  );

  useEffect(() => {
    const controller = new AbortController();
    void daemonApi
      .descriptors(controller.signal)
      .then(setDescriptors)
      .catch(() => undefined);
    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (revision === null) return;
    setDismissedNotificationIds((current) => {
      const activeIds = new Set(rawNotifications.map((item) => item.id));
      const next = new Set([...current].filter((id) => activeIds.has(id)));
      if (next.size === current.size) return current;
      persistDismissedNotificationIds(next);
      return next;
    });
  }, [rawNotifications, revision]);

  useEffect(() => {
    if (connection !== "connected") return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;
    const syncUnread = async () => {
      controller = new AbortController();
      try {
        const snapshot = await daemonApi.messages(controller.signal);
        if (!cancelled && typeof snapshot.unread_count === "number")
          setUnreadMessageCount(snapshot.unread_count);
      } catch {
        // The badge is supplemental. The Messages page will surface an error
        // if the inbox itself cannot be refreshed.
      } finally {
        controller = undefined;
        if (!cancelled)
          timer = setTimeout(syncUnread, MESSAGE_BADGE_REFRESH_MS);
      }
    };
    void syncUnread();
    return () => {
      cancelled = true;
      controller?.abort();
      if (timer !== undefined) clearTimeout(timer);
    };
  }, [connection]);

  // Page and selection are mirrored into the location hash so a refresh keeps
  // the current view and every page is linkable.
  useEffect(() => {
    const hash = routeToHash({
      page: shell.page,
      entity: shell.selectedEntity,
    });
    if (window.location.hash !== hash) window.history.pushState(null, "", hash);
  }, [shell.page, shell.selectedEntity]);

  useEffect(() => {
    const onPopState = () => {
      const route = routeFromHash(window.location.hash, {
        page: initialShellState.page,
        entity: null,
      });
      dispatch({
        type: "restore",
        route: { ...route, page: canonicalPage(route.page) },
      });
    };
    window.addEventListener("popstate", onPopState);
    window.addEventListener("hashchange", onPopState);
    return () => {
      window.removeEventListener("popstate", onPopState);
      window.removeEventListener("hashchange", onPopState);
    };
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        dispatch({ type: "set_palette", open: !shell.paletteOpen });
      }
      if (event.key === "Escape") {
        if (shell.paletteOpen) dispatch({ type: "set_palette", open: false });
        else if (shell.inspectorOpen) dispatch({ type: "toggle_inspector" });
        else if (shell.activityOpen) dispatch({ type: "toggle_activity" });
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [shell.activityOpen, shell.inspectorOpen, shell.paletteOpen]);

  const entityList = useMemo(
    () => Object.values(entities).map((summary) => summary.entity),
    [entities],
  );
  const currentReplicant = entityList.find(
    (entity) => entity.kind === "replicant",
  );
  const currentReplicantValue = currentReplicant
    ? entities[`replicant:${currentReplicant.id}`]
    : undefined;
  const currentLocation = currentReplicantValue?.location ?? null;
  const currentSystem = currentReplicantValue?.system ?? null;
  const activeWorkflows = workflows.filter((workflow) =>
    activeWorkflowStatuses.includes(workflow.status),
  );
  const warnings = notifications.filter(
    (notification) => notification.level !== "info",
  );
  const status =
    connection === "connected" ? (health?.status ?? "healthy") : connection;
  const selectedValue = shell.selectedEntity
    ? shell.selectedEntity.kind === "workflow"
      ? daemon.workflows[shell.selectedEntity.id]
      : shell.selectedEntity.kind === "device"
        ? selectedDevice?.entity.id === shell.selectedEntity.id
          ? selectedDevice
          : entities[`device:${shell.selectedEntity.id}`]
        : shell.selectedEntity.kind === "event" &&
            selectedEvent?.designation === shell.selectedEntity.id
          ? selectedEvent
          : shell.selectedEntity.kind === "system" &&
              selectedGalaxyStar?.id === shell.selectedEntity.id
            ? selectedGalaxyStar
            : selectedSystemMarker?.entity.kind === shell.selectedEntity.kind &&
                selectedSystemMarker.entity.id === shell.selectedEntity.id
              ? selectedSystemMarker
              : entities[
                  `${shell.selectedEntity.kind}:${shell.selectedEntity.id}`
                ]
    : undefined;
  const commandContext: CommandContext = {
    system:
      (shell.selectedEntity?.kind === "system"
        ? shell.selectedEntity.id
        : null) ??
      textField(selectedValue, "system", "system_code") ??
      selectedSystem ??
      currentSystem ??
      undefined,
    location:
      (shell.selectedEntity?.kind === "location"
        ? shell.selectedEntity.id
        : null) ??
      textField(selectedValue, "location", "location_code") ??
      currentLocation ??
      undefined,
    device:
      shell.selectedEntity?.kind === "device"
        ? shell.selectedEntity.id
        : undefined,
    replicant:
      (shell.selectedEntity?.kind === "replicant"
        ? shell.selectedEntity.id
        : null) ??
      textField(selectedValue, "replicant", "replicant_id", "owner") ??
      currentReplicant?.id,
  };
  const group =
    navigation.find(([, items]) =>
      (items as readonly string[]).includes(shell.page),
    )?.[0] ?? "Settings";

  const navigate = (destination: string) => {
    dispatch({ type: "navigate", page: destination });
    setNavOpen(false);
  };
  const select = (entity: SelectedEntity) => {
    if (entity.kind !== "device") setSelectedDevice(undefined);
    if (entity.kind !== "event") setSelectedEvent(undefined);
    dispatch({ type: "select", entity });
  };
  const openSystem = (system: string) => {
    setSelectedSystem(system);
    select({ kind: "system", id: system });
    navigate("System");
  };
  const openGalaxy = (system: string) => {
    setSelectedSystem(system);
    select({ kind: "system", id: system });
    navigate("Galaxy");
  };
  const openWorkflow = (workflowId: string) => {
    setSelectedAutomationWorkflow(workflowId);
    navigate("Automations");
  };
  const openNotification = (notification: Notification) => {
    const workflowMatch = /^workflow:([^:]+):attention$/.exec(notification.id);
    if (workflowMatch?.[1]) {
      openWorkflow(workflowMatch[1]);
      return;
    }
    navigate("History");
  };

  return (
    <div className={`app-shell ${navOpen ? "nav-open" : ""}`}>
      <aside className="sidebar">
        <header className="brand">
          <span className="brand-mark">RS</span>
          <span>
            <strong>Replicant Space</strong>
            <small>Application console</small>
          </span>
        </header>
        <nav aria-label="Primary navigation">
          {navigation.map(([navGroup, items]) => (
            <section key={navGroup}>
              <h2>{navGroup}</h2>
              {items.map((item) => (
                <button
                  className={shell.page === item ? "active" : ""}
                  key={item}
                  onClick={() => {
                    navigate(item);
                  }}
                >
                  <span>{item}</span>
                  {item === "Messages" && unreadMessageCount > 0 && (
                    <span
                      className="nav-unread-badge"
                      aria-label={`${String(unreadMessageCount)} unread messages`}
                    >
                      {unreadMessageCount > 99 ? "99+" : unreadMessageCount}
                    </span>
                  )}
                </button>
              ))}
            </section>
          ))}
        </nav>
        <button
          className={shell.page === "Settings" ? "active settings" : "settings"}
          onClick={() => {
            navigate("Settings");
          }}
        >
          Settings
        </button>
      </aside>

      <main>
        <header className="status-bar">
          <button
            aria-expanded={navOpen}
            aria-label="Toggle navigation"
            className="nav-toggle"
            onClick={() => {
              setNavOpen((open) => !open);
            }}
          >
            ☰
          </button>
          <button
            className="status-item identity"
            disabled={!currentReplicant}
            onClick={() => {
              if (currentReplicant) select(currentReplicant);
            }}
          >
            <small>Replicant</small>
            <strong>{currentReplicant?.id ?? "Not selected"}</strong>
          </button>
          <span className="status-item">
            <small>Location / system</small>
            <strong>{currentLocation ?? currentSystem ?? "Unknown"}</strong>
          </span>
          <span className="status-item sync-status">
            <small>Daemon / managed sync</small>
            <strong>
              <span className={`status-dot ${status}`} aria-hidden="true" />
              {status} · {daemon.sync?.phase ?? (syncing ? "syncing" : "ready")}
            </strong>
          </span>
          <button
            className="status-item"
            onClick={() => {
              navigate("Automations");
            }}
          >
            <small>Active workflows</small>
            <strong>{activeWorkflows.length}</strong>
          </button>
          <button
            aria-expanded={notificationsOpen}
            className={`status-item notifications-trigger ${warnings.length ? "warning" : ""}`}
            onClick={() => {
              setNotificationsOpen((open) => !open);
            }}
          >
            <small>Notifications</small>
            <strong>
              {warnings.length > 0
                ? `${String(warnings.length)} need attention`
                : `${String(notifications.length)} total`}
            </strong>
          </button>
          <div
            className={`status-item automation-safety ${daemon.automation.workflows_paused ? "paused" : ""}`}
            title={automationError}
          >
            <small>Automation safety</small>
            <strong>
              {daemon.automation.workflows_paused ? "Paused" : "Running"} ·
              triggers{" "}
              {daemon.automation.automatic_triggers_enabled ? "on" : "off"}
            </strong>
            <span>
              <button
                disabled={connection !== "connected" || automationBusy}
                onClick={() => {
                  controlAutomation(
                    daemon.automation.workflows_paused
                      ? "resume_all"
                      : "pause_all",
                  );
                }}
              >
                {daemon.automation.workflows_paused ? "Resume" : "Pause all"}
              </button>
              <button
                disabled={connection !== "connected" || automationBusy}
                onClick={() => {
                  controlAutomation(
                    daemon.automation.automatic_triggers_enabled
                      ? "disable_triggers"
                      : "enable_triggers",
                  );
                }}
              >
                Triggers{" "}
                {daemon.automation.automatic_triggers_enabled ? "off" : "on"}
              </button>
              <button
                className="danger"
                disabled={
                  connection !== "connected" ||
                  automationBusy ||
                  activeWorkflows.length === 0
                }
                onClick={() => {
                  setConfirmRequest({
                    title: "Cancel every eligible workflow?",
                    message: `This cancels ${String(activeWorkflows.length)} running or queued workflow(s) and cannot be undone. Work already committed upstream is not rolled back.`,
                    items: activeWorkflows.map(
                      (workflow) =>
                        `${workflow.kind} · ${workflow.id.slice(0, 8)} (${workflow.status})`,
                    ),
                    confirmLabel: "Cancel all workflows",
                    requireTyped: "cancel all",
                    destructive: true,
                    onConfirm: () => {
                      controlAutomation("cancel");
                    },
                  });
                }}
              >
                Cancel all
              </button>
            </span>
          </div>
          <button
            className="palette-trigger"
            onClick={() => {
              setGalaxyCommand(undefined);
              dispatch({ type: "set_palette", open: true });
            }}
          >
            Commands <kbd>⌘K</kbd>
          </button>
        </header>

        <div className="workspace">
          <div className="content-column">
            {shell.page === "Overview" ? (
              <OverviewPage
                onNavigate={navigate}
                onSelectEntity={select}
                onSelectWorkflow={openWorkflow}
                onOpenSystem={openSystem}
              />
            ) : shell.page === "Devices" ? (
              <DevicesPage
                descriptors={descriptors}
                onSelectDevice={(device) => {
                  setSelectedDevice(device);
                  select(device.entity);
                }}
                onSelectEntity={select}
                onOpenSystem={openSystem}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Inventory" ? (
              <InventoryPage
                onSelectEntity={select}
                onOpenSystem={openSystem}
              />
            ) : shell.page === "Autofactory" ? (
              <AutofactoryPage
                descriptors={descriptors}
                onSelectDevice={(device) => {
                  setSelectedDevice(device);
                  select(device.entity);
                }}
                onSelectEntity={select}
                onOpenSystem={openSystem}
                onSelectWorkflow={openWorkflow}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Cargo" ? (
              <CargoPage
                descriptors={descriptors}
                onSelectDevice={(device) => {
                  setSelectedDevice(device);
                  select(device.entity);
                }}
                onSelectEntity={select}
                onOpenSystem={openSystem}
                onSelectWorkflow={openWorkflow}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Survey" ? (
              <SurveyPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={openGalaxy}
                onSelectWorkflow={openWorkflow}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Mining" ? (
              <MiningPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={openGalaxy}
                onSelectWorkflow={openWorkflow}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Relay" || shell.page === "Network" ? (
              <RelayPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={openGalaxy}
                onSelectWorkflow={openWorkflow}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Bootstrap" ? (
              <BootstrapPage
                descriptors={descriptors}
                onOpenGalaxy={openGalaxy}
                onOpenHistory={() => {
                  navigate("History");
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Galaxy Events" || shell.page === "Events" ? (
              <EventsPage
                descriptors={descriptors}
                onSelectEvent={(event) => {
                  setSelectedEvent(event);
                  select({ kind: "event", id: event.designation });
                }}
                onOpenGalaxy={openGalaxy}
                onOpenSystem={openSystem}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Trade" ? (
              <TradePage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenSystem={openSystem}
                onSelectWorkflow={openWorkflow}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Automations" ? (
              <AutomationsPage
                entities={entities}
                workflows={workflows}
                selectedWorkflowId={selectedAutomationWorkflow}
                onSelectedWorkflowConsumed={() => {
                  setSelectedAutomationWorkflow(undefined);
                }}
              />
            ) : shell.page === "Requirements" ? (
              <RequirementsPage
                requirements={daemon.requirements}
                onSelectWorkflow={openWorkflow}
              />
            ) : shell.page === "History" ? (
              <HistoryPage
                workflows={workflows}
                selectedExecution={selectedExecution}
                onSelectWorkflow={openWorkflow}
                onSelectEntity={select}
              />
            ) : shell.page === "Reports" ? (
              <ReportsPage entities={entities} onSelectEntity={select} />
            ) : shell.page === "Messages" ? (
              <MessagesPage onUnreadCountChange={setUnreadMessageCount} />
            ) : shell.page === "BobNet" ? (
              <BobNetPage onSelectEntity={select} />
            ) : shell.page === "Achievements" ? (
              <AchievementsPage />
            ) : shell.page === "Species Reputation" ||
              shell.page === "Reputation" ||
              shell.page === "Standing" ? (
              <ReputationPage />
            ) : shell.page === "Leaderboards" ? (
              <LeaderboardsPage onSelectEntity={select} />
            ) : shell.page === "Galaxy" ? (
              <GalaxyPage
                descriptors={descriptors}
                onSelectStar={(star) => {
                  setSelectedGalaxyStar(star);
                  setSelectedSystem(star.id);
                  select({ kind: "system", id: star.id });
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
                onSelectWorkflow={openWorkflow}
                onOpenSystem={(star) => {
                  setSelectedGalaxyStar(star);
                  setSelectedSystem(star.id);
                  select({ kind: "system", id: star.id });
                  navigate("System");
                }}
              />
            ) : shell.page === "System" ? (
              <SystemPage
                system={selectedSystem ?? currentSystem ?? undefined}
                descriptors={descriptors}
                onSelectMarker={(marker) => {
                  setSelectedSystemMarker(marker);
                  select(marker.entity);
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
                onOpenGalaxy={() => {
                  navigate("Galaxy");
                }}
                onSelectEntity={select}
              />
            ) : shell.page === "Observatory" ? (
              <ObservatoryPage
                descriptors={descriptors}
                onSelectEntity={select}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Cloning" ? (
              <CloningPage
                descriptors={descriptors}
                onSelectEntity={select}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Blueprints" ? (
              <BlueprintsPage />
            ) : shell.page === "Activity" ? (
              <ActivityPage onSelectEntity={select} />
            ) : shell.page === "Directory" ? (
              <DirectoryPage />
            ) : shell.page === "Tutorials" ? (
              <TutorialsPage />
            ) : shell.page === "Simulations" ? (
              <SimulationsPage
                descriptors={descriptors}
                onSelectEntity={select}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
                onOpenLeaderboards={() => {
                  navigate("Leaderboards");
                }}
              />
            ) : shell.page === "Settings" ? (
              <SettingsPage />
            ) : (
              <article className="page">
                <p className="eyebrow">{group}</p>
                <h1>{shell.page}</h1>
                <p className="lede">
                  Live application state is synchronized through the local
                  daemon.
                </p>
                <section className="connection-card">
                  <span className={`status-dot ${status}`} aria-hidden="true" />
                  <div>
                    <strong>Daemon connection</strong>
                    <p>
                      {health?.detail ??
                        (connection === "offline"
                          ? "Start replicantd to connect."
                          : syncing
                            ? "Synchronizing daemon state…"
                            : "Daemon state is current.")}
                    </p>
                    {revision === null ? null : (
                      <small>Revision {revision}</small>
                    )}
                  </div>
                </section>
              </article>
            )}
          </div>

          {shell.inspectorOpen && shell.selectedEntity ? (
            <Inspector
              entity={shell.selectedEntity}
              value={selectedValue}
              descriptors={descriptors}
              entities={entities}
              onClose={() => {
                dispatch({ type: "toggle_inspector" });
              }}
              onClear={() => {
                dispatch({ type: "clear_selection" });
              }}
              onOpenGalaxy={openGalaxy}
              onOpenSystem={openSystem}
              onOpenWorkflow={openWorkflow}
              onRunCommand={(command) => {
                setGalaxyCommand(command);
                dispatch({ type: "set_palette", open: true });
              }}
            />
          ) : null}
        </div>

        <section
          className={`activity-drawer ${shell.activityOpen ? "open" : ""}`}
          aria-label="Activity"
        >
          <button
            className="activity-toggle"
            aria-expanded={shell.activityOpen}
            onClick={() => {
              dispatch({ type: "toggle_activity" });
            }}
          >
            <span>Activity</span>
            <span>{activity.length} updates</span>
            <span aria-hidden="true">{shell.activityOpen ? "⌄" : "⌃"}</span>
          </button>
          {shell.activityOpen ? (
            <div className="activity-drawer-content">
              <div
                className="activity-drawer-tabs"
                role="tablist"
                aria-label="Activity views"
              >
                <button
                  role="tab"
                  aria-selected={activityTab === "workflow"}
                  onClick={() => {
                    setActivityTab("workflow");
                  }}
                >
                  Workflow activity
                </button>
                <button
                  role="tab"
                  aria-selected={activityTab === "ami"}
                  onClick={() => {
                    setActivityTab("ami");
                  }}
                >
                  AMI reports
                </button>
              </div>
              {activityTab === "workflow" ? (
                <div className="activity-list">
                  {activity.length ? (
                    activity
                      .slice()
                      .reverse()
                      .map((item) => (
                        <button
                          className={`activity-item ${item.level}`}
                          key={item.id}
                          onClick={() => {
                            select({ kind: "workflow", id: item.workflow_id });
                          }}
                        >
                          <time
                            dateTime={new Date(
                              item.occurred_at_ms,
                            ).toISOString()}
                            title={absoluteTime(item.occurred_at_ms)}
                          >
                            {relativeTime(item.occurred_at_ms)}
                          </time>
                          <strong>
                            {daemon.workflows[item.workflow_id]?.kind ??
                              "workflow"}{" "}
                            · {item.workflow_id.slice(0, 8)}
                          </strong>
                          <span>{item.step ?? item.level}</span>
                          <p>{item.message}</p>
                        </button>
                      ))
                  ) : (
                    <p className="empty-state">No workflow activity yet.</p>
                  )}
                </div>
              ) : (
                <AmiReportsDrawer onSelectEntity={select} />
              )}
            </div>
          ) : null}
        </section>
      </main>

      {shell.paletteOpen ? (
        <CommandPalette
          catalog={descriptors}
          context={commandContext}
          entities={entities}
          navigation={navigationCommands}
          onClose={() => {
            setGalaxyCommand(undefined);
            dispatch({ type: "set_palette", open: false });
          }}
          onNavigate={navigate}
          initialCommand={galaxyCommand}
          onWorkflowStarted={(workflow) => {
            // Deliberately does not navigate: the user started this from
            // wherever they were working, and being moved to another page
            // loses that context. Offer the destination instead.
            setSelectedAutomationWorkflow(workflow.id);
            setCommandResult({
              message: `Started ${workflow.kind}`,
              actionLabel: "View in Automations",
              onAction: () => {
                navigate("Automations");
              },
            });
          }}
          onOperationFinished={(execution) => {
            setSelectedExecution(execution);
            setCommandResult({
              message:
                execution.status === "running"
                  ? `${execution.kind} started`
                  : `${execution.kind} ${execution.status}`,
              actionLabel: "View in History",
              onAction: () => {
                navigate("History");
              },
            });
          }}
        />
      ) : null}

      <NotificationToasts
        notifications={notifications}
        ready={revision !== null}
        onSelect={(notification) => {
          openNotification(notification);
        }}
        onDismiss={(notification) => {
          setDismissedNotificationIds((current) => {
            const next = new Set(current);
            next.add(notification.id);
            persistDismissedNotificationIds(next);
            return next;
          });
        }}
      />
      {commandResult && (
        <div className="toast-stack command-result" role="status">
          <article className="toast info">
            <div>
              <strong>{commandResult.message}</strong>
            </div>
            <div className="toast-actions">
              <button
                onClick={() => {
                  commandResult.onAction();
                  setCommandResult(null);
                }}
              >
                {commandResult.actionLabel}
              </button>
              <button
                aria-label="Dismiss"
                className="toast-dismiss"
                onClick={() => {
                  setCommandResult(null);
                }}
              >
                ×
              </button>
            </div>
          </article>
        </div>
      )}
      {notificationsOpen && (
        <NotificationCenter
          notifications={notifications}
          onClose={() => {
            setNotificationsOpen(false);
          }}
          onSelect={(notification) => {
            openNotification(notification);
          }}
          onDismiss={(notification: Notification) => {
            setDismissedNotificationIds((current) => {
              const next = new Set(current);
              next.add(notification.id);
              persistDismissedNotificationIds(next);
              return next;
            });
          }}
          onClearAll={() => {
            setDismissedNotificationIds((current) => {
              const next = new Set(current);
              for (const notification of notifications)
                next.add(notification.id);
              persistDismissedNotificationIds(next);
              return next;
            });
          }}
        />
      )}
      <ConfirmDialog
        onClose={() => {
          setConfirmRequest(null);
        }}
        request={confirmRequest}
      />
    </div>
  );
}
