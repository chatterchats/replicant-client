import { useEffect, useMemo, useReducer, useState } from "react";

import {
  useActivity,
  useDaemonConnection,
  useDaemonHealth,
  useDaemonState,
  useEntities,
  useNotifications,
  useWorkflows,
} from "./daemon";
import { AutomationsPage } from "./AutomationsPage";
import { AutofactoryPage } from "./AutofactoryPage";
import { BootstrapPage } from "./BootstrapPage";
import { CargoPage } from "./CargoPage";
import {
  CommandPalette,
  type CommandContext,
  type DescriptorCommand,
} from "./CommandPalette";
import { GalaxyPage } from "./GalaxyPage";
import { HistoryPage } from "./HistoryPage";
import { InventoryPage } from "./InventoryPage";
import { MiningPage } from "./MiningPage";
import { OverviewPage } from "./OverviewPage";
import { DevicesPage } from "./DevicesPage";
import { RequirementsPage } from "./RequirementsPage";
import { RelayPage } from "./RelayPage";
import { SystemPage } from "./SystemPage";
import { SurveyPage } from "./SurveyPage";
import { daemonApi } from "./api";
import type {
  DescriptorCatalog,
  DeviceSummary,
  EntitySummary,
  FiniteExecution,
  GalaxyStar,
  SystemMarker,
  WorkflowStatus,
} from "./protocol";
import {
  initialShellState,
  shellReducer,
  type SelectedEntity,
} from "./shellState";

const navigation = [
  ["Operations", ["Overview", "Galaxy", "System"]],
  ["Assets", ["Devices", "Inventory", "Autofactory", "Cargo"]],
  ["Missions", ["Survey", "Mining", "Relay", "Events", "Bootstrap", "Trade"]],
  ["Automation", ["Automations", "Requirements", "History"]],
  [
    "Intelligence",
    ["Reports", "Messages", "Network", "Standing", "Leaderboards"],
  ],
] as const;

const navigationCommands = [
  ...navigation.flatMap(([, items]) => items),
  "Settings",
];
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

function Inspector({
  entity,
  value,
  onClose,
  onClear,
}: {
  entity: SelectedEntity;
  value: unknown;
  onClose: () => void;
  onClear: () => void;
}) {
  const device = isDeviceSummary(value) ? value : undefined;
  const summary = isEntitySummary(value) ? value : undefined;
  return (
    <aside className="inspector" aria-label="Selected entity inspector">
      <header className="drawer-header">
        <div>
          <small>{entity.kind}</small>
          <strong>{summary?.label ?? entity.id}</strong>
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
                <dd>{device.controlled_devices.join(", ")}</dd>
              </>
            )}
            {device.attached_devices.length > 0 && (
              <>
                <dt>Attached devices</dt>
                <dd>{device.attached_devices.join(", ")}</dd>
              </>
            )}
            {device.stowed_devices.length > 0 && (
              <>
                <dt>Stowed devices</dt>
                <dd>{device.stowed_devices.join(", ")}</dd>
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
                <dt>Attach capacity</dt>
                <dd>{device.attach_capacity}</dd>
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
          <pre>{JSON.stringify(value, null, 2)}</pre>
        )}
      </div>
      <button className="clear-selection" onClick={onClear}>
        Clear selection
      </button>
    </aside>
  );
}

export function App() {
  const [shell, dispatch] = useReducer(shellReducer, initialShellState);
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
  const [galaxyCommand, setGalaxyCommand] = useState<DescriptorCommand>();
  const [selectedAutomationWorkflow, setSelectedAutomationWorkflow] =
    useState<string>();
  const [automationBusy, setAutomationBusy] = useState(false);
  const [automationError, setAutomationError] = useState<string>();
  const daemon = useDaemonState();
  const health = useDaemonHealth();
  const { connection, syncing, revision } = useDaemonConnection();
  const entities = useEntities();
  const workflows = useWorkflows();
  const activity = useActivity();
  const notifications = useNotifications();

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
      : shell.selectedEntity.kind === "device" &&
          selectedDevice?.entity.id === shell.selectedEntity.id
        ? selectedDevice
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
  };
  const select = (entity: SelectedEntity) => {
    if (entity.kind !== "device") setSelectedDevice(undefined);
    dispatch({ type: "select", entity });
  };
  const controlAutomation = (
    action:
      | "enable_triggers"
      | "disable_triggers"
      | "pause_all"
      | "resume_all"
      | "cancel",
  ) => {
    const confirmed =
      action !== "cancel" ||
      window.confirm("Cancel every eligible workflow? This cannot be undone.");
    if (!confirmed) return;
    setAutomationBusy(true);
    setAutomationError(undefined);
    void daemonApi
      .controlAutomation(action, [], action === "cancel")
      .catch((error: unknown) => {
        setAutomationError(String(error));
      })
      .finally(() => {
        setAutomationBusy(false);
      });
  };

  return (
    <div className="app-shell">
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
                  {item}
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
          <span className={`status-item ${warnings.length ? "warning" : ""}`}>
            <small>Warnings</small>
            <strong>{warnings.length}</strong>
          </span>
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
                disabled={connection !== "connected" || automationBusy}
                onClick={() => {
                  controlAutomation("cancel");
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
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
                onOpenSystem={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("System");
                }}
              />
            ) : shell.page === "Devices" ? (
              <DevicesPage
                descriptors={descriptors}
                onSelectDevice={(device) => {
                  setSelectedDevice(device);
                  select(device.entity);
                }}
                onSelectEntity={select}
                onOpenSystem={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("System");
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Inventory" ? (
              <InventoryPage
                onSelectEntity={select}
                onOpenSystem={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("System");
                }}
              />
            ) : shell.page === "Autofactory" ? (
              <AutofactoryPage
                descriptors={descriptors}
                onSelectDevice={(device) => {
                  setSelectedDevice(device);
                  select(device.entity);
                }}
                onSelectEntity={select}
                onOpenSystem={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("System");
                }}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
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
                onOpenSystem={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("System");
                }}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Survey" ? (
              <SurveyPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("Galaxy");
                }}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Mining" ? (
              <MiningPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("Galaxy");
                }}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Relay" ? (
              <RelayPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("Galaxy");
                }}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
                onRunCommand={(command) => {
                  setGalaxyCommand(command);
                  dispatch({ type: "set_palette", open: true });
                }}
              />
            ) : shell.page === "Bootstrap" ? (
              <BootstrapPage
                descriptors={descriptors}
                onOpenGalaxy={(system) => {
                  setSelectedSystem(system);
                  select({ kind: "system", id: system });
                  navigate("Galaxy");
                }}
                onOpenHistory={() => {
                  navigate("History");
                }}
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
              />
            ) : shell.page === "Requirements" ? (
              <RequirementsPage
                requirements={daemon.requirements}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
              />
            ) : shell.page === "History" ? (
              <HistoryPage
                workflows={workflows}
                selectedExecution={selectedExecution}
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
                onSelectEntity={select}
              />
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
                onSelectWorkflow={(workflowId) => {
                  setSelectedAutomationWorkflow(workflowId);
                  navigate("Automations");
                }}
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
              onClose={() => {
                dispatch({ type: "toggle_inspector" });
              }}
              onClear={() => {
                dispatch({ type: "clear_selection" });
              }}
            />
          ) : null}
        </div>

        <section
          className={`activity-drawer ${shell.activityOpen ? "open" : ""}`}
          aria-label="Workflow activity"
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
                        dateTime={new Date(item.occurred_at_ms).toISOString()}
                      >
                        {new Date(item.occurred_at_ms).toLocaleTimeString()}
                      </time>
                      <strong>{item.workflow_id}</strong>
                      <span>{item.step ?? item.level}</span>
                      <p>{item.message}</p>
                    </button>
                  ))
              ) : (
                <p className="empty-state">No workflow activity yet.</p>
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
            setSelectedAutomationWorkflow(workflow.id);
            navigate("Automations");
          }}
          onOperationFinished={(execution) => {
            setSelectedExecution(execution);
            navigate("History");
          }}
        />
      ) : null}
    </div>
  );
}
