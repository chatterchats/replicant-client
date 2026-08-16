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
import { LeaderboardsPage } from "./LeaderboardsPage";
import { MessagesPage } from "./MessagesPage";
import { MiningPage } from "./MiningPage";
import { NetworkPage } from "./NetworkPage";
import { OverviewPage } from "./OverviewPage";
import { DevicesPage } from "./DevicesPage";
import { EventsPage } from "./EventsPage";
import { RequirementsPage } from "./RequirementsPage";
import { RelayPage } from "./RelayPage";
import { ReportsPage } from "./ReportsPage";
import { SettingsPage } from "./SettingsPage";
import { StandingPage } from "./StandingPage";
import { SystemPage } from "./SystemPage";
import { SurveyPage } from "./SurveyPage";
import { TradePage } from "./TradePage";
import { ConfirmDialog, type ConfirmRequest } from "./ConfirmDialog";
import { NotificationCenter, NotificationToasts } from "./Notifications";
import { absoluteTime, relativeTime } from "./time";
import { daemonApi } from "./api";
import type {
  DescriptorCatalog,
  DeviceSummary,
  EntitySummary,
  EventSummary,
  FiniteExecution,
  GalaxyStar,
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

function Inspector({
  entity,
  value,
  onClose,
  onClear,
  onOpenGalaxy,
  onOpenSystem,
  onOpenWorkflow,
}: {
  entity: SelectedEntity;
  value: unknown;
  onClose: () => void;
  onClear: () => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onOpenWorkflow: (workflowId: string) => void;
}) {
  const device = isDeviceSummary(value) ? value : undefined;
  const workflow = !device && isWorkflowSummary(value) ? value : undefined;
  const event =
    !device && !workflow && isEventSummary(value) ? value : undefined;
  const summary =
    !device && !workflow && !event && isEntitySummary(value)
      ? value
      : undefined;
  const targetSystem =
    entity.kind === "system"
      ? entity.id
      : (event?.system ?? device?.system ?? summary?.system ?? null);
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
      page: route.page,
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
      dispatch({
        type: "restore",
        route: routeFromHash(window.location.hash, {
          page: initialShellState.page,
          entity: null,
        }),
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
      : shell.selectedEntity.kind === "device" &&
          selectedDevice?.entity.id === shell.selectedEntity.id
        ? selectedDevice
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
            ) : shell.page === "Relay" ? (
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
            ) : shell.page === "Events" ? (
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
              <MessagesPage onSelectEntity={select} />
            ) : shell.page === "Network" ? (
              <NetworkPage onSelectEntity={select} />
            ) : shell.page === "Standing" ? (
              <StandingPage />
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
              onClose={() => {
                dispatch({ type: "toggle_inspector" });
              }}
              onClear={() => {
                dispatch({ type: "clear_selection" });
              }}
              onOpenGalaxy={openGalaxy}
              onOpenSystem={openSystem}
              onOpenWorkflow={openWorkflow}
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
                        title={absoluteTime(item.occurred_at_ms)}
                      >
                        {relativeTime(item.occurred_at_ms)}
                      </time>
                      <strong>
                        {daemon.workflows[item.workflow_id]?.kind ?? "workflow"}{" "}
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
        onSelect={() => {
          setNotificationsOpen(true);
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
          onClose={() => {
            setNotificationsOpen(false);
          }}
          onSelect={() => {
            navigate("History");
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
