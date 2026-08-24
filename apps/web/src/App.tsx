/* eslint-disable react-refresh/only-export-components */
import { useCallback, useEffect, useMemo, useReducer, useState } from "react";

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
import { DirectoryPage } from "./DirectoryPage";
import {
  CommandPalette,
  type CommandContext,
  type DescriptorCommand,
} from "./CommandPalette";
import { GalaxyPage } from "./GalaxyPage";
import { HistoryPage } from "./HistoryPage";
import { InventoryPage } from "./InventoryPage";
import { Inspector } from "./inspector/Inspector";
export {
  relatedDeviceLabel,
  specializeDeviceCommand,
} from "./inspector/Inspector";
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
  DeviceSummary,
  EventSummary,
  FiniteExecution,
  GalaxyStar,
  Notification,
  SystemMarker,
  WorkflowStatus,
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
  const runCommand = useCallback((command: DescriptorCommand) => {
    setGalaxyCommand(command);
    dispatch({ type: "set_palette", open: true });
  }, []);
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
                onRunCommand={runCommand}
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
                onRunCommand={runCommand}
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
                onRunCommand={runCommand}
              />
            ) : shell.page === "Survey" ? (
              <SurveyPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={openGalaxy}
                onSelectWorkflow={openWorkflow}
                onRunCommand={runCommand}
              />
            ) : shell.page === "Mining" ? (
              <MiningPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={openGalaxy}
                onSelectWorkflow={openWorkflow}
                onRunCommand={runCommand}
              />
            ) : shell.page === "Relay" || shell.page === "Network" ? (
              <RelayPage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenGalaxy={openGalaxy}
                onSelectWorkflow={openWorkflow}
                onRunCommand={runCommand}
              />
            ) : shell.page === "Bootstrap" ? (
              <BootstrapPage
                descriptors={descriptors}
                onOpenGalaxy={openGalaxy}
                onOpenHistory={() => {
                  navigate("History");
                }}
                onRunCommand={runCommand}
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
                onRunCommand={runCommand}
              />
            ) : shell.page === "Trade" ? (
              <TradePage
                descriptors={descriptors}
                onSelectEntity={select}
                onOpenSystem={openSystem}
                onSelectWorkflow={openWorkflow}
                onRunCommand={runCommand}
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
                onRunCommand={runCommand}
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
                onRunCommand={runCommand}
                onOpenGalaxy={() => {
                  navigate("Galaxy");
                }}
                onSelectEntity={select}
              />
            ) : shell.page === "Observatory" ? (
              <ObservatoryPage
                descriptors={descriptors}
                onSelectEntity={select}
                onRunCommand={runCommand}
              />
            ) : shell.page === "Cloning" ? (
              <CloningPage
                descriptors={descriptors}
                onSelectEntity={select}
                onRunCommand={runCommand}
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
                onRunCommand={runCommand}
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
              onRunCommand={runCommand}
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
