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
import { CommandPalette, type CommandContext } from "./CommandPalette";
import { daemonApi } from "./api";
import type { DescriptorCatalog, EntityKind, WorkflowStatus } from "./protocol";
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
  return (
    <aside className="inspector" aria-label="Selected entity inspector">
      <header className="drawer-header">
        <div>
          <small>{entity.kind}</small>
          <strong>{entity.id}</strong>
        </div>
        <button aria-label="Close inspector" onClick={onClose}>
          ×
        </button>
      </header>
      <div className="inspector-body">
        {value === undefined ? (
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
    () =>
      Object.keys(entities).map((key) => {
        const separator = key.indexOf(":");
        return {
          kind: key.slice(0, separator) as EntityKind,
          id: key.slice(separator + 1),
        };
      }),
    [entities],
  );
  const currentReplicant = entityList.find(
    (entity) => entity.kind === "replicant",
  );
  const currentReplicantValue = currentReplicant
    ? entities[`replicant:${currentReplicant.id}`]
    : undefined;
  const currentLocation = textField(
    currentReplicantValue,
    "location",
    "location_code",
  );
  const currentSystem = textField(
    currentReplicantValue,
    "system",
    "system_code",
  );
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
      : entities[`${shell.selectedEntity.kind}:${shell.selectedEntity.id}`]
    : undefined;
  const commandContext: CommandContext = {
    system:
      (shell.selectedEntity?.kind === "system"
        ? shell.selectedEntity.id
        : null) ??
      textField(selectedValue, "system", "system_code") ??
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
    dispatch({ type: "select", entity });
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
          <button
            className="palette-trigger"
            onClick={() => {
              dispatch({ type: "set_palette", open: true });
            }}
          >
            Commands <kbd>⌘K</kbd>
          </button>
        </header>

        <div className="workspace">
          <div className="content-column">
            {shell.page === "Automations" ? (
              <AutomationsPage entities={entities} workflows={workflows} />
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

                {entityList.length || workflows.length ? (
                  <section className="entity-list" aria-label="Live entities">
                    <h2>Live entities</h2>
                    <div>
                      {entityList.map((entity) => (
                        <button
                          key={`${entity.kind}:${entity.id}`}
                          onClick={() => {
                            select(entity);
                          }}
                        >
                          <small>{entity.kind}</small>
                          {entity.id}
                        </button>
                      ))}
                      {workflows.map((workflow) => (
                        <button
                          key={workflow.id}
                          onClick={() => {
                            select({ kind: "workflow", id: workflow.id });
                          }}
                        >
                          <small>workflow · {workflow.status}</small>
                          {workflow.kind}
                        </button>
                      ))}
                    </div>
                  </section>
                ) : null}
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
            dispatch({ type: "set_palette", open: false });
          }}
          onNavigate={navigate}
          onWorkflowStarted={() => {
            navigate("Automations");
          }}
        />
      ) : null}
    </div>
  );
}
