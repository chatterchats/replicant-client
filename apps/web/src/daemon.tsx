/* eslint-disable react-refresh/only-export-components */
import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useReducer,
  useRef,
  useState,
} from "react";

import { createBootstrapBackoff } from "./bootstrapBackoff";
import { daemonApi, daemonToken, daemonUrl } from "./api";
import { sharedQueryCache, type QueryCacheSubscription } from "./queryCache";
import { recordWebEvent } from "./telemetry";
import {
  type AutomationControlAction,
  type AutomationStatus,
  type DaemonHealth,
  type DomainSlice,
  type EntityIndexSnapshot,
  type EntitySummary,
  type LiveMessage,
  type Notification,
  type OperationUpdate,
  type RequirementSummary,
  parseLiveMessage,
  type RuntimeSnapshot,
  type RuntimeSyncStatus,
  type WorkflowActivity,
  type WorkflowSummary,
} from "./protocol";

export type ConnectionState =
  "connecting" | "connected" | "reconnecting" | "offline";

/** Caps on session-lived lists so a long automation run cannot grow state without bound. */
export const ACTIVITY_LIMIT = 500;
export const NOTIFICATION_LIMIT = 200;

export interface DaemonState {
  connection: ConnectionState;
  revision: number | null;
  entityRevision: number;
  galaxyRevision: number;
  syncing: boolean;
  health: DaemonHealth | null;
  sync: RuntimeSyncStatus | null;
  automation: AutomationStatus;
  entities: Record<string, EntitySummary>;
  workflows: Record<string, WorkflowSummary>;
  requirements: RequirementSummary[];
  activity: WorkflowActivity[];
  notifications: Notification[];
  operations: Record<string, OperationUpdate>;
  invalidated: Partial<Record<DomainSlice, number>>;
  needsResnapshot: boolean;
  error: string | null;
}

export type DaemonAction =
  | { type: "connecting"; retry: boolean }
  | { type: "connected" }
  | { type: "disconnected"; error: string }
  | {
      type: "snapshot";
      snapshot: RuntimeSnapshot;
      health: DaemonHealth;
      entities: EntityIndexSnapshot;
    }
  | { type: "entity_index"; entities: EntityIndexSnapshot }
  | { type: "live"; message: LiveMessage }
  | { type: "continuity_lost"; error: string };

export const initialDaemonState: DaemonState = {
  connection: "connecting",
  revision: null,
  entityRevision: 0,
  galaxyRevision: 0,
  syncing: true,
  health: null,
  sync: null,
  automation: {
    automatic_triggers_enabled: false,
    workflows_paused: true,
  },
  entities: {},
  workflows: {},
  requirements: [],
  activity: [],
  notifications: [],
  operations: {},
  invalidated: {},
  needsResnapshot: false,
  error: null,
};

function key(kind: string, id: string) {
  return `${kind}:${id}`;
}

function workflowEntity(workflow: WorkflowSummary): EntitySummary {
  return {
    entity: { kind: "workflow", id: workflow.id },
    label: workflow.kind,
    secondary_label: workflow.id,
    system: null,
    location: null,
    entity_type: null,
    status: workflow.status,
  };
}

function continuityLost(state: DaemonState, error: string): DaemonState {
  return { ...state, syncing: true, needsResnapshot: true, error };
}

/** Keeps the newest `limit` entries of an append-only list. */
function capped<T>(items: T[], limit: number): T[] {
  return items.length > limit ? items.slice(items.length - limit) : items;
}

/**
 * Merges announced slice revisions, keeping the highest seen per slice.
 *
 * Domain queries refetch when their slice's number changes, so taking the
 * maximum makes replayed or out-of-order messages idempotent.
 */
function mergeSlices(
  current: Partial<Record<DomainSlice, number>>,
  incoming: Partial<Record<DomainSlice, number>>,
): Partial<Record<DomainSlice, number>> {
  const merged = { ...current };
  for (const [slice, revision] of Object.entries(incoming) as [
    DomainSlice,
    number,
  ][]) {
    if ((merged[slice] ?? 0) < revision) merged[slice] = revision;
  }
  return merged;
}

export function daemonReducer(
  state: DaemonState,
  action: DaemonAction,
): DaemonState {
  switch (action.type) {
    case "connecting":
      return {
        ...state,
        connection: action.retry ? "reconnecting" : "connecting",
        syncing: true,
      };
    case "connected":
      return { ...state, connection: "connected", error: null };
    case "disconnected":
      return { ...state, connection: "offline", error: action.error };
    case "continuity_lost":
      return continuityLost(state, action.error);
    case "snapshot": {
      if (
        state.revision !== null &&
        action.snapshot.metadata.revision < state.revision
      )
        return state;
      const replaceEntities =
        action.entities.metadata.revision >= state.entityRevision;
      return {
        ...state,
        revision: action.snapshot.metadata.revision,
        entityRevision: replaceEntities
          ? action.entities.metadata.revision
          : state.entityRevision,
        galaxyRevision: action.snapshot.metadata.revision,
        syncing: action.snapshot.sync.phase !== "ready",
        health: action.health,
        sync: action.snapshot.sync,
        automation: action.snapshot.automation,
        entities: replaceEntities
          ? Object.fromEntries(
              action.entities.entities.map((summary) => [
                key(summary.entity.kind, summary.entity.id),
                summary,
              ]),
            )
          : state.entities,
        workflows: Object.fromEntries(
          action.snapshot.workflows.map((workflow) => [workflow.id, workflow]),
        ),
        requirements: action.snapshot.requirements,
        activity: [],
        notifications: capped(
          action.snapshot.notifications,
          NOTIFICATION_LIMIT,
        ),
        operations: {},
        // Seeded from the snapshot rather than cleared: pages compare these
        // numbers to decide whether their projection is stale, so keeping them
        // avoids a refetch storm on every reconnect.
        invalidated: action.snapshot.slice_revisions,
        needsResnapshot: false,
        error: null,
      };
    }
    case "entity_index":
      if (action.entities.metadata.revision < state.entityRevision)
        return state;
      return {
        ...state,
        entityRevision: action.entities.metadata.revision,
        entities: Object.fromEntries(
          action.entities.entities.map((summary) => [
            key(summary.entity.kind, summary.entity.id),
            summary,
          ]),
        ),
      };
    case "live": {
      const { message } = action;
      if (state.needsResnapshot) return state;
      if (message.delta.type === "snapshot") {
        // The daemon marks a resnapshot point (for example after a subscriber
        // lagged). Anything we already have at or beyond this revision is
        // current; only an advance means we missed state.
        return state.revision !== null && message.revision <= state.revision
          ? state
          : continuityLost(state, "A newer daemon snapshot is available");
      }
      if (state.revision === null) {
        return continuityLost(state, "Live updates arrived before a snapshot");
      }
      // Messages at or below the loaded revision are replays (the socket is
      // opened before the snapshot is fetched, so overlap is expected).
      if (message.revision <= state.revision) return state;

      // Deliberately not requiring `revision + 1`. Slice revisions carried by
      // invalidations let a client that missed messages recover by comparison,
      // so a gap no longer forces a full resnapshot cycle.
      const next = { ...state, revision: message.revision };
      switch (message.delta.type) {
        case "entity_upsert": {
          const { entity, value } = message.delta.data;
          return {
            ...next,
            entityRevision: message.revision,
            entities: {
              ...state.entities,
              [key(entity.kind, entity.id)]: value,
            },
          };
        }
        case "entity_remove": {
          const { entity } = message.delta.data;
          const removed = key(entity.kind, entity.id);
          const entities = Object.fromEntries(
            Object.entries(state.entities).filter(
              ([entry]) => entry !== removed,
            ),
          );
          return { ...next, entityRevision: message.revision, entities };
        }
        case "domain_invalidated":
          return {
            ...next,
            galaxyRevision:
              message.delta.data.slice === "universe"
                ? message.revision
                : state.galaxyRevision,
            invalidated: mergeSlices(state.invalidated, {
              [message.delta.data.slice]: message.revision,
            }),
          };
        case "domains_invalidated": {
          const { slices } = message.delta.data;
          return {
            ...next,
            galaxyRevision:
              slices.universe === undefined
                ? state.galaxyRevision
                : slices.universe,
            invalidated: mergeSlices(state.invalidated, slices),
          };
        }
        case "workflow_created":
        case "workflow_updated": {
          const workflow = message.delta.data;
          const entity = workflowEntity(workflow);
          return {
            ...next,
            entityRevision: message.revision,
            galaxyRevision: message.revision,
            needsResnapshot: workflow.kind === "requirement.fulfillment",
            entities: {
              ...state.entities,
              [key(entity.entity.kind, entity.entity.id)]: entity,
            },
            workflows: {
              ...state.workflows,
              [workflow.id]: workflow,
            },
          };
        }
        case "workflow_activity":
          return {
            ...next,
            activity: capped(
              [...state.activity, message.delta.data],
              ACTIVITY_LIMIT,
            ),
          };
        case "operation_updated":
          return {
            ...next,
            operations: {
              ...state.operations,
              [message.delta.data.id]: message.delta.data,
            },
          };
        case "notification": {
          const notification = message.delta.data;
          return {
            ...next,
            notifications: capped(
              [
                ...state.notifications.filter(
                  (item) => item.id !== notification.id,
                ),
                notification,
              ],
              NOTIFICATION_LIMIT,
            ),
          };
        }
        case "automation_changed":
          return { ...next, automation: message.delta.data };
        case "daemon_status_changed":
          return {
            ...next,
            health: message.delta.data.health,
            sync: message.delta.data.sync,
            syncing: message.delta.data.sync.phase !== "ready",
          };
      }
    }
  }
}

type BootstrapKind = "connect" | "resnapshot";

interface BootstrapTask {
  controller: AbortController;
  buffered: LiveMessage[];
  promise: Promise<void>;
}

const DaemonContext = createContext<DaemonState | null>(null);
export function DaemonProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(daemonReducer, initialDaemonState);
  const socketRef = useRef<WebSocket | undefined>(undefined);
  const lastDiagnosticState = useRef<string>("");
  const entityQueryRef = useRef<QueryCacheSubscription | null>(null);
  const entityInvalidationTimerRef = useRef<number | null>(null);
  const bootstrapRef = useRef<BootstrapTask | null>(null);
  const resnapshotStartRef = useRef<(() => void) | null>(null);
  const resnapshotTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const initialized = state.revision !== null;
  const committedRef = useRef(false);
  committedRef.current = initialized;
  const entityInitialRevisionRef = useRef("0");
  entityInitialRevisionRef.current = String(
    state.invalidated.entities ?? state.revision ?? 0,
  );

  useEffect(() => {
    const lifecycleController = new AbortController();
    let socket: WebSocket | undefined;
    let healthTimer: ReturnType<typeof setTimeout> | null = null;
    let healthController: AbortController | undefined;
    let healthProbeActive = false;
    const healthBackoff = createBootstrapBackoff();

    const clearHealthTimer = () => {
      if (healthTimer !== null) {
        clearTimeout(healthTimer);
        healthTimer = null;
      }
    };
    const clearResnapshotTimer = () => {
      if (resnapshotTimerRef.current !== null) {
        clearTimeout(resnapshotTimerRef.current);
        resnapshotTimerRef.current = null;
      }
    };

    const scheduleHealthProbe = () => {
      if (
        lifecycleController.signal.aborted ||
        healthTimer !== null ||
        healthProbeActive ||
        socketRef.current?.readyState === WebSocket.OPEN ||
        socketRef.current?.readyState === WebSocket.CONNECTING
      )
        return;
      const delayMs = healthBackoff.nextDelayMs();
      recordWebEvent(
        "warn",
        "frontend.daemon_reconnect_scheduled",
        "daemon health probe scheduled",
        { attempt: healthBackoff.attempt, delay_ms: delayMs },
      );
      healthTimer = setTimeout(() => {
        healthTimer = null;
        void probeHealth();
      }, delayMs);
    };

    const runBootstrap = (
      kind: BootstrapKind,
      started = performance.now(),
      knownHealth?: DaemonHealth,
    ): Promise<void> => {
      const active = bootstrapRef.current;
      if (active !== null) {
        if (active.controller.signal.aborted) {
          return active.promise
            .catch(() => undefined)
            .then(() => runBootstrap(kind, started, knownHealth));
        }
        return active.promise;
      }

      const task: BootstrapTask = {
        controller: new AbortController(),
        buffered: [],
        promise: Promise.resolve(),
      };
      bootstrapRef.current = task;
      task.promise = (async () => {
        const health =
          knownHealth ?? (await daemonApi.health(task.controller.signal));
        if (health.status !== "healthy")
          throw new Error(health.detail ?? `Daemon health is ${health.status}`);
        const [snapshot, entities] = await Promise.all([
          daemonApi.snapshot(task.controller.signal),
          daemonApi.entities(task.controller.signal),
        ]);
        if (task.controller.signal.aborted) return;
        sharedQueryCache.seed(
          "entities",
          entities,
          String(
            snapshot.slice_revisions.entities ?? entities.metadata.revision,
          ),
        );
        dispatch({ type: "snapshot", snapshot, health, entities });
        if (kind === "connect") {
          recordWebEvent(
            "info",
            "frontend.daemon_snapshot_loaded",
            "initial daemon snapshot loaded",
            {
              elapsed_ms: Math.round(performance.now() - started),
              health: health.status,
              sync_phase: snapshot.sync.phase,
              revision: snapshot.metadata.revision,
              workflows: snapshot.workflows.length,
              entities: entities.entities.length,
              notifications: snapshot.notifications.length,
            },
          );
        }
        const replay = task.buffered;
        task.buffered = [];
        for (const message of replay) dispatch({ type: "live", message });
      })().finally(() => {
        if (bootstrapRef.current === task) bootstrapRef.current = null;
      });
      return task.promise;
    };

    const scheduleResnapshot = () => {
      if (
        lifecycleController.signal.aborted ||
        resnapshotTimerRef.current !== null ||
        socketRef.current?.readyState !== WebSocket.OPEN
      )
        return;
      const delayMs = healthBackoff.nextDelayMs();
      recordWebEvent(
        "warn",
        "frontend.daemon_resnapshot_scheduled",
        "daemon resnapshot scheduled",
        { attempt: healthBackoff.attempt, delay_ms: delayMs },
      );
      resnapshotTimerRef.current = setTimeout(() => {
        resnapshotTimerRef.current = null;
        resnapshotStartRef.current?.();
      }, delayMs);
    };

    const startResnapshot = () => {
      if (
        lifecycleController.signal.aborted ||
        socketRef.current?.readyState !== WebSocket.OPEN
      )
        return;
      void runBootstrap("resnapshot")
        .then(() => {
          healthBackoff.reset();
        })
        .catch((error: unknown) => {
          if (lifecycleController.signal.aborted) return;
          dispatch({ type: "continuity_lost", error: String(error) });
          scheduleResnapshot();
        });
    };
    resnapshotStartRef.current = startResnapshot;

    const connect = async (health: DaemonHealth) => {
      const started = performance.now();
      dispatch({ type: "connecting", retry: committedRef.current });
      recordWebEvent(
        "info",
        "frontend.daemon_connecting",
        committedRef.current
          ? "reconnecting to daemon"
          : "connecting to daemon",
        { attempt: healthBackoff.attempt },
      );
      let nextSocket: WebSocket | undefined;
      try {
        const deliver = (source: WebSocket, message: LiveMessage) => {
          if (socketRef.current !== source) return;
          const active = bootstrapRef.current;
          if (active !== null) active.buffered.push(message);
          else dispatch({ type: "live", message });
        };

        nextSocket = new WebSocket(socketUrl());
        const activeSocket = nextSocket;
        socket = nextSocket;
        socketRef.current = nextSocket;
        activeSocket.addEventListener("open", () => {
          recordWebEvent(
            "info",
            "frontend.daemon_socket_open",
            "daemon WebSocket connected",
            { connect_ms: Math.round(performance.now() - started) },
          );
          dispatch({ type: "connected" });
        });
        activeSocket.addEventListener("message", (event) => {
          try {
            if (typeof event.data !== "string")
              throw new Error("Invalid binary live message");
            deliver(
              activeSocket,
              parseLiveMessage(JSON.parse(event.data) as unknown),
            );
          } catch (error) {
            recordWebEvent(
              "error",
              "frontend.daemon_live_message_invalid",
              "daemon live message could not be parsed",
              { error: String(error).slice(0, 500) },
            );
            dispatch({ type: "continuity_lost", error: String(error) });
          }
        });
        activeSocket.addEventListener("close", (event) => {
          if (lifecycleController.signal.aborted) return;
          if (socketRef.current !== activeSocket) return;
          socketRef.current = undefined;
          socket = undefined;
          recordWebEvent(
            event.wasClean ? "info" : "warn",
            "frontend.daemon_socket_closed",
            "daemon WebSocket closed",
            {
              code: event.code,
              clean: event.wasClean,
              reason: event.reason.slice(0, 300),
            },
          );
          bootstrapRef.current?.controller.abort();
          clearResnapshotTimer();
          healthBackoff.reset();
          dispatch({ type: "disconnected", error: "Daemon connection closed" });
          scheduleHealthProbe();
        });
        activeSocket.addEventListener("error", () => {
          if (socketRef.current !== activeSocket) return;
          recordWebEvent(
            "warn",
            "frontend.daemon_socket_error",
            "daemon WebSocket reported an error",
          );
          activeSocket.close();
        });

        await runBootstrap("connect", started, health);
      } catch (error) {
        if (lifecycleController.signal.aborted) return;
        if (
          socketRef.current !== nextSocket ||
          nextSocket?.readyState === WebSocket.CLOSING ||
          nextSocket?.readyState === WebSocket.CLOSED
        )
          return;
        recordWebEvent(
          "warn",
          "frontend.daemon_connection_failed",
          "daemon connection/bootstrap failed",
          {
            elapsed_ms: Math.round(performance.now() - started),
            error: String(error).slice(0, 500),
          },
        );
        bootstrapRef.current?.controller.abort();
        socketRef.current = undefined;
        socket = undefined;
        nextSocket?.close();
        dispatch({ type: "disconnected", error: String(error) });
        scheduleHealthProbe();
      }
    };

    const probeHealth = async () => {
      if (
        lifecycleController.signal.aborted ||
        healthProbeActive ||
        socketRef.current?.readyState === WebSocket.OPEN ||
        socketRef.current?.readyState === WebSocket.CONNECTING
      )
        return;
      healthProbeActive = true;
      const controller = new AbortController();
      healthController = controller;
      dispatch({ type: "connecting", retry: committedRef.current });
      try {
        const health = await daemonApi.health(controller.signal);
        lifecycleController.signal.throwIfAborted();
        if (health.status !== "healthy")
          throw new Error(health.detail ?? `Daemon health is ${health.status}`);
        healthBackoff.reset();
        healthController = undefined;
        healthProbeActive = false;
        await connect(health);
      } catch (error) {
        healthController = undefined;
        healthProbeActive = false;
        if (controller.signal.aborted) return;
        dispatch({ type: "disconnected", error: String(error) });
        scheduleHealthProbe();
      }
    };

    void probeHealth();
    return () => {
      lifecycleController.abort();
      clearHealthTimer();
      healthController?.abort();
      healthProbeActive = false;
      bootstrapRef.current?.controller.abort();
      clearResnapshotTimer();
      resnapshotStartRef.current = null;
      socket?.close();
      socketRef.current = undefined;
    };
  }, []);
  useEffect(() => {
    if (!initialized || entityQueryRef.current !== null) return;
    const subscription = sharedQueryCache.subscribe(
      "entities",
      (signal) => daemonApi.entities(signal),
      entityInitialRevisionRef.current,
      (event) => {
        if (event.type === "success")
          dispatch({ type: "entity_index", entities: event.data });
        else if (event.type === "error")
          dispatch({ type: "continuity_lost", error: String(event.error) });
      },
    );
    entityQueryRef.current = subscription;
    return () => {
      subscription.unsubscribe();
      if (entityQueryRef.current === subscription)
        entityQueryRef.current = null;
    };
  }, [initialized]);

  useEffect(() => {
    const revision = state.invalidated.entities;
    if (revision === undefined || entityQueryRef.current === null) return;
    if (entityInvalidationTimerRef.current !== null)
      clearTimeout(entityInvalidationTimerRef.current);
    entityInvalidationTimerRef.current = window.setTimeout(() => {
      entityInvalidationTimerRef.current = null;
      entityQueryRef.current?.updateRevision(String(revision));
    }, 1_500);
    return () => {
      if (entityInvalidationTimerRef.current !== null) {
        clearTimeout(entityInvalidationTimerRef.current);
        entityInvalidationTimerRef.current = null;
      }
    };
  }, [state.invalidated.entities]);

  useEffect(() => {
    const signature = [
      state.connection,
      state.health?.status ?? "unknown",
      state.sync?.phase ?? "unknown",
      state.sync?.detail ?? "",
    ].join("|");
    if (signature === lastDiagnosticState.current) return;
    lastDiagnosticState.current = signature;
    const degraded =
      state.connection !== "connected" ||
      (state.health !== null && state.health.status !== "healthy") ||
      (state.sync !== null && state.sync.phase !== "ready");
    recordWebEvent(
      degraded ? "warn" : "info",
      "frontend.daemon_state",
      degraded
        ? "daemon connectivity/synchronization is degraded"
        : "daemon is ready",
      {
        connection: state.connection,
        health: state.health?.status ?? null,
        sync_phase: state.sync?.phase ?? null,
        revision: state.revision,
        detail: (state.sync?.detail ?? state.health?.detail ?? "").slice(
          0,
          500,
        ),
      },
    );
  }, [
    state.connection,
    state.health,
    state.revision,
    state.sync,
    state.health?.detail,
    state.health?.status,
    state.sync?.detail,
    state.sync?.phase,
  ]);

  return <DaemonContext value={state}>{children}</DaemonContext>;
}

export function socketUrl(
  location: Pick<Location, "href"> = window.location,
  daemonOrigin?: string,
) {
  const url = new URL(daemonUrl("/ws", daemonOrigin), location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  // Browsers cannot set headers on a WebSocket handshake, so the daemon also
  // accepts the shared secret as a query parameter.
  const token = daemonToken();
  if (token !== undefined) url.searchParams.set("token", token);
  return url.href;
}

export function useDaemonState(): DaemonState {
  const state = useContext(DaemonContext);
  if (!state)
    throw new Error("useDaemonState must be used within DaemonProvider");
  return state;
}

export const useDaemonHealth = () => useDaemonState().health;
export const useDaemonConnection = () => {
  const { connection, syncing, revision } = useDaemonState();
  return { connection, syncing, revision };
};
export const useEntities = () => useDaemonState().entities;
export const useWorkflows = () => Object.values(useDaemonState().workflows);
export const useActivity = () => useDaemonState().activity;
export const useNotifications = () => useDaemonState().notifications;
export const useGalaxyRevision = () => useDaemonState().galaxyRevision;

export function useAutomationControl() {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();

  /**
   * Runs an automation control action.
   *
   * Confirmation is the caller's responsibility: the shell raises a modal that
   * can name the affected workflows, which a blocking `window.confirm` could
   * not do.
   */
  const control = (
    action: AutomationControlAction,
    workflowIds: string[] = [],
  ) => {
    setBusy(true);
    setError(undefined);
    void daemonApi
      .controlAutomation(action, workflowIds, action === "cancel")
      .catch((err: unknown) => {
        setError(String(err));
      })
      .finally(() => {
        setBusy(false);
      });
  };

  return { busy, error, control };
}
