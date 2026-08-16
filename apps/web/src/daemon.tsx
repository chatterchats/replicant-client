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

import { daemonApi, daemonToken, daemonUrl } from "./api";
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
    case "snapshot":
      return {
        ...state,
        revision: action.snapshot.metadata.revision,
        galaxyRevision: action.snapshot.metadata.revision,
        syncing: action.snapshot.sync.phase !== "ready",
        health: action.health,
        sync: action.snapshot.sync,
        automation: action.snapshot.automation,
        entities: Object.fromEntries(
          action.entities.entities.map((summary) => [
            key(summary.entity.kind, summary.entity.id),
            summary,
          ]),
        ),
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
    case "entity_index":
      return {
        ...state,
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
          return { ...next, entities };
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
        case "workflow_updated":
          return {
            ...next,
            galaxyRevision: message.revision,
            needsResnapshot:
              message.delta.data.kind === "requirement.fulfillment",
            workflows: {
              ...state.workflows,
              [message.delta.data.id]: message.delta.data,
            },
          };
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

export function retryDelay(attempt: number): number {
  return Math.min(500 * 2 ** Math.min(attempt, 5), 10_000);
}

const DaemonContext = createContext<DaemonState | null>(null);

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

export function DaemonProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(daemonReducer, initialDaemonState);
  const socketRef = useRef<WebSocket | undefined>(undefined);

  useEffect(() => {
    const controller = new AbortController();
    let socket: WebSocket | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;

    const connect = async () => {
      dispatch({ type: "connecting", retry: attempt > 0 });
      try {
        // The socket is opened *before* the snapshot is fetched, and messages
        // that arrive meanwhile are buffered and replayed afterwards. Fetching
        // first left a window where a publish between the snapshot and the
        // subscription was lost, which the old strict-continuity check then
        // reported as lost continuity.
        let buffered: LiveMessage[] | null = [];
        const deliver = (message: LiveMessage) => {
          if (buffered) buffered.push(message);
          else dispatch({ type: "live", message });
        };

        socket = new WebSocket(socketUrl());
        socketRef.current = socket;
        socket.addEventListener("open", () => {
          attempt = 0;
          dispatch({ type: "connected" });
        });
        socket.addEventListener("message", (event) => {
          try {
            if (typeof event.data !== "string")
              throw new Error("Invalid binary live message");
            deliver(parseLiveMessage(JSON.parse(event.data) as unknown));
          } catch (error) {
            dispatch({ type: "continuity_lost", error: String(error) });
          }
        });
        socket.addEventListener("close", scheduleReconnect);
        socket.addEventListener("error", () => socket?.close());

        const [health, snapshot, entities] = await Promise.all([
          daemonApi.health(controller.signal),
          daemonApi.snapshot(controller.signal),
          daemonApi.entities(controller.signal),
        ]);
        if (controller.signal.aborted) return;
        dispatch({ type: "snapshot", snapshot, health, entities });

        // Replay anything received while the snapshot was in flight; the
        // reducer discards messages at or below the snapshot's revision.
        const replay = buffered;
        buffered = null;
        for (const message of replay) dispatch({ type: "live", message });
      } catch (error) {
        if (!controller.signal.aborted) {
          dispatch({ type: "disconnected", error: String(error) });
          scheduleReconnect();
        }
      }
    };

    const scheduleReconnect = () => {
      if (controller.signal.aborted || timer !== undefined) return;
      dispatch({ type: "disconnected", error: "Daemon connection closed" });
      timer = setTimeout(() => {
        timer = undefined;
        attempt += 1;
        void connect();
      }, retryDelay(attempt));
    };

    void connect();
    return () => {
      controller.abort();
      if (timer !== undefined) clearTimeout(timer);
      socket?.close();
      socketRef.current = undefined;
    };
  }, []);

  useEffect(() => {
    if (!state.needsResnapshot) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;

    const resnapshot = async () => {
      try {
        const [health, snapshot, entities] = await Promise.all([
          daemonApi.health(controller.signal),
          daemonApi.snapshot(controller.signal),
          daemonApi.entities(controller.signal),
        ]);
        if (!controller.signal.aborted)
          dispatch({ type: "snapshot", snapshot, health, entities });
      } catch (error) {
        if (!controller.signal.aborted) {
          dispatch({ type: "continuity_lost", error: String(error) });
          timer = setTimeout(() => void resnapshot(), 500);
        }
      }
    };

    void resnapshot();
    return () => {
      controller.abort();
      if (timer !== undefined) clearTimeout(timer);
    };
  }, [state.needsResnapshot]);

  useEffect(() => {
    if (state.invalidated.entities === undefined) return;
    const controller = new AbortController();
    void daemonApi
      .entities(controller.signal)
      .then((entities) => {
        dispatch({ type: "entity_index", entities });
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted)
          dispatch({ type: "continuity_lost", error: String(error) });
      });
    return () => {
      controller.abort();
    };
  }, [state.invalidated.entities]);

  return <DaemonContext value={state}>{children}</DaemonContext>;
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
