/* eslint-disable react-refresh/only-export-components */
import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useReducer,
  useRef,
} from "react";

import { daemonApi, daemonUrl } from "./api";
import {
  type AutomationStatus,
  type DaemonHealth,
  type DomainSlice,
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

export interface DaemonState {
  connection: ConnectionState;
  revision: number | null;
  galaxyRevision: number;
  syncing: boolean;
  health: DaemonHealth | null;
  sync: RuntimeSyncStatus | null;
  automation: AutomationStatus;
  entities: Record<string, unknown>;
  workflows: Record<string, WorkflowSummary>;
  requirements: RequirementSummary[];
  activity: WorkflowActivity[];
  notifications: Notification[];
  operations: Record<string, OperationUpdate>;
  invalidated: DomainSlice[];
  needsResnapshot: boolean;
  error: string | null;
}

export type DaemonAction =
  | { type: "connecting"; retry: boolean }
  | { type: "connected" }
  | { type: "disconnected"; error: string }
  | { type: "snapshot"; snapshot: RuntimeSnapshot; health: DaemonHealth }
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
  invalidated: [],
  needsResnapshot: false,
  error: null,
};

function key(kind: string, id: string) {
  return `${kind}:${id}`;
}

function continuityLost(state: DaemonState, error: string): DaemonState {
  return { ...state, syncing: true, needsResnapshot: true, error };
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
        entities: {},
        workflows: Object.fromEntries(
          action.snapshot.workflows.map((workflow) => [workflow.id, workflow]),
        ),
        requirements: action.snapshot.requirements,
        activity: [],
        notifications: action.snapshot.notifications,
        operations: {},
        invalidated: [],
        needsResnapshot: false,
        error: null,
      };
    case "live": {
      const { message } = action;
      if (state.needsResnapshot) return state;
      if (message.delta.type === "snapshot") {
        return message.revision === state.revision
          ? state
          : continuityLost(state, "A newer daemon snapshot is available");
      }
      if (state.revision === null || message.revision !== state.revision + 1) {
        return continuityLost(state, "Live revision continuity was lost");
      }

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
            invalidated: state.invalidated.includes(message.delta.data.slice)
              ? state.invalidated
              : [...state.invalidated, message.delta.data.slice],
          };
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
          return { ...next, activity: [...state.activity, message.delta.data] };
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
            notifications: [
              ...state.notifications.filter(
                (item) => item.id !== notification.id,
              ),
              notification,
            ],
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
        const [health, snapshot] = await Promise.all([
          daemonApi.health(controller.signal),
          daemonApi.snapshot(controller.signal),
        ]);
        if (controller.signal.aborted) return;
        dispatch({ type: "snapshot", snapshot, health });
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
            dispatch({
              type: "live",
              message: parseLiveMessage(JSON.parse(event.data) as unknown),
            });
          } catch (error) {
            dispatch({ type: "continuity_lost", error: String(error) });
          }
        });
        socket.addEventListener("close", scheduleReconnect);
        socket.addEventListener("error", () => socket?.close());
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
        const [health, snapshot] = await Promise.all([
          daemonApi.health(controller.signal),
          daemonApi.snapshot(controller.signal),
        ]);
        if (!controller.signal.aborted)
          dispatch({ type: "snapshot", snapshot, health });
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
