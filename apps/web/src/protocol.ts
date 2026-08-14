export const PROTOCOL_VERSION = 1;

export type HealthStatus = "healthy" | "degraded" | "unhealthy";
export type SyncPhase =
  "starting" | "syncing" | "ready" | "degraded" | "offline";
export type WorkflowStatus =
  | "queued"
  | "running"
  | "waiting"
  | "paused"
  | "reconciling"
  | "succeeded"
  | "failed"
  | "cancelled";
export type EntityKind =
  | "system"
  | "location"
  | "replicant"
  | "device"
  | "inventory"
  | "autofactory"
  | "cargo"
  | "operation"
  | "workflow";
export type DomainSlice =
  | "universe"
  | "devices"
  | "inventory"
  | "autofactories"
  | "workflows"
  | "operations";

export interface DaemonHealth {
  status: HealthStatus;
  daemon_version: string;
  detail: string | null;
}

export interface RuntimeSyncStatus {
  phase: SyncPhase;
  revision: number;
  last_event_at_ms: number | null;
  detail: string | null;
}

export interface SnapshotMetadata {
  revision: number;
  generated_at_ms: number;
}

export interface WorkflowSummary {
  id: string;
  kind: string;
  status: WorkflowStatus;
  current_step: string | null;
  revision: number;
  updated_at_ms: number;
}

export interface RuntimeSnapshot {
  metadata: SnapshotMetadata;
  sync: RuntimeSyncStatus;
  workflows: WorkflowSummary[];
}

export interface EntityRef {
  kind: EntityKind;
  id: string;
}

export interface WorkflowActivity {
  id: number;
  workflow_id: string;
  occurred_at_ms: number;
  level: "debug" | "info" | "warning" | "error";
  step: string | null;
  message: string;
}

export interface OperationUpdate {
  id: string;
  workflow_id: string | null;
  status: "pending" | "running" | "succeeded" | "failed" | "ambiguous";
  message: string | null;
  updated_at_ms: number;
}

export interface Notification {
  id: string;
  level: "info" | "warning" | "error";
  title: string;
  message: string;
  created_at_ms: number;
}

export type LiveDelta =
  | { type: "snapshot"; data: SnapshotMetadata }
  | { type: "entity_upsert"; data: { entity: EntityRef; value: unknown } }
  | { type: "entity_remove"; data: { entity: EntityRef } }
  | { type: "domain_invalidated"; data: { slice: DomainSlice } }
  | { type: "workflow_created"; data: WorkflowSummary }
  | { type: "workflow_updated"; data: WorkflowSummary }
  | { type: "workflow_activity"; data: WorkflowActivity }
  | { type: "operation_updated"; data: OperationUpdate }
  | { type: "notification"; data: Notification }
  | {
      type: "daemon_status_changed";
      data: { health: DaemonHealth; sync: RuntimeSyncStatus };
    };

export interface LiveMessage {
  protocol_version: number;
  revision: number;
  delta: LiveDelta;
}

export interface Versioned<T> {
  protocol_version: number;
  payload: T;
}

function record(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null)
    throw new Error(`Invalid ${name}`);
  return value as Record<string, unknown>;
}

function oneOf<T extends string>(
  value: unknown,
  values: readonly T[],
  name: string,
): T {
  if (typeof value !== "string" || !values.includes(value as T))
    throw new Error(`Invalid ${name}`);
  return value as T;
}

function nullableString(value: unknown, name: string): string | null {
  if (value !== null && typeof value !== "string")
    throw new Error(`Invalid ${name}`);
  return value;
}

function number(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new Error(`Invalid ${name}`);
  return value;
}

const healthStatuses = ["healthy", "degraded", "unhealthy"] as const;
const syncPhases = [
  "starting",
  "syncing",
  "ready",
  "degraded",
  "offline",
] as const;
const workflowStatuses = [
  "queued",
  "running",
  "waiting",
  "paused",
  "reconciling",
  "succeeded",
  "failed",
  "cancelled",
] as const;
const entityKinds = [
  "system",
  "location",
  "replicant",
  "device",
  "inventory",
  "autofactory",
  "cargo",
  "operation",
  "workflow",
] as const;
const domainSlices = [
  "universe",
  "devices",
  "inventory",
  "autofactories",
  "workflows",
  "operations",
] as const;

function health(value: unknown): DaemonHealth {
  const item = record(value, "daemon health");
  if (typeof item.daemon_version !== "string")
    throw new Error("Invalid daemon version");
  return {
    status: oneOf(item.status, healthStatuses, "health status"),
    daemon_version: item.daemon_version,
    detail: nullableString(item.detail, "health detail"),
  };
}

function sync(value: unknown): RuntimeSyncStatus {
  const item = record(value, "sync status");
  return {
    phase: oneOf(item.phase, syncPhases, "sync phase"),
    revision: number(item.revision, "sync revision"),
    last_event_at_ms:
      item.last_event_at_ms === null
        ? null
        : number(item.last_event_at_ms, "last event time"),
    detail: nullableString(item.detail, "sync detail"),
  };
}

function metadata(value: unknown): SnapshotMetadata {
  const item = record(value, "snapshot metadata");
  return {
    revision: number(item.revision, "snapshot revision"),
    generated_at_ms: number(item.generated_at_ms, "snapshot time"),
  };
}

function workflow(value: unknown): WorkflowSummary {
  const item = record(value, "workflow");
  if (typeof item.id !== "string" || typeof item.kind !== "string")
    throw new Error("Invalid workflow identity");
  return {
    id: item.id,
    kind: item.kind,
    status: oneOf(item.status, workflowStatuses, "workflow status"),
    current_step: nullableString(item.current_step, "workflow step"),
    revision: number(item.revision, "workflow revision"),
    updated_at_ms: number(item.updated_at_ms, "workflow update time"),
  };
}

function entity(value: unknown): EntityRef {
  const item = record(value, "entity reference");
  if (typeof item.id !== "string") throw new Error("Invalid entity id");
  return { kind: oneOf(item.kind, entityKinds, "entity kind"), id: item.id };
}

function activity(value: unknown): WorkflowActivity {
  const item = record(value, "workflow activity");
  if (typeof item.workflow_id !== "string" || typeof item.message !== "string")
    throw new Error("Invalid workflow activity");
  return {
    id: number(item.id, "activity id"),
    workflow_id: item.workflow_id,
    occurred_at_ms: number(item.occurred_at_ms, "activity time"),
    level: oneOf(
      item.level,
      ["debug", "info", "warning", "error"] as const,
      "activity level",
    ),
    step: nullableString(item.step, "activity step"),
    message: item.message,
  };
}

function operation(value: unknown): OperationUpdate {
  const item = record(value, "operation update");
  if (typeof item.id !== "string") throw new Error("Invalid operation id");
  return {
    id: item.id,
    workflow_id: nullableString(item.workflow_id, "operation workflow"),
    status: oneOf(
      item.status,
      ["pending", "running", "succeeded", "failed", "ambiguous"] as const,
      "operation status",
    ),
    message: nullableString(item.message, "operation message"),
    updated_at_ms: number(item.updated_at_ms, "operation update time"),
  };
}

function notification(value: unknown): Notification {
  const item = record(value, "notification");
  if (
    typeof item.id !== "string" ||
    typeof item.title !== "string" ||
    typeof item.message !== "string"
  )
    throw new Error("Invalid notification");
  return {
    id: item.id,
    level: oneOf(
      item.level,
      ["info", "warning", "error"] as const,
      "notification level",
    ),
    title: item.title,
    message: item.message,
    created_at_ms: number(item.created_at_ms, "notification time"),
  };
}

function envelope<T>(
  value: unknown,
  parse: (payload: unknown) => T,
): Versioned<T> {
  const item = record(value, "daemon response");
  if (item.protocol_version !== PROTOCOL_VERSION)
    throw new Error("Unsupported daemon protocol version");
  return { protocol_version: PROTOCOL_VERSION, payload: parse(item.payload) };
}

export function parseHealthResponse(value: unknown): Versioned<DaemonHealth> {
  return envelope(value, health);
}

export function parseSnapshotResponse(
  value: unknown,
): Versioned<RuntimeSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "runtime snapshot");
    if (!Array.isArray(item.workflows))
      throw new Error("Invalid snapshot workflows");
    return {
      metadata: metadata(item.metadata),
      sync: sync(item.sync),
      workflows: item.workflows.map(workflow),
    };
  });
}

export function parseLiveMessage(value: unknown): LiveMessage {
  const item = record(value, "live message");
  if (item.protocol_version !== PROTOCOL_VERSION)
    throw new Error("Unsupported daemon protocol version");
  const revision = number(item.revision, "live revision");
  const raw = record(item.delta, "live delta");
  const type = raw.type;
  if (typeof type !== "string") throw new Error("Invalid live delta type");
  const data = raw.data;
  let delta: LiveDelta;
  switch (type) {
    case "snapshot":
      delta = { type, data: metadata(data) };
      break;
    case "entity_upsert": {
      const value = record(data, "entity upsert");
      delta = {
        type,
        data: { entity: entity(value.entity), value: value.value },
      };
      break;
    }
    case "entity_remove": {
      const value = record(data, "entity removal");
      delta = { type, data: { entity: entity(value.entity) } };
      break;
    }
    case "domain_invalidated": {
      const value = record(data, "domain invalidation");
      delta = {
        type,
        data: { slice: oneOf(value.slice, domainSlices, "domain slice") },
      };
      break;
    }
    case "workflow_created":
    case "workflow_updated":
      delta = { type, data: workflow(data) };
      break;
    case "workflow_activity":
      delta = { type, data: activity(data) };
      break;
    case "operation_updated":
      delta = { type, data: operation(data) };
      break;
    case "notification":
      delta = { type, data: notification(data) };
      break;
    case "daemon_status_changed": {
      const value = record(data, "daemon status");
      delta = {
        type,
        data: { health: health(value.health), sync: sync(value.sync) },
      };
      break;
    }
    default:
      throw new Error(`Unsupported live delta: ${type}`);
  }
  return { protocol_version: PROTOCOL_VERSION, revision, delta };
}
