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

export type ParameterKind =
  | { type: "string" | "integer" | "number" | "boolean" | "enum" }
  | {
      type:
        "system" | "location" | "replicant" | "device" | "device_type" | "tag";
    }
  | { type: "entity"; entity_kind: EntityKind };

export interface ParameterDescriptor {
  name: string;
  label: string;
  description: string;
  kind: ParameterKind;
  required: boolean;
  default: unknown;
  options: { value: string; label: string }[];
  validation: {
    minimum: number | null;
    maximum: number | null;
    min_length: number | null;
    max_length: number | null;
  };
}

export interface WorkflowDescriptor {
  kind: string;
  display_name: string;
  description: string;
  category: string;
  risk: "none" | "low" | "elevated";
  parameters: ParameterDescriptor[];
  supported_triggers: (
    "manual" | "schedule" | "game_event" | "state_condition" | "parent_workflow"
  )[];
}

export interface DescriptorCatalog {
  workflows: WorkflowDescriptor[];
}

export interface WorkflowDetail {
  summary: WorkflowSummary;
  schema_version: number;
  parameters: Record<string, unknown>;
  wait_reason: string | null;
  parent_id: string | null;
  claims: EntityRef[];
  created_at_ms: number;
  finished_at_ms: number | null;
  error: string | null;
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

function optionalFiniteNumber(value: unknown, name: string): number | null {
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value))
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

function parameter(value: unknown): ParameterDescriptor {
  const item = record(value, "parameter descriptor");
  const rawKind = record(item.kind, "parameter kind");
  const kindType = oneOf(
    rawKind.type,
    [
      "string",
      "integer",
      "number",
      "boolean",
      "enum",
      "system",
      "location",
      "replicant",
      "device",
      "device_type",
      "tag",
      "entity",
    ] as const,
    "parameter kind",
  );
  const kind: ParameterKind =
    kindType === "entity"
      ? {
          type: kindType,
          entity_kind: oneOf(
            rawKind.entity_kind,
            entityKinds,
            "parameter entity kind",
          ),
        }
      : { type: kindType };
  if (
    typeof item.name !== "string" ||
    typeof item.label !== "string" ||
    typeof item.description !== "string" ||
    typeof item.required !== "boolean" ||
    !Array.isArray(item.options)
  )
    throw new Error("Invalid parameter descriptor");
  const validation = record(item.validation, "parameter validation");
  return {
    name: item.name,
    label: item.label,
    description: item.description,
    kind,
    required: item.required,
    default: item.default,
    options: item.options.map((option) => {
      const value = record(option, "parameter option");
      if (typeof value.value !== "string" || typeof value.label !== "string")
        throw new Error("Invalid parameter option");
      return { value: value.value, label: value.label };
    }),
    validation: {
      minimum: optionalFiniteNumber(validation.minimum, "parameter minimum"),
      maximum: optionalFiniteNumber(validation.maximum, "parameter maximum"),
      min_length:
        validation.min_length === null
          ? null
          : number(validation.min_length, "parameter minimum length"),
      max_length:
        validation.max_length === null
          ? null
          : number(validation.max_length, "parameter maximum length"),
    },
  };
}

function descriptor(value: unknown): WorkflowDescriptor {
  const item = record(value, "workflow descriptor");
  if (
    typeof item.kind !== "string" ||
    typeof item.display_name !== "string" ||
    typeof item.description !== "string" ||
    typeof item.category !== "string" ||
    !Array.isArray(item.parameters) ||
    !Array.isArray(item.supported_triggers)
  )
    throw new Error("Invalid workflow descriptor");
  return {
    kind: item.kind,
    display_name: item.display_name,
    description: item.description,
    category: item.category,
    risk: oneOf(item.risk, ["none", "low", "elevated"] as const, "risk"),
    parameters: item.parameters.map(parameter),
    supported_triggers: item.supported_triggers.map((trigger) =>
      oneOf(
        trigger,
        [
          "manual",
          "schedule",
          "game_event",
          "state_condition",
          "parent_workflow",
        ] as const,
        "workflow trigger",
      ),
    ),
  };
}

function workflowDetail(value: unknown): WorkflowDetail {
  const item = record(value, "workflow detail");
  if (!Array.isArray(item.claims)) throw new Error("Invalid workflow claims");
  return {
    summary: workflow(item.summary),
    schema_version: number(item.schema_version, "workflow schema version"),
    parameters: record(item.parameters, "workflow parameters"),
    wait_reason: nullableString(item.wait_reason, "workflow wait reason"),
    parent_id: nullableString(item.parent_id, "workflow parent"),
    claims: item.claims.map(entity),
    created_at_ms: number(item.created_at_ms, "workflow creation time"),
    finished_at_ms:
      item.finished_at_ms === null
        ? null
        : number(item.finished_at_ms, "workflow finish time"),
    error: nullableString(item.error, "workflow error"),
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

export function parseDescriptorsResponse(
  value: unknown,
): Versioned<DescriptorCatalog> {
  return envelope(value, (payload) => {
    const item = record(payload, "descriptor catalog");
    if (!Array.isArray(item.workflows))
      throw new Error("Invalid workflow descriptors");
    return { workflows: item.workflows.map(descriptor) };
  });
}

export function parseWorkflowResponse(
  value: unknown,
): Versioned<WorkflowSummary> {
  return envelope(value, (payload) => {
    const item = record(payload, "workflow response");
    return workflow(item.workflow);
  });
}

export function parseWorkflowDetailResponse(
  value: unknown,
): Versioned<WorkflowDetail> {
  return envelope(value, workflowDetail);
}

export function parseWorkflowActivityResponse(
  value: unknown,
): Versioned<WorkflowActivity[]> {
  return envelope(value, (payload) => {
    const item = record(payload, "workflow activity response");
    if (!Array.isArray(item.activity)) throw new Error("Invalid activity list");
    return item.activity.map(activity);
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
