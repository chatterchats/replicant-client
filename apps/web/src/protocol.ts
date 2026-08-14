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

interface Descriptor {
  kind: string;
  display_name: string;
  aliases: string[];
  description: string;
  category: string;
  operation_class: "report" | "action" | "workflow";
  applicable_to: EntityKind[];
  parameters: ParameterDescriptor[];
}

export interface ReportDescriptor extends Descriptor {
  operation_class: "report";
  risk: "none";
}

export interface ActionDescriptor extends Descriptor {
  operation_class: "action";
  risk: "none" | "low" | "elevated";
}

export interface WorkflowDescriptor extends Descriptor {
  operation_class: "workflow";
  risk: "none" | "low" | "elevated";
  supported_triggers: (
    "manual" | "schedule" | "game_event" | "state_condition" | "parent_workflow"
  )[];
}

export interface DescriptorCatalog {
  reports: ReportDescriptor[];
  actions: ActionDescriptor[];
  workflows: WorkflowDescriptor[];
}

export type OperationDescriptor =
  ReportDescriptor | ActionDescriptor | WorkflowDescriptor;

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

export interface GalaxyPoint {
  x: number;
  y: number;
  z: number;
}

export interface GalaxyStar {
  id: string;
  name: string | null;
  spectral_type: string | null;
  position: GalaxyPoint;
  exploration: "undiscovered" | "partial" | "explored";
  current: boolean;
  has_hub: boolean;
  has_life: boolean;
  has_relay: boolean;
}

export interface GalaxySceneSnapshot {
  revision: number;
  generated_at_ms: number;
  stars: GalaxyStar[];
  relay_edges: { from: string; to: string }[];
  active_travel: {
    entity: EntityRef;
    from: string;
    to: string;
    started_at: string | null;
    arrives_at: string | null;
  }[];
  signals: { id: string; label: string | null; position: GalaxyPoint }[];
  highlights: { workflow_id: string; from: string; to: string }[];
  overlays: {
    kind: "life" | "device" | "influence";
    system: string;
    position: GalaxyPoint;
    count: number;
  }[];
  workflow_targets: {
    workflow_id: string;
    workflow_kind: string;
    system: string;
  }[];
}

export type SystemMarkerKind =
  | "star"
  | "planet"
  | "moon"
  | "belt"
  | "lagrange"
  | "location"
  | "vessel"
  | "device"
  | "factory"
  | "relay"
  | "event"
  | "resource_site";

export interface SystemPoint {
  x: number;
  y: number;
}

export interface SystemMarker {
  id: string;
  label: string;
  kind: SystemMarkerKind;
  entity: EntityRef;
  location: string;
  parent: string | null;
  in_habitable_zone: boolean | null;
  position: SystemPoint;
  count: number;
}

export interface SystemSceneSnapshot {
  system: string;
  revision: number;
  generated_at_ms: number;
  markers: SystemMarker[];
  active_travel: {
    entity: EntityRef;
    from: string;
    to: string;
    started_at: string | null;
    arrives_at: string | null;
  }[];
  workflow_markers: {
    workflow_id: string;
    workflow_kind: string;
    location: string;
  }[];
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

function finiteNumber(value: unknown, name: string): number {
  const parsed = optionalFiniteNumber(value, name);
  if (parsed === null) throw new Error(`Invalid ${name}`);
  return parsed;
}

function array(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`Invalid ${name}`);
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

function descriptor(value: unknown, label: string) {
  const item = record(value, label);
  if (
    typeof item.kind !== "string" ||
    typeof item.display_name !== "string" ||
    !Array.isArray(item.aliases) ||
    !item.aliases.every((alias) => typeof alias === "string") ||
    typeof item.description !== "string" ||
    typeof item.category !== "string" ||
    !Array.isArray(item.applicable_to) ||
    !Array.isArray(item.parameters)
  )
    throw new Error(`Invalid ${label}`);
  return {
    kind: item.kind,
    display_name: item.display_name,
    aliases: item.aliases,
    description: item.description,
    category: item.category,
    operation_class: oneOf(
      item.operation_class,
      ["report", "action", "workflow"] as const,
      "operation class",
    ),
    applicable_to: item.applicable_to.map((kind) =>
      oneOf(kind, entityKinds, "applicable entity kind"),
    ),
    parameters: item.parameters.map(parameter),
  };
}

function workflowDescriptor(value: unknown): WorkflowDescriptor {
  const item = record(value, "workflow descriptor");
  if (!Array.isArray(item.supported_triggers))
    throw new Error("Invalid workflow descriptor");
  return {
    ...descriptor(value, "workflow descriptor"),
    operation_class: oneOf(
      item.operation_class,
      ["workflow"] as const,
      "workflow operation class",
    ),
    risk: oneOf(item.risk, ["none", "low", "elevated"] as const, "risk"),
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

function point(value: unknown): GalaxyPoint {
  const item = record(value, "galaxy point");
  return {
    x: finiteNumber(item.x, "galaxy x"),
    y: finiteNumber(item.y, "galaxy y"),
    z: finiteNumber(item.z, "galaxy z"),
  };
}

function stringPair(value: unknown, name: string) {
  const item = record(value, name);
  if (typeof item.from !== "string" || typeof item.to !== "string")
    throw new Error(`Invalid ${name}`);
  return { from: item.from, to: item.to };
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

export function parseGalaxySceneResponse(
  value: unknown,
): Versioned<GalaxySceneSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "galaxy scene");
    return {
      revision: number(item.revision, "galaxy scene revision"),
      generated_at_ms: number(item.generated_at_ms, "galaxy scene time"),
      stars: array(item.stars, "galaxy scene stars").map((value) => {
        const star = record(value, "galaxy star");
        if (
          typeof star.id !== "string" ||
          typeof star.current !== "boolean" ||
          typeof star.has_hub !== "boolean" ||
          typeof star.has_life !== "boolean" ||
          typeof star.has_relay !== "boolean"
        )
          throw new Error("Invalid galaxy star");
        return {
          id: star.id,
          name: nullableString(star.name, "galaxy star name"),
          spectral_type: nullableString(
            star.spectral_type,
            "galaxy spectral type",
          ),
          position: point(star.position),
          exploration: oneOf(
            star.exploration,
            ["undiscovered", "partial", "explored"] as const,
            "galaxy exploration",
          ),
          current: star.current,
          has_hub: star.has_hub,
          has_life: star.has_life,
          has_relay: star.has_relay,
        };
      }),
      relay_edges: array(item.relay_edges, "galaxy relay edges").map((value) =>
        stringPair(value, "relay edge"),
      ),
      active_travel: array(item.active_travel, "galaxy travel").map((value) => {
        const travel = record(value, "active travel");
        return {
          entity: entity(travel.entity),
          ...stringPair(value, "active travel"),
          started_at: nullableString(travel.started_at, "travel start"),
          arrives_at: nullableString(travel.arrives_at, "travel arrival"),
        };
      }),
      signals: array(item.signals, "galaxy signals").map((value) => {
        const signal = record(value, "galaxy signal");
        if (typeof signal.id !== "string")
          throw new Error("Invalid galaxy signal");
        return {
          id: signal.id,
          label: nullableString(signal.label, "signal label"),
          position: point(signal.position),
        };
      }),
      highlights: array(item.highlights, "galaxy highlights").map((value) => {
        const highlight = record(value, "galaxy highlight");
        if (typeof highlight.workflow_id !== "string")
          throw new Error("Invalid galaxy highlight");
        return {
          workflow_id: highlight.workflow_id,
          ...stringPair(value, "galaxy highlight"),
        };
      }),
      overlays: array(item.overlays, "galaxy overlays").map((value) => {
        const overlay = record(value, "galaxy overlay");
        if (typeof overlay.system !== "string")
          throw new Error("Invalid galaxy overlay");
        return {
          kind: oneOf(
            overlay.kind,
            ["life", "device", "influence"] as const,
            "galaxy overlay kind",
          ),
          system: overlay.system,
          position: point(overlay.position),
          count: number(overlay.count, "galaxy overlay count"),
        };
      }),
      workflow_targets: array(item.workflow_targets, "workflow targets").map(
        (value) => {
          const target = record(value, "workflow target");
          if (
            typeof target.workflow_id !== "string" ||
            typeof target.workflow_kind !== "string" ||
            typeof target.system !== "string"
          )
            throw new Error("Invalid workflow target");
          return {
            workflow_id: target.workflow_id,
            workflow_kind: target.workflow_kind,
            system: target.system,
          };
        },
      ),
    };
  });
}

export function parseSystemSceneResponse(
  value: unknown,
): Versioned<SystemSceneSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "system scene");
    if (typeof item.system !== "string")
      throw new Error("Invalid system scene");
    return {
      system: item.system,
      revision: number(item.revision, "system scene revision"),
      generated_at_ms: number(item.generated_at_ms, "system scene time"),
      markers: array(item.markers, "system markers").map((value) => {
        const marker = record(value, "system marker");
        if (
          typeof marker.id !== "string" ||
          typeof marker.label !== "string" ||
          typeof marker.location !== "string" ||
          (marker.in_habitable_zone !== null &&
            typeof marker.in_habitable_zone !== "boolean")
        )
          throw new Error("Invalid system marker");
        const position = record(marker.position, "system marker position");
        return {
          id: marker.id,
          label: marker.label,
          kind: oneOf(
            marker.kind,
            [
              "star",
              "planet",
              "moon",
              "belt",
              "lagrange",
              "location",
              "vessel",
              "device",
              "factory",
              "relay",
              "event",
              "resource_site",
            ] as const,
            "system marker kind",
          ),
          entity: entity(marker.entity),
          location: marker.location,
          parent: nullableString(marker.parent, "system marker parent"),
          in_habitable_zone: marker.in_habitable_zone,
          position: {
            x: finiteNumber(position.x, "system marker x"),
            y: finiteNumber(position.y, "system marker y"),
          },
          count: number(marker.count, "system marker count"),
        };
      }),
      active_travel: array(item.active_travel, "system travel").map((value) => {
        const travel = record(value, "system travel");
        return {
          entity: entity(travel.entity),
          ...stringPair(value, "system travel"),
          started_at: nullableString(travel.started_at, "travel start"),
          arrives_at: nullableString(travel.arrives_at, "travel arrival"),
        };
      }),
      workflow_markers: array(
        item.workflow_markers,
        "system workflow markers",
      ).map((value) => {
        const marker = record(value, "system workflow marker");
        if (
          typeof marker.workflow_id !== "string" ||
          typeof marker.workflow_kind !== "string" ||
          typeof marker.location !== "string"
        )
          throw new Error("Invalid system workflow marker");
        return {
          workflow_id: marker.workflow_id,
          workflow_kind: marker.workflow_kind,
          location: marker.location,
        };
      }),
    };
  });
}

export function parseDescriptorsResponse(
  value: unknown,
): Versioned<DescriptorCatalog> {
  return envelope(value, (payload) => {
    const item = record(payload, "descriptor catalog");
    if (
      !Array.isArray(item.reports) ||
      !Array.isArray(item.actions) ||
      !Array.isArray(item.workflows)
    )
      throw new Error("Invalid descriptors");
    return {
      reports: item.reports.map((value) => {
        const parsed = descriptor(value, "report descriptor");
        const item = record(value, "report descriptor");
        return {
          ...parsed,
          operation_class: oneOf(
            item.operation_class,
            ["report"] as const,
            "report operation class",
          ),
          risk: oneOf(item.risk, ["none"] as const, "report risk"),
        };
      }),
      actions: item.actions.map((value) => {
        const parsed = descriptor(value, "action descriptor");
        const item = record(value, "action descriptor");
        return {
          ...parsed,
          operation_class: oneOf(
            item.operation_class,
            ["action"] as const,
            "action operation class",
          ),
          risk: oneOf(item.risk, ["none", "low", "elevated"] as const, "risk"),
        };
      }),
      workflows: item.workflows.map(workflowDescriptor),
    };
  });
}

export function parseOperationResponse(value: unknown): Versioned<unknown> {
  return envelope(
    value,
    (payload) => record(payload, "operation response").result,
  );
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
