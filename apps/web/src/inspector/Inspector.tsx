import { useEffect, useMemo, useState } from "react";

import { daemonApi } from "../api";
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "../CommandPalette";
import { DeviceLogPanel } from "../DeviceLogPanel";
import type {
  CargoSnapshot,
  DescriptorCatalog,
  DeviceSummary,
  EntitySummary,
  EventSummary,
  GalaxyStar,
  InventorySnapshot,
  SystemMarker,
  WorkflowSummary,
} from "../protocol";
import type { SelectedEntity } from "../shellState";
import { InspectorShell } from "./InspectorShell";

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
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

export function relatedDeviceLabel(
  code: string,
  entities: Record<string, EntitySummary>,
) {
  const related = entities[`device:${code}`];
  return related?.entity_type ? `${related.entity_type} (${code})` : code;
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

function isGalaxyStar(value: unknown): value is GalaxyStar {
  const record = asRecord(value);
  const position = asRecord(record?.position);
  return (
    typeof record?.id === "string" &&
    typeof position?.x === "number" &&
    typeof position.y === "number" &&
    typeof position.z === "number"
  );
}

function isSystemMarker(value: unknown): value is SystemMarker {
  const record = asRecord(value);
  return (
    typeof record?.id === "string" &&
    typeof record.label === "string" &&
    typeof record.location === "string" &&
    asRecord(record.entity) !== null
  );
}

function readableFieldName(value: string) {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function formatInspectorValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return value.toString();
  }
  if (typeof value === "symbol") return value.toString();
  if (typeof value === "function") return value.name || "function";
  return "—";
}

function StructuredDetails({ value }: { value: Record<string, unknown> }) {
  const entries = Object.entries(value).filter(
    ([, item]) => item !== null && item !== undefined,
  );
  if (!entries.length) return <p>No additional details.</p>;
  return (
    <dl>
      {entries.map(([key, item]) => (
        <div key={key} className="inspector-structured-row">
          <dt>{readableFieldName(key)}</dt>
          <dd>
            {Array.isArray(item)
              ? item
                  .map((entry) =>
                    typeof entry === "object" && entry !== null
                      ? JSON.stringify(entry)
                      : String(entry),
                  )
                  .join(", ") || "—"
              : typeof item === "object"
                ? Object.entries(item as Record<string, unknown>)
                    .map(
                      ([childKey, child]) =>
                        `${readableFieldName(childKey)}: ${String(child)}`,
                    )
                    .join(" · ")
                : formatInspectorValue(item)}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function aggregateInventory(
  inventory: InventorySnapshot | undefined,
  predicate: (row: InventorySnapshot["locations"][number]) => boolean,
) {
  const resources = new Map<string, number>();
  for (const row of inventory?.locations.filter(predicate) ?? []) {
    for (const item of row.resources) {
      resources.set(
        item.resource,
        (resources.get(item.resource) ?? 0) + item.quantity,
      );
    }
  }
  return [...resources.entries()]
    .map(([resource, quantity]) => ({ resource, quantity }))
    .sort(
      (left, right) =>
        right.quantity - left.quantity ||
        left.resource.localeCompare(right.resource),
    );
}

function commandFitsDevice(command: DescriptorCommand, device: DeviceSummary) {
  const kind = command.descriptor.kind;
  const deviceType = device.device_type?.toLowerCase() ?? "";
  const advertised = new Set(device.available_commands);
  const supports = (commandName: string) =>
    advertised.size === 0 || advertised.has(commandName);
  if (kind === "autofactory.print") {
    return deviceType === "autofactory" && supports("enqueue_print");
  }
  if (kind === "device.stow") return supports("stow");
  if (kind === "device.attach") return supports("attach");
  if (kind === "device.detach") return supports("detach");
  if (kind === "device.adopt" || kind === "device.release") {
    return deviceType.startsWith("ami_") && supports(kind.slice(7));
  }
  if (kind === "device.set_directive") {
    return (
      (device.available_directives ?? []).length > 0 &&
      supports("set_directive")
    );
  }
  if (kind === "device.repair") return supports("repair");
  if (kind === "device.change_owner") return supports("change_owner");
  if (kind === "device.travel") return supports("travel");
  if (kind.startsWith("observatory."))
    return deviceType === "galactic_observatory";
  if (kind.startsWith("hub.")) return deviceType === "system_hub";
  if (kind.startsWith("trade.")) return deviceType.includes("trade_controller");
  if (kind.startsWith("simulation."))
    return deviceType.includes("replicant_interface");
  if (kind.startsWith("clone.")) return deviceType.includes("replicant_matrix");
  return true;
}

export function specializeDeviceCommand(
  command: DescriptorCommand,
  device: DeviceSummary,
  entities: Record<string, EntitySummary> = {},
): DescriptorCommand {
  if (
    command.descriptor.kind === "device.detach" ||
    command.descriptor.kind === "device.release"
  ) {
    const targets =
      command.descriptor.kind === "device.detach"
        ? device.attached_devices
        : device.controlled_devices;
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "target"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: targets.map((code) => ({
                  value: code,
                  label: relatedDeviceLabel(code, entities),
                })),
              }
            : parameter,
        ),
      },
    };
  }
  if (command.descriptor.kind === "device.adopt") {
    const targets = Object.keys(entities)
      .filter(
        (key) => key.startsWith("device:") && key.slice(7) !== device.entity.id,
      )
      .map((key) => key.slice(7));
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "target"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: targets.map((code) => ({
                  value: code,
                  label: relatedDeviceLabel(code, entities),
                })),
              }
            : parameter,
        ),
      },
    };
  }
  if (command.descriptor.kind === "device.set_directive") {
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "directive"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: (device.available_directives ?? []).map(
                  (directive) => ({
                    value: directive,
                    label: directive.replaceAll("_", " "),
                  }),
                ),
              }
            : parameter,
        ),
      },
    };
  }
  if (
    command.descriptor.kind !== "device.lifecycle" ||
    device.available_commands.length === 0
  ) {
    return command;
  }
  const supported = new Set(device.available_commands);
  const descriptor = {
    ...command.descriptor,
    parameters: command.descriptor.parameters.map((parameter) =>
      parameter.name === "command"
        ? {
            ...parameter,
            options: parameter.options.filter(
              (option) =>
                supported.has(option.value) ||
                (option.value === "retrieve" && device.stowed_in !== null),
            ),
          }
        : parameter,
    ),
  };
  return { ...command, descriptor };
}

export function Inspector({
  entity,
  value,
  descriptors,
  entities,
  onClose,
  onClear,
  onOpenGalaxy,
  onOpenSystem,
  onOpenWorkflow,
  onRunCommand,
}: {
  entity: SelectedEntity;
  value: unknown;
  descriptors: DescriptorCatalog;
  entities: Record<string, EntitySummary>;
  onClose: () => void;
  onClear: () => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onOpenWorkflow: (workflowId: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [inventory, setInventory] = useState<InventorySnapshot>();
  const [cargo, setCargo] = useState<CargoSnapshot>();
  const [detailError, setDetailError] = useState<string | null>(null);
  const device = isDeviceSummary(value) ? value : undefined;
  const workflow = !device && isWorkflowSummary(value) ? value : undefined;
  const event =
    !device && !workflow && isEventSummary(value) ? value : undefined;
  const summary =
    !device && !workflow && !event && isEntitySummary(value)
      ? value
      : undefined;
  const galaxyStar = isGalaxyStar(value) ? value : undefined;
  const marker = isSystemMarker(value) ? value : undefined;
  const deviceActions = useMemo(
    () =>
      device
        ? applicableDescriptorCommands(descriptors, "device")
            .filter(
              (command) =>
                command.operationClass === "action" &&
                commandFitsDevice(command, device),
            )
            .map((command) =>
              specializeDeviceCommand(command, device, entities),
            )
            .filter(
              (command) =>
                command.descriptor.kind !== "device.lifecycle" ||
                command.descriptor.parameters.some(
                  (parameter) =>
                    parameter.name === "command" &&
                    parameter.options.length > 0,
                ),
            )
        : [],
    [descriptors, device, entities],
  );
  useEffect(() => {
    setInventory(undefined);
    setCargo(undefined);
    setDetailError(null);
    const controller = new AbortController();
    const tasks: Promise<void>[] = [];
    if (["system", "location", "resource"].includes(entity.kind)) {
      tasks.push(
        daemonApi
          .inventory(controller.signal)
          .then(setInventory)
          .catch((error: unknown) => {
            if (!controller.signal.aborted) setDetailError(String(error));
          }),
      );
    }
    if (entity.kind === "device") {
      tasks.push(
        daemonApi
          .cargo(controller.signal)
          .then(setCargo)
          .catch((error: unknown) => {
            if (!controller.signal.aborted) setDetailError(String(error));
          }),
      );
    }
    void Promise.all(tasks);
    return () => {
      controller.abort();
    };
  }, [entity.id, entity.kind]);

  const systemResources =
    entity.kind === "system"
      ? aggregateInventory(inventory, (row) => row.system === entity.id)
      : [];
  const locationResources =
    entity.kind === "location"
      ? aggregateInventory(inventory, (row) => row.location === entity.id)
      : [];
  const resourceDistribution =
    entity.kind === "resource"
      ? inventory?.resources.find((item) => item.resource === entity.id)
      : undefined;
  const carrier = device
    ? cargo?.carriers.find((item) => item.device.entity.id === device.entity.id)
    : undefined;
  const containedEntities =
    entity.kind === "system" || entity.kind === "location"
      ? Object.values(entities)
          .filter((item) =>
            entity.kind === "system"
              ? item.system === entity.id
              : item.location === entity.id,
          )
          .filter(
            (item) =>
              item.entity.kind === "device" || item.entity.kind === "replicant",
          )
          .sort((left, right) => left.label.localeCompare(right.label))
      : [];
  const targetSystem =
    entity.kind === "system"
      ? entity.id
      : (event?.system ?? device?.system ?? summary?.system ?? null);
  return (
    <InspectorShell
      kind={entity.kind}
      label={summary?.label ?? galaxyStar?.name ?? marker?.label ?? entity.id}
      onClose={onClose}
      onClear={onClear}
      actions={
        targetSystem || entity.kind === "workflow" ? (
          <>
            {targetSystem ? (
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
            ) : null}
            {entity.kind === "workflow" ? (
              <button
                onClick={() => {
                  onOpenWorkflow(entity.id);
                }}
              >
                Open in Automation
              </button>
            ) : null}
          </>
        ) : undefined
      }
    >
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
                  ? `Attached to ${relatedDeviceLabel(device.attached_to, entities)}`
                  : device.stowed_in
                    ? `Stowed in ${relatedDeviceLabel(device.stowed_in, entities)}`
                    : `Controlled by ${device.controller ?? "—"}`}
              </dd>
            </>
          )}
          {device.controlled_devices.length > 0 && (
            <>
              <dt>Controlled devices</dt>
              <dd>
                {device.controlled_devices.map((code) => (
                  <div key={code}>{relatedDeviceLabel(code, entities)}</div>
                ))}
              </dd>
            </>
          )}
          {device.attached_devices.length > 0 && (
            <>
              <dt>Attached devices</dt>
              <dd>
                {device.attached_devices.map((code) => (
                  <div key={code}>{relatedDeviceLabel(code, entities)}</div>
                ))}
              </dd>
            </>
          )}
          {device.stowed_devices.length > 0 && (
            <>
              <dt>Stowed devices</dt>
              <dd>
                {device.stowed_devices.map((code) => (
                  <div key={code}>{relatedDeviceLabel(code, entities)}</div>
                ))}
              </dd>
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
              <dt>Attach points</dt>
              <dd>
                {device.attached_devices.length} / {device.attach_capacity}
                {device.attach_capacity > device.attached_devices.length
                  ? ` · ${String(device.attach_capacity - device.attached_devices.length)} free`
                  : ""}
              </dd>
            </>
          )}
          {device.active_directive && (
            <>
              <dt>Directive</dt>
              <dd>
                {device.active_directive}
                {device.directive_status ? ` · ${device.directive_status}` : ""}
              </dd>
            </>
          )}
          {device.travel_destination && (
            <>
              <dt>Traveling to</dt>
              <dd>{device.travel_destination}</dd>
            </>
          )}
          {carrier?.resources.length ? (
            <>
              <dt>Stowed cargo</dt>
              <dd>
                {carrier.resources.map((item) => (
                  <div key={item.resource}>
                    {item.resource}: {item.quantity.toLocaleString()}
                  </div>
                ))}
              </dd>
            </>
          ) : null}
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
                {event.rewards.xp !== null && <div>{event.rewards.xp} XP</div>}
              </dd>
            </>
          )}
        </dl>
      ) : galaxyStar ? (
        <dl>
          <dt>Exploration</dt>
          <dd>{galaxyStar.exploration}</dd>
          <dt>Spectral type</dt>
          <dd>{galaxyStar.spectral_type ?? "Unknown"}</dd>
          <dt>Coordinates</dt>
          <dd>
            {galaxyStar.position.x.toFixed(2)},{" "}
            {galaxyStar.position.y.toFixed(2)},{" "}
            {galaxyStar.position.z.toFixed(2)} LY
          </dd>
          <dt>Infrastructure</dt>
          <dd>
            {[
              galaxyStar.has_hub ? "System Hub" : null,
              galaxyStar.has_relay ? "Relay" : null,
              galaxyStar.has_megastructure ? "Megastructure" : null,
            ]
              .filter(Boolean)
              .join(" · ") || "None known"}
          </dd>
          <dt>Life</dt>
          <dd>{galaxyStar.has_life ? "Detected" : "None known"}</dd>
        </dl>
      ) : marker ? (
        <dl>
          <dt>Kind</dt>
          <dd>{marker.kind}</dd>
          <dt>Location</dt>
          <dd>{marker.location}</dd>
          {marker.parent ? (
            <>
              <dt>Parent</dt>
              <dd>{marker.parent}</dd>
            </>
          ) : null}
          <dt>Map position</dt>
          <dd>
            {marker.position.x.toFixed(2)}, {marker.position.y.toFixed(2)}
          </dd>
          {marker.in_habitable_zone !== null ? (
            <>
              <dt>Habitable zone</dt>
              <dd>{marker.in_habitable_zone ? "Yes" : "No"}</dd>
            </>
          ) : null}
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
        <StructuredDetails value={asRecord(value) ?? { value }} />
      )}
      {entity.kind === "system" && inventory ? (
        <section className="inspector-section">
          <h3>Combined system resources</h3>
          {systemResources.length ? (
            <ul className="inspector-resource-list">
              {systemResources.map((item) => (
                <li key={item.resource}>
                  <span>{item.resource}</span>
                  <strong>{item.quantity.toLocaleString()}</strong>
                </li>
              ))}
            </ul>
          ) : (
            <p className="empty-state">No stored resources in this system.</p>
          )}
        </section>
      ) : null}
      {entity.kind === "location" && inventory ? (
        <section className="inspector-section">
          <h3>Location resources</h3>
          {locationResources.length ? (
            <ul className="inspector-resource-list">
              {locationResources.map((item) => (
                <li key={item.resource}>
                  <span>{item.resource}</span>
                  <strong>{item.quantity.toLocaleString()}</strong>
                </li>
              ))}
            </ul>
          ) : (
            <p className="empty-state">No stored resources at this location.</p>
          )}
        </section>
      ) : null}
      {containedEntities.length ? (
        <section className="inspector-section">
          <h3>Present assets</h3>
          <ul className="inspector-entity-list">
            {containedEntities.slice(0, 50).map((item) => (
              <li key={`${item.entity.kind}:${item.entity.id}`}>
                <span>
                  <strong>{item.label}</strong>
                  <small>{item.secondary_label ?? item.entity.kind}</small>
                </span>
                <span className="status-chip">{item.status ?? "present"}</span>
              </li>
            ))}
          </ul>
          {containedEntities.length > 50 ? (
            <small>
              Showing 50 of {containedEntities.length} present assets.
            </small>
          ) : null}
        </section>
      ) : null}
      {resourceDistribution ? (
        <section className="inspector-section">
          <h3>{resourceDistribution.resource}</h3>
          <p>{resourceDistribution.total_quantity.toLocaleString()} total</p>
          <ul className="inspector-resource-list">
            {resourceDistribution.distribution.map((item, index) => (
              <li
                key={`${item.owner}:${item.location ?? "none"}:${String(index)}`}
              >
                <span>{item.location ?? item.owner}</span>
                <strong>{item.quantity.toLocaleString()}</strong>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      {detailError ? (
        <p className="inline-warning">Detail refresh failed: {detailError}</p>
      ) : null}
      {device && deviceActions.length ? (
        <section className="inspector-section">
          <h3>Actions</h3>
          <div className="inspector-command-grid">
            {deviceActions.map((command) => (
              <button
                key={`${command.operationClass}:${command.descriptor.kind}`}
                onClick={() => {
                  onRunCommand(command);
                }}
              >
                {command.descriptor.display_name}
              </button>
            ))}
          </div>
        </section>
      ) : null}
      {device?.ownership.toLowerCase() === "owned" ? (
        <DeviceLogPanel device={device.entity.id} />
      ) : null}
    </InspectorShell>
  );
}
