import { useEffect, useMemo, useState, type SyntheticEvent } from "react";

import {
  ParameterField,
  validateParameters,
  visibleParameters,
} from "../AutomationsPage";
import { daemonApi } from "../api";
import type { DescriptorCommand } from "../CommandPalette";
import type {
  DescriptorCatalog,
  DeviceInspectorSummary,
  DeviceSummary,
  BlueprintSummary,
  EntityCollectionSummary,
  EntitySummary,
  FiniteExecution,
  ParameterDescriptor,
} from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import { DeviceActivityPanel } from "./DeviceActivityPanel";
import { DeviceRolePanel } from "./DeviceRolePanel";
import { InspectorFields } from "./InspectorFields";
import { TravelSection } from "./TravelInspector";
import {
  advertisedDeviceCommands,
  relatedDeviceLabel,
  type DescriptorDeviceCommand,
} from "./inspectorModel";

function relationCollection(
  kind: string,
  codes: string[],
  entities: Record<string, EntitySummary>,
): EntityCollectionSummary {
  const items = codes.map((code): EntitySummary => {
    const existing = entities[`device:${code}`];
    if (existing) return existing;
    return {
      entity: { kind: "device", id: code },
      label: code,
      secondary_label: null,
      system: null,
      location: null,
      entity_type: null,
      status: null,
    };
  });
  return items.length <= 8
    ? { total: items.length, items, groups: [] }
    : {
        total: items.length,
        items: [],
        groups: [
          {
            entity_kind: "device",
            entity_type: kind,
            count: items.length,
            statuses: [],
          },
        ],
      };
}

function initialValues(
  command: DescriptorDeviceCommand,
): Record<string, unknown> {
  const values: Record<string, unknown> = {
    ...(command.initialParameters ?? {}),
  };
  for (const parameter of command.descriptor.parameters) {
    if (values[parameter.name] === undefined && parameter.default !== null) {
      values[parameter.name] = parameter.default;
    }
  }
  return values;
}

function normalizeValues(
  parameters: ParameterDescriptor[],
  values: Record<string, unknown>,
) {
  return Object.fromEntries(
    Object.entries(values).map(([name, value]) => {
      const kind = parameters.find((parameter) => parameter.name === name)?.kind
        .type;
      return [
        name,
        (kind === "integer" || kind === "number") && value !== ""
          ? Number(value)
          : value,
      ];
    }),
  );
}

function InlineDeviceAction({
  command,
  entities,
  onFinished,
}: {
  command: DescriptorDeviceCommand;
  entities: Record<string, EntitySummary>;
  onFinished: (execution: FiniteExecution) => void;
}) {
  const [values, setValues] = useState<Record<string, unknown>>(() =>
    initialValues(command),
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [serverError, setServerError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const fixed = command.initialParameters ?? {};
  const visibleFields = visibleParameters(command.descriptor, values).filter(
    (parameter) => fixed[parameter.name] === undefined,
  );
  const submit = async (event: SyntheticEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextErrors = validateParameters(command.descriptor, values);
    setErrors(nextErrors);
    if (Object.keys(nextErrors).length) return;
    setRunning(true);
    setServerError(null);
    try {
      const execution = await daemonApi.runOperation(
        "action",
        command.descriptor.kind,
        normalizeValues(command.descriptor.parameters, values),
      );
      onFinished(execution);
    } catch (error) {
      setServerError(String(error));
    } finally {
      setRunning(false);
    }
  };
  return (
    <form
      className="inspector-command-form"
      onSubmit={(event) => {
        void submit(event);
      }}
    >
      {visibleFields.map((parameter) => (
        <ParameterField
          key={parameter.name}
          parameter={parameter}
          value={values[parameter.name] ?? ""}
          entities={entities}
          error={errors[parameter.name]}
          operationKind={command.descriptor.kind}
          onChange={(value) => {
            setValues((current) => ({ ...current, [parameter.name]: value }));
          }}
        />
      ))}
      {serverError ? <p className="inline-warning">{serverError}</p> : null}
      <button type="submit" disabled={running}>
        {running ? "Running…" : `Run ${command.descriptor.display_name}`}
      </button>
    </form>
  );
}

function canRunInline(command: DescriptorDeviceCommand) {
  if (command.descriptor.risk === "elevated") return false;
  const values = initialValues(command);
  return command.descriptor.parameters.every(
    (parameter) =>
      !parameter.required ||
      (values[parameter.name] !== undefined && values[parameter.name] !== ""),
  );
}

export function DeviceInspector({
  device,
  detail,
  descriptors,
  entities,
  onNavigate,
  onRunCommand,
  onOperationFinished,
}: {
  device: DeviceSummary;
  detail?: DeviceInspectorSummary;
  descriptors: DescriptorCatalog;
  entities: Record<string, EntitySummary>;
  onNavigate?: (kind: string, id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
  onOperationFinished: (execution: FiniteExecution) => void;
}) {
  const [devices, setDevices] = useState<DeviceSummary[]>([]);
  const [blueprints, setBlueprints] = useState<BlueprintSummary[]>([]);
  useEffect(() => {
    const controller = new AbortController();
    void Promise.all([
      daemonApi
        .devices(controller.signal)
        .then((snapshot) => {
          setDevices(snapshot.devices);
        })
        .catch(() => undefined),
      daemonApi
        .blueprints(controller.signal)
        .then((snapshot) => {
          setBlueprints(snapshot.blueprints);
        })
        .catch(() => undefined),
    ]);
    return () => {
      controller.abort();
    };
  }, []);
  const [expanded, setExpanded] = useState<string | null>(null);
  const commands = useMemo(
    () =>
      advertisedDeviceCommands(
        descriptors,
        device,
        entities,
        devices,
        blueprints,
      ),
    [descriptors, device, entities, devices, blueprints],
  );
  const executable = new Map(
    commands.map((command) => [command.bindingCommand, command]),
  );
  const capabilities = [...new Set(device.available_commands)];
  const directiveDetails = Object.entries(device.directive_details ?? {})
    .filter(([key]) => key !== "directive" && key !== "name")
    .flatMap(([key, value]) => {
      if (
        key === "configuration" &&
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
      ) {
        return Object.entries(value as Record<string, unknown>).map(
          ([configurationKey, configurationValue]) => ({
            label: configurationKey
              .replace(/[._-]+/g, " ")
              .replace(/\b\w/g, (letter) => letter.toUpperCase()),
            value: configurationValue,
          }),
        );
      }
      return [
        {
          label: key
            .replace(/[._-]+/g, " ")
            .replace(/\b\w/g, (letter) => letter.toUpperCase()),
          value,
        },
      ];
    });
  const hasDirective =
    device.active_directive !== null ||
    device.directive_status !== null ||
    Object.keys(device.directive_details ?? {}).length > 0;
  const relations = [
    device.system
      ? {
          label: "System",
          kind: "system",
          id: device.system,
          value: device.system,
        }
      : null,
    device.location
      ? {
          label: "Location",
          kind: "location",
          id: device.location,
          value: device.location,
        }
      : null,
    device.owner
      ? {
          label: "Owned by",
          kind: "replicant",
          id: device.owner,
          value: device.owner_name ?? device.owner,
        }
      : null,
    detail?.hosting_replicant
      ? {
          label: "Hosting Replicant",
          kind: detail.hosting_replicant.kind,
          id: detail.hosting_replicant.id,
          value: detail.hosting_replicant.id,
        }
      : null,
    device.attached_to
      ? {
          label: "Attached to",
          kind: "device",
          id: device.attached_to,
          value: relatedDeviceLabel(device.attached_to, entities),
        }
      : null,
    device.stowed_in
      ? {
          label: "Stowed in",
          kind: "device",
          id: device.stowed_in,
          value: relatedDeviceLabel(device.stowed_in, entities),
        }
      : null,
    device.controller
      ? {
          label: "Controlled by",
          kind: "device",
          id: device.controller,
          value: relatedDeviceLabel(device.controller, entities),
        }
      : null,
    device.linked_device
      ? {
          label: "Linked device",
          kind: "device",
          id: device.linked_device,
          value: relatedDeviceLabel(device.linked_device, entities),
        }
      : null,
    device.claim
      ? {
          label: "Claimed by workflow",
          kind: "workflow",
          id: device.claim.workflow_id,
          value: device.claim.workflow_kind,
        }
      : null,
  ].filter(
    (
      value,
    ): value is { label: string; kind: string; id: string; value: string } =>
      value !== null,
  );
  const relationGroups = [
    ["Attached devices", device.attached_devices],
    ["Controlled devices", device.controlled_devices],
    ["Stowed devices", device.stowed_devices],
  ] as const;

  return (
    <>
      <InspectorFields
        fields={[
          { label: "Type", value: device.device_type },
          { label: "Status", value: device.status },
          {
            label: "Ownership",
            value: device.owner_name ?? device.owner ?? device.ownership,
          },
          { label: "Region", value: device.region },
          { label: "System", value: device.system },
          { label: "Location", value: device.location },
          { label: "Deployed", value: detail?.deployed_at },
          { label: "In controller range", value: detail?.in_control_range },
          { label: "Tags", value: device.tags },
          { label: "Features", value: device.features ?? [] },
          {
            label: "Available directives",
            value: device.available_directives ?? [],
          },
          {
            label: "Operational",
            value: device.operational_capacity_percent,
            render: (value) => `${String(value)}%`,
          },
          {
            label: "Attach",
            value:
              device.attach_capacity === null
                ? null
                : `${String(device.attached_devices.length)} / ${String(device.attach_capacity)}`,
          },
          {
            label: "Cargo",
            value:
              device.cargo_capacity === null && device.cargo_used === null
                ? null
                : `${String(device.cargo_used ?? 0)} / ${String(device.cargo_capacity ?? 0)}`,
          },
          {
            label: "Stow",
            value:
              device.stow_capacity == null && device.stow_used == null
                ? null
                : `${String(device.stow_used ?? 0)} / ${String(device.stow_capacity ?? 0)}`,
          },
          {
            label: "Travel destination",
            value: detail?.travel ? null : device.travel_destination,
          },
          { label: "Grace period", value: device.grace_period_remaining },
        ]}
      />
      <DeviceRolePanel
        device={device}
        detail={detail}
        onNavigate={onNavigate}
      />
      {detail?.runtime.description || detail?.runtime.short_description ? (
        <section className="inspector-section">
          <h3>Description</h3>
          <p>
            {detail.runtime.description ?? detail.runtime.short_description}
          </p>
        </section>
      ) : null}
      {detail ? (
        <DeviceActivityPanel runtime={detail.runtime} onNavigate={onNavigate} />
      ) : null}
      {detail &&
      (detail.runtime.created_at ||
        detail.runtime.queue_size !== null ||
        detail.runtime.taxi_mode ||
        detail.runtime.tracking_site_id !== null ||
        detail.runtime.beacon_only !== null ||
        detail.runtime.welcome_message ||
        detail.runtime.repair_paid_pct !== null) ? (
        <section className="inspector-section">
          <h3>Device configuration</h3>
          <InspectorFields
            fields={[
              { label: "Created", value: detail.runtime.created_at },
              { label: "Queue size", value: detail.runtime.queue_size },
              { label: "Taxi mode", value: detail.runtime.taxi_mode },
              {
                label: "Tracking site",
                value: detail.runtime.tracking_site_id,
              },
              { label: "Beacon only", value: detail.runtime.beacon_only },
              {
                label: "Welcome message",
                value: detail.runtime.welcome_message,
              },
              { label: "Repair paid", value: detail.runtime.repair_paid_pct },
            ]}
          />
        </section>
      ) : null}
      <TravelSection travel={detail?.travel ?? null} />
      {device.system_status || (device.upkeep_requirements ?? []).length ? (
        <section className="inspector-section">
          <h3>Hub & upkeep</h3>
          <InspectorFields
            fields={[
              { label: "Grace period", value: device.grace_period_remaining },
              { label: "System status", value: device.system_status },
              {
                label: "Upkeep requirements",
                value: device.upkeep_requirements ?? [],
              },
            ]}
          />
        </section>
      ) : null}
      {hasDirective ? (
        <section className="inspector-section" aria-label="Directive details">
          <h3>Directive</h3>
          <InspectorFields
            fields={[
              {
                label: "Name",
                value: device.active_directive
                  ? device.active_directive
                      .replace(/[._-]+/g, " ")
                      .replace(/\b\w/g, (letter) => letter.toUpperCase())
                  : "Unidentified directive",
              },
              { label: "Status", value: device.directive_status },
              ...directiveDetails,
            ]}
          />
        </section>
      ) : null}
      {(device.cargo ?? []).length ? (
        <section className="inspector-section">
          <h3>Cargo</h3>
          <ul className="inspector-resource-list">
            {(device.cargo ?? []).map((item) => (
              <li key={item.resource}>
                <button
                  type="button"
                  className="inspector-resource-link"
                  disabled={!onNavigate}
                  onClick={() => onNavigate?.("resource", item.resource)}
                >
                  <span>{item.resource}</span>
                  <strong>{item.quantity.toLocaleString()}</strong>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      {device.claim ? (
        <section className="inspector-section">
          <h3>Automation</h3>
          <InspectorFields
            fields={[
              { label: "Workflow", value: device.claim.workflow_kind },
              { label: "Workflow status", value: device.claim.workflow_status },
              { label: "Workflow ID", value: device.claim.workflow_id },
            ]}
          />
        </section>
      ) : null}
      {detail && Object.keys(detail.settings).length ? (
        <section className="inspector-section">
          <h3>Configuration</h3>
          <InspectorFields
            fields={Object.entries(detail.settings).map(([key, value]) => ({
              label: key
                .replace(/[._-]+/g, " ")
                .replace(/\b\w/g, (letter) => letter.toUpperCase()),
              value,
            }))}
          />
        </section>
      ) : null}
      {relations.length ? (
        <section className="inspector-section">
          <h3>Relations</h3>
          <ul className="inspector-entity-list">
            {relations.map((relation) => (
              <li key={`${relation.label}:${relation.kind}:${relation.id}`}>
                <button
                  type="button"
                  disabled={!onNavigate}
                  onClick={() => onNavigate?.(relation.kind, relation.id)}
                >
                  <strong>{relation.label}</strong>
                  <small>{relation.value}</small>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      {capabilities.length ? (
        <section className="inspector-section">
          <h3>Capabilities</h3>
          <div className="inspector-command-grid">
            {capabilities.map((capability) => {
              const command = executable.get(capability);
              if (!command) {
                return (
                  <span
                    className="inspector-capability unsupported"
                    key={capability}
                  >
                    {capability} · unsupported
                  </span>
                );
              }
              const key = `${command.descriptor.kind}:${command.bindingCommand}`;
              return (
                <div key={key}>
                  <button
                    onClick={() => {
                      if (canRunInline(command)) {
                        setExpanded((current) =>
                          current === key ? null : key,
                        );
                      } else {
                        onRunCommand(command);
                      }
                    }}
                  >
                    {command.descriptor.display_name}
                  </button>
                  {expanded === key ? (
                    <InlineDeviceAction
                      command={command}
                      entities={entities}
                      onFinished={onOperationFinished}
                    />
                  ) : null}
                </div>
              );
            })}
          </div>
        </section>
      ) : null}
      {relationGroups.map(([label, codes]) =>
        codes.length ? (
          <section className="inspector-section" key={label}>
            <h3>{label}</h3>
            <InspectorCollection
              collection={relationCollection(
                label.toLowerCase(),
                codes,
                entities,
              )}
              onNavigate={onNavigate}
            />
          </section>
        ) : null,
      )}
    </>
  );
}
