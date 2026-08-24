import { useMemo, useState, type FormEvent } from "react";

import { ParameterField, validateParameters } from "../AutomationsPage";
import { daemonApi } from "../api";
import type { DescriptorCommand } from "../CommandPalette";
import type {
  DescriptorCatalog,
  DeviceSummary,
  EntityCollectionSummary,
  EntitySummary,
  FiniteExecution,
  ParameterDescriptor,
} from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import { InspectorFields } from "./InspectorFields";
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
  const fields = command.descriptor.parameters.filter(
    (parameter) => fixed[parameter.name] === undefined,
  );
  const submit = async (event: FormEvent) => {
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
    <form className="inspector-command-form" onSubmit={submit}>
      {fields.map((parameter) => (
        <ParameterField
          key={parameter.name}
          parameter={parameter}
          value={values[parameter.name] ?? ""}
          entities={entities}
          error={errors[parameter.name]}
          operationKind={command.descriptor.kind}
          onChange={(value) =>
            setValues((current) => ({ ...current, [parameter.name]: value }))
          }
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
  descriptors,
  entities,
  onRunCommand,
  onOperationFinished,
}: {
  device: DeviceSummary;
  descriptors: DescriptorCatalog;
  entities: Record<string, EntitySummary>;
  onRunCommand: (command: DescriptorCommand) => void;
  onOperationFinished: (execution: FiniteExecution) => void;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const commands = useMemo(
    () => advertisedDeviceCommands(descriptors, device, entities),
    [descriptors, device, entities],
  );
  const executable = new Map(
    commands.map((command) => [command.bindingCommand, command]),
  );
  const capabilities = [...new Set(device.available_commands)];
  const relations = [
    device.attached_to
      ? `Attached to ${relatedDeviceLabel(device.attached_to, entities)}`
      : null,
    device.stowed_in
      ? `Stowed in ${relatedDeviceLabel(device.stowed_in, entities)}`
      : null,
    device.controller ? `Controlled by ${device.controller}` : null,
  ].filter((value): value is string => value !== null);
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
          { label: "System", value: device.system },
          { label: "Location", value: device.location },
          { label: "Tags", value: device.tags },
          { label: "Features", value: device.features ?? [] },
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
          { label: "Directive", value: device.active_directive },
          { label: "Directive status", value: device.directive_status },
          { label: "Travel destination", value: device.travel_destination },
          { label: "Grace period", value: device.grace_period_remaining },
          { label: "Upkeep", value: device.upkeep_requirements ?? [] },
          { label: "System status", value: device.system_status },
        ]}
      />
      {(device.cargo ?? []).length ? (
        <section className="inspector-section">
          <h3>Cargo</h3>
          <ul className="inspector-resource-list">
            {(device.cargo ?? []).map((item) => (
              <li key={item.resource}>
                <span>{item.resource}</span>
                <strong>{item.quantity.toLocaleString()}</strong>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      {relations.length ? (
        <section className="inspector-section">
          <h3>Relations</h3>
          <ul>
            {relations.map((relation) => (
              <li key={relation}>{relation}</li>
            ))}
          </ul>
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
            />
          </section>
        ) : null,
      )}
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
    </>
  );
}
