import type { DescriptorCommand } from "../CommandPalette";
import type {
  BlueprintSummary,
  DescriptorCatalog,
  DeviceSummary,
  EntitySummary,
  EventSummary,
  GalaxyStar,
  SystemMarker,
  WorkflowSummary,
} from "../protocol";
import type { SelectedEntity } from "../shellState";

export function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}

export function isEntitySummary(value: unknown): value is EntitySummary {
  const item = asRecord(value);
  return typeof item?.label === "string" && asRecord(item.entity) !== null;
}

export function isDeviceSummary(value: unknown): value is DeviceSummary {
  const item = asRecord(value);
  return (
    asRecord(item?.entity)?.kind === "device" &&
    typeof item?.ownership === "string"
  );
}

export function isWorkflowSummary(value: unknown): value is WorkflowSummary {
  const item = asRecord(value);
  return (
    typeof item?.id === "string" &&
    typeof item.kind === "string" &&
    typeof item.status === "string" &&
    typeof item.revision === "number"
  );
}

export function isEventSummary(value: unknown): value is EventSummary {
  const item = asRecord(value);
  return (
    typeof item?.designation === "string" &&
    typeof item.title === "string" &&
    typeof item.system === "string" &&
    typeof item.location === "string"
  );
}

export function isGalaxyStar(value: unknown): value is GalaxyStar {
  const item = asRecord(value);
  const position = asRecord(item?.position);
  return (
    typeof item?.id === "string" &&
    typeof position?.x === "number" &&
    typeof position.y === "number" &&
    typeof position.z === "number"
  );
}

export function isSystemMarker(value: unknown): value is SystemMarker {
  const item = asRecord(value);
  return (
    typeof item?.id === "string" &&
    typeof item.label === "string" &&
    typeof item.location === "string" &&
    asRecord(item.entity) !== null
  );
}

export function relatedDeviceLabel(
  code: string,
  entities: Record<string, EntitySummary>,
) {
  const related = entities[`device:${code}`];
  return related?.entity_type ? `${related.entity_type} (${code})` : code;
}

export function fallbackSummary(
  entity: SelectedEntity,
  value: unknown,
): EntitySummary {
  if (isEntitySummary(value)) return value;
  if (isDeviceSummary(value)) {
    return {
      entity: value.entity,
      label: value.entity.id,
      secondary_label: value.device_type,
      system: value.system,
      location: value.location,
      entity_type: value.device_type,
      status: value.status,
    };
  }
  if (isWorkflowSummary(value)) {
    return {
      entity: { kind: "workflow", id: value.id },
      label: value.kind,
      secondary_label: value.id,
      system: null,
      location: null,
      entity_type: value.kind,
      status: value.status,
    };
  }
  if (isEventSummary(value)) {
    return {
      entity: { kind: "operation", id: value.designation },
      label: value.title,
      secondary_label: value.designation,
      system: value.system,
      location: value.location,
      entity_type: value.event_type,
      status: value.status,
    };
  }
  return {
    entity: {
      kind:
        entity.kind === "event" || entity.kind === "resource"
          ? "operation"
          : entity.kind,
      id: entity.id,
    },
    label: isGalaxyStar(value)
      ? (value.name ?? entity.id)
      : isSystemMarker(value)
        ? value.label
        : entity.id,
    secondary_label: null,
    system: null,
    location: null,
    entity_type: null,
    status: null,
  };
}

const RESOURCE_TYPES = [
  "carbon",
  "conductive",
  "rares",
  "silicates",
  "structural",
  "volatiles",
];

function compatibleAdoptionTarget(
  controllerType: string | null,
  targetType: string | null,
  blueprints: BlueprintSummary[],
) {
  if (!controllerType || !targetType) return false;
  if (controllerType === "mining_controller")
    return targetType === "mining_drone";
  if (controllerType === "survey_controller")
    return targetType === "survey_drone";
  const blueprint = blueprints.find(
    (candidate) => candidate.device_type === targetType,
  );
  if (controllerType === "transport_controller")
    return blueprint
      ? blueprint.features.includes("transport")
      : targetType.includes("transport") || targetType === "cargo_freighter";
  if (controllerType === "fleet_controller")
    return blueprint
      ? blueprint.features.includes("surge")
      : targetType.includes("surge") || targetType === "cargo_freighter";
  return true;
}

function deviceOptions(
  candidates: DeviceSummary[],
  entities: Record<string, EntitySummary>,
) {
  return candidates.map((candidate) => ({
    value: candidate.entity.id,
    label: relatedDeviceLabel(candidate.entity.id, entities),
  }));
}

export function specializeDeviceCommand(
  command: DescriptorCommand,
  device: DeviceSummary,
  entities: Record<string, EntitySummary> = {},
  devices: DeviceSummary[] = [],
  blueprints: BlueprintSummary[] = [],
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
    const targets = devices.filter(
      (candidate) =>
        candidate.entity.id !== device.entity.id &&
        candidate.system === device.system &&
        candidate.controller === null &&
        compatibleAdoptionTarget(
          device.device_type,
          candidate.device_type,
          blueprints,
        ),
    );
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "target"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: deviceOptions(targets, entities),
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
    command.descriptor.kind === "device.attach" ||
    command.descriptor.kind === "device.repair" ||
    command.descriptor.kind === "device.replicate"
  ) {
    const targets = devices.filter((candidate) => {
      if (
        candidate.entity.id === device.entity.id ||
        candidate.system !== device.system
      )
        return false;
      if (command.descriptor.kind === "device.attach")
        return candidate.attached_to === null && candidate.stowed_in === null;
      if (command.descriptor.kind === "device.repair")
        return (
          candidate.operational_capacity_percent !== null &&
          candidate.operational_capacity_percent < 100
        );
      return candidate.device_type === "empty_replicant_matrix";
    });
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "target"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: deviceOptions(targets, entities),
              }
            : parameter,
        ),
      },
    };
  }
  if (
    command.descriptor.kind === "device.stow" &&
    command.descriptor.parameters.some(
      (parameter) => parameter.name === "target",
    )
  ) {
    const targets = devices.filter(
      (candidate) =>
        candidate.entity.id !== device.entity.id &&
        candidate.system === device.system &&
        candidate.stow_capacity !== null &&
        candidate.stow_capacity !== undefined &&
        candidate.stow_capacity > (candidate.stow_used ?? 0),
    );
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "target"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: deviceOptions(targets, entities),
              }
            : parameter,
        ),
      },
    };
  }
  if (command.descriptor.kind === "device.configure") {
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "mode"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: [
                  { value: "taxi", label: "taxi" },
                  { value: "manual", label: "manual" },
                ],
              }
            : parameter,
        ),
      },
    };
  }
  if (
    (command.descriptor.kind === "device.retarget" ||
      command.descriptor.kind === "device.start_mining") &&
    command.descriptor.parameters.some(
      (parameter) => parameter.name === "resource_type",
    )
  ) {
    return {
      ...command,
      descriptor: {
        ...command.descriptor,
        parameters: command.descriptor.parameters.map((parameter) =>
          parameter.name === "resource_type"
            ? {
                ...parameter,
                kind: { type: "enum" as const },
                options: RESOURCE_TYPES.map((resource) => ({
                  value: resource,
                  label: resource,
                })),
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

export interface DescriptorDeviceCommand extends DescriptorCommand {
  bindingCommand: string;
}

export function advertisedDeviceCommands(
  catalog: DescriptorCatalog,
  device: DeviceSummary,
  entities: Record<string, EntitySummary> = {},
  devices: DeviceSummary[] = [],
  blueprints: BlueprintSummary[] = [],
): DescriptorDeviceCommand[] {
  const seen = new Set<string>();
  return device.available_commands
    .filter((command) => {
      if (seen.has(command)) return false;
      seen.add(command);
      return true;
    })
    .flatMap((bindingCommand) => {
      const descriptor = catalog.actions.find((action) =>
        (action.device_commands ?? []).some(
          (binding) => binding.command === bindingCommand,
        ),
      );
      const binding = descriptor?.device_commands?.find(
        (candidate) => candidate.command === bindingCommand,
      );
      if (!descriptor || !binding) return [];
      const lifecycleLabel =
        descriptor.kind === "device.lifecycle"
          ? descriptor.parameters
              .find((parameter) => parameter.name === "command")
              ?.options.find((option) => option.value === bindingCommand)?.label
          : undefined;
      const command = specializeDeviceCommand(
        {
          operationClass: "action",
          descriptor: lifecycleLabel
            ? { ...descriptor, display_name: lifecycleLabel }
            : descriptor,
          initialParameters: {
            ...binding.parameters,
            device: device.entity.id,
          },
        },
        device,
        entities,
        devices,
        blueprints,
      );
      return [{ ...command, bindingCommand }];
    });
}
