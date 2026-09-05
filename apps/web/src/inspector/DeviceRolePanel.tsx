import type { DeviceInspectorSummary, DeviceSummary } from "../protocol";

interface RoleField {
  label: string;
  value: unknown;
  relation?: { kind: string; id: string } | null;
}

interface DeviceRoleProfile {
  title: string;
  fields: RoleField[];
}

const TRANSPORT_TYPES = new Set([
  "cargo_freighter",
  "cargo_lifter",
  "cargo_vessel",
  "colony_shuttle",
  "fleet_tender",
  "freighter",
  "ftl_transport",
  "heaven_vessel",
  "mobile_fleet",
  "racing_vessel",
  "surge_carrier",
  "transport_drone",
  "transport_hauler",
]);

const NETWORK_TYPES = new Set([
  "comm_satellite",
  "deep_space_relay_station",
  "ftl_beacon",
  "ftl_relay",
  "mesh_relay",
  "signal_booster",
]);

const SURVEY_SENSOR_TYPES = new Set([
  "belt_surveyor",
  "galactic_observatory",
  "parallax_array",
  "planetary_surveyor",
  "seismic_monitor",
  "sensor_array",
]);

const DEFENCE_TYPES = new Set([
  "defence_grid",
  "orbital_defence_platform",
  "point_defence_array",
  "radiation_shroud",
  "shield_generator",
  "system_ward",
  "thermal_lance",
]);

const HABITAT_TYPES = new Set([
  "atmo_processor",
  "filtration_array",
  "hab_module",
  "hydroponic_bay",
  "nutrient_synthesizer",
  "orbital_farm",
  "tidal_compensator",
]);

const POWER_PROPULSION_TYPES = new Set([
  "casimir_array",
  "electrodynamic_tether",
  "exotic_matter_injector",
  "exotic_particle_trap",
  "fusion_barge",
  "graviton_stabiliser",
  "gravity_lens",
  "inertial_anchor",
  "mass_driver",
  "negative_energy_conduit",
  "power_cell_array",
  "propulsor",
  "solar_collector",
  "surge_plate",
  "surge_platform",
]);

const MATRIX_COMPUTE_TYPES = new Set([
  "compute_core",
  "empty_replicant_matrix",
  "matrix_container",
  "replicant_interface",
  "replicant_matrix",
]);

function capacityLabel(
  used: number | null | undefined,
  capacity: number | null | undefined,
) {
  if (used == null && capacity == null) return null;
  return `${String(used ?? 0)} / ${String(capacity ?? 0)}`;
}

function runtimeRecord(value: unknown) {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function runtimeText(value: unknown, key: string) {
  const item = runtimeRecord(value)?.[key];
  return typeof item === "string" && item.trim() ? item : null;
}

function runtimeNumber(value: unknown, key: string) {
  const item = runtimeRecord(value)?.[key];
  return typeof item === "number" && Number.isFinite(item) ? item : null;
}

function progressLabel(value: unknown) {
  const progress = runtimeNumber(value, "progress_percent");
  return progress === null ? null : `${String(Math.round(progress))}%`;
}

function operationalLabel(device: DeviceSummary) {
  return device.operational_capacity_percent === null
    ? null
    : `${String(Math.round(device.operational_capacity_percent))}%`;
}

function placement(device: DeviceSummary): RoleField[] {
  return [
    device.system
      ? {
          label: "System",
          value: device.system,
          relation: { kind: "system", id: device.system },
        }
      : { label: "System", value: null },
    device.location
      ? {
          label: "Location",
          value: device.location,
          relation: { kind: "location", id: device.location },
        }
      : { label: "Location", value: null },
  ];
}

function controllerField(device: DeviceSummary): RoleField {
  return device.controller
    ? {
        label: "Controller",
        value: device.controller,
        relation: { kind: "device", id: device.controller },
      }
    : { label: "Controller", value: null };
}

function linkedField(device: DeviceSummary): RoleField {
  return device.linked_device
    ? {
        label: "Linked device",
        value: device.linked_device,
        relation: { kind: "device", id: device.linked_device },
      }
    : { label: "Linked device", value: null };
}

function commonOperational(device: DeviceSummary): RoleField[] {
  return [
    { label: "Status", value: device.status },
    { label: "Operational", value: operationalLabel(device) },
    ...placement(device),
  ];
}

function controllerTitle(type: string) {
  if (type.includes("mining")) return "Mining controller summary";
  if (type.includes("survey")) return "Survey controller summary";
  if (type.includes("trade")) return "Trade controller summary";
  if (type.includes("transport")) return "Transport controller summary";
  if (type.includes("fleet")) return "Fleet controller summary";
  return "Controller summary";
}

// Exported for focused role-classification tests; the component remains the runtime surface.
// eslint-disable-next-line react-refresh/only-export-components
export function deviceRoleProfile(
  device: DeviceSummary,
  detail?: DeviceInspectorSummary,
): DeviceRoleProfile | null {
  const type = device.device_type?.toLowerCase() ?? "";
  if (!type) return null;

  if (type === "autofactory" || type === "structural_fabricator") {
    return {
      title: "Production summary",
      fields: [
        {
          label: "Current job",
          value:
            runtimeText(detail?.runtime.printing, "device_type") ??
            (detail?.runtime.printing ? "Printing" : "Idle"),
        },
        { label: "Progress", value: progressLabel(detail?.runtime.printing) },
        {
          label: "Queued jobs",
          value: detail?.runtime.print_queue.length ?? 0,
        },
        { label: "Queue capacity", value: detail?.runtime.queue_size },
        {
          label: "Cargo",
          value: capacityLabel(device.cargo_used, device.cargo_capacity),
        },
        ...placement(device),
      ],
    };
  }

  if (
    TRANSPORT_TYPES.has(type) ||
    type.includes("carrier") ||
    type.includes("vessel")
  ) {
    return {
      title:
        type === "transport_drone" || type === "transport_hauler"
          ? "Transport device summary"
          : "Transport summary",
      fields: [
        detail?.hosting_replicant
          ? {
              label: "Hosted Replicant",
              value: detail.hosting_replicant.id,
              relation: {
                kind: "replicant",
                id: detail.hosting_replicant.id,
              },
            }
          : { label: "Hosted Replicant", value: null },
        {
          label: "Attached payloads",
          value: capacityLabel(
            device.attached_devices.length,
            device.attach_capacity,
          ),
        },
        {
          label: "Stowed payloads",
          value: capacityLabel(
            device.stow_used ?? device.stowed_devices.length,
            device.stow_capacity,
          ),
        },
        {
          label: "Cargo",
          value: capacityLabel(device.cargo_used, device.cargo_capacity),
        },
        {
          label: "Final destination",
          value:
            detail?.travel?.final_destination ?? detail?.travel?.destination,
        },
        controllerField(device),
        ...placement(device),
      ],
    };
  }

  if (type === "mining_drone") {
    const belt = runtimeText(detail?.runtime.mining, "belt");
    return {
      title: "Mining drone summary",
      fields: [
        {
          label: "Resource",
          value: runtimeText(detail?.runtime.mining, "resource_type"),
        },
        {
          label: "Belt",
          value: belt,
          relation: belt ? { kind: "location", id: belt } : null,
        },
        {
          label: "Density",
          value: runtimeText(detail?.runtime.mining, "density"),
        },
        {
          label: "Pending quantity",
          value: runtimeNumber(detail?.runtime.mining, "pending_quantity"),
        },
        controllerField(device),
        { label: "In controller range", value: detail?.in_control_range },
      ],
    };
  }

  if (type === "survey_drone") {
    return {
      title: "Survey drone summary",
      fields: [
        {
          label: "Scan target",
          value: runtimeText(detail?.runtime.scan, "target"),
        },
        { label: "Scan progress", value: progressLabel(detail?.runtime.scan) },
        controllerField(device),
        { label: "In controller range", value: detail?.in_control_range },
      ],
    };
  }

  if (type === "maintenance_drone" || type === "service_bot") {
    const target = runtimeText(detail?.runtime.repair, "target_device_code");
    return {
      title: type === "service_bot" ? "Service summary" : "Maintenance summary",
      fields: [
        {
          label: "Repair target",
          value: target,
          relation: target ? { kind: "device", id: target } : null,
        },
        {
          label: "Repair progress",
          value: progressLabel(detail?.runtime.repair),
        },
        { label: "Repair paid", value: detail?.runtime.repair_paid_pct },
        controllerField(device),
        { label: "In controller range", value: detail?.in_control_range },
        ...placement(device),
      ],
    };
  }

  if (SURVEY_SENSOR_TYPES.has(type)) {
    const prospect = detail?.runtime.prospect;
    return {
      title:
        type === "galactic_observatory"
          ? "Observatory summary"
          : "Survey & sensing summary",
      fields: [
        { label: "State", value: prospect ? "Prospecting" : device.status },
        {
          label: "Progress",
          value: progressLabel(prospect ?? detail?.runtime.scan),
        },
        { label: "Origin", value: runtimeText(prospect, "origin") },
        {
          label: "Controlled devices",
          value: device.controlled_devices.length || null,
        },
        { label: "Operational", value: operationalLabel(device) },
        ...placement(device),
      ],
    };
  }

  if (type === "ftl_slingshot") {
    return {
      title: "FTL slingshot summary",
      fields: [
        ...commonOperational(device),
        linkedField(device),
        {
          label: "Attached devices",
          value: capacityLabel(
            device.attached_devices.length,
            device.attach_capacity,
          ),
        },
      ],
    };
  }

  if (NETWORK_TYPES.has(type)) {
    return {
      title: "FTL & communications summary",
      fields: [
        { label: "Network state", value: device.status },
        { label: "Operational", value: operationalLabel(device) },
        { label: "Beacon only", value: detail?.runtime.beacon_only },
        { label: "Tracking site", value: detail?.runtime.tracking_site_id },
        linkedField(device),
        ...placement(device),
      ],
    };
  }

  if (type.includes("controller") || type.startsWith("ami_")) {
    return {
      title: controllerTitle(type),
      fields: [
        {
          label: "Controlled devices",
          value: device.controlled_devices.length,
        },
        { label: "Directive", value: device.active_directive },
        { label: "Directive status", value: device.directive_status },
        {
          label: "Collect system",
          value: device.directive_collect_system,
          relation: device.directive_collect_system
            ? { kind: "system", id: device.directive_collect_system }
            : null,
        },
        {
          label: "Target system",
          value: device.directive_target_system,
          relation: device.directive_target_system
            ? { kind: "system", id: device.directive_target_system }
            : null,
        },
        { label: "In control range", value: detail?.in_control_range },
        ...placement(device),
      ],
    };
  }

  if (type === "system_hub") {
    return {
      title: "System Hub summary",
      fields: [
        { label: "Status", value: device.status },
        { label: "Operational", value: operationalLabel(device) },
        { label: "Grace period", value: device.grace_period_remaining },
        {
          label: "Upkeep blockers",
          value: device.upkeep_requirements?.length ?? 0,
        },
        {
          label: "Attached devices",
          value: capacityLabel(
            device.attached_devices.length,
            device.attach_capacity,
          ),
        },
        {
          label: "Stowed devices",
          value: capacityLabel(device.stow_used, device.stow_capacity),
        },
        { label: "Beacon only", value: detail?.runtime.beacon_only },
        ...placement(device),
      ],
    };
  }

  if (DEFENCE_TYPES.has(type)) {
    return {
      title: "Defence & protection summary",
      fields: [
        ...commonOperational(device),
        { label: "Grace period", value: device.grace_period_remaining },
        {
          label: "Attached devices",
          value: capacityLabel(
            device.attached_devices.length,
            device.attach_capacity,
          ),
        },
        linkedField(device),
      ],
    };
  }

  if (HABITAT_TYPES.has(type)) {
    return {
      title: "Habitat & planetary support summary",
      fields: [
        ...commonOperational(device),
        {
          label: "Cargo",
          value: capacityLabel(device.cargo_used, device.cargo_capacity),
        },
        {
          label: "Upkeep blockers",
          value: device.upkeep_requirements?.length ?? 0,
        },
      ],
    };
  }

  if (POWER_PROPULSION_TYPES.has(type)) {
    return {
      title: "Power & propulsion summary",
      fields: [
        ...commonOperational(device),
        linkedField(device),
        {
          label: "Attached devices",
          value: capacityLabel(
            device.attached_devices.length,
            device.attach_capacity,
          ),
        },
        {
          label: "Upkeep blockers",
          value: device.upkeep_requirements?.length ?? 0,
        },
      ],
    };
  }

  if (MATRIX_COMPUTE_TYPES.has(type)) {
    return {
      title: "Matrix & compute summary",
      fields: [
        ...commonOperational(device),
        device.stowed_in
          ? {
              label: "Stowed in",
              value: device.stowed_in,
              relation: { kind: "device", id: device.stowed_in },
            }
          : { label: "Stowed in", value: null },
        linkedField(device),
        {
          label: "Stow",
          value: capacityLabel(device.stow_used, device.stow_capacity),
        },
      ],
    };
  }

  return {
    title: "Operational summary",
    fields: [
      ...commonOperational(device),
      controllerField(device),
      linkedField(device),
      {
        label: "Cargo",
        value: capacityLabel(device.cargo_used, device.cargo_capacity),
      },
      {
        label: "Attached",
        value: capacityLabel(
          device.attached_devices.length,
          device.attach_capacity,
        ),
      },
      {
        label: "Stowed",
        value: capacityLabel(device.stow_used, device.stow_capacity),
      },
    ],
  };
}

function visible(value: unknown) {
  if (value === null || value === undefined || value === "") return false;
  if (Array.isArray(value) && !value.length) return false;
  return true;
}

function display(value: unknown) {
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "string" || typeof value === "number")
    return String(value);
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return String(value);
}

export function DeviceRolePanel({
  device,
  detail,
  onNavigate,
}: {
  device: DeviceSummary;
  detail?: DeviceInspectorSummary;
  onNavigate?: (kind: string, id: string) => void;
}) {
  const profile = deviceRoleProfile(device, detail);
  if (!profile) return null;
  const fields = profile.fields.filter((field) => visible(field.value));
  if (!fields.length) return null;

  return (
    <section className="inspector-section inspector-role-panel">
      <h3>{profile.title}</h3>
      <dl className="inspector-role-facts">
        {fields.map((field) => {
          const relation = field.relation;
          return (
            <div key={field.label}>
              <dt>{field.label}</dt>
              <dd>
                {relation ? (
                  <button
                    type="button"
                    className="inspector-inline-link"
                    disabled={!onNavigate}
                    onClick={() => {
                      onNavigate?.(relation.kind, relation.id);
                    }}
                  >
                    {display(field.value)}
                  </button>
                ) : (
                  display(field.value)
                )}
              </dd>
            </div>
          );
        })}
      </dl>
    </section>
  );
}
