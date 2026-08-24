import type { DeviceSummary, EntitySummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";
import { relatedDeviceLabel } from "./inspectorModel";

export function DeviceInspector({
  device,
  entities,
}: {
  device: DeviceSummary;
  entities: Record<string, EntitySummary>;
}) {
  const related = [
    device.attached_to
      ? { label: "Attached to", code: device.attached_to }
      : null,
    device.stowed_in ? { label: "Stowed in", code: device.stowed_in } : null,
    device.controller
      ? { label: "Controlled by", code: device.controller }
      : null,
  ].filter((item): item is { label: string; code: string } => item !== null);
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
          {
            label: "Operational",
            value: device.operational_capacity_percent,
            render: (value) => `${String(value)}%`,
          },
        ]}
      />
      {related.length ? (
        <ul className="inspector-entity-list">
          {related.map(({ label, code }) => (
            <li key={label}>
              <span>{label}</span>
              <strong>{relatedDeviceLabel(code, entities)}</strong>
            </li>
          ))}
        </ul>
      ) : null}
    </>
  );
}
