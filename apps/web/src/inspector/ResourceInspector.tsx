import type { EntitySummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";
export function ResourceInspector({ summary }: { summary: EntitySummary }) {
  return (
    <InspectorFields
      fields={[
        {
          label: "Type",
          value: summary.entity_type ?? summary.secondary_label,
        },
        { label: "Status", value: summary.status },
        { label: "System", value: summary.system },
        { label: "Location", value: summary.location },
      ]}
    />
  );
}
