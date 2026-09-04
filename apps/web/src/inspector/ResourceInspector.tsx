import type { EntitySummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";

export function ResourceInspector({
  resource,
  summary,
}: {
  resource: string;
  summary?: EntitySummary;
}) {
  return (
    <InspectorFields
      fields={[
        { label: "Resource", value: resource },
        { label: "Type", value: summary?.entity_type ?? "resource" },
        { label: "Status", value: summary?.status },
      ]}
    />
  );
}
