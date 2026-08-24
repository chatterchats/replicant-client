import type { WorkflowSummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";
export function WorkflowInspector({ workflow }: { workflow: WorkflowSummary }) {
  return (
    <InspectorFields
      fields={[
        { label: "Kind", value: workflow.kind },
        { label: "Status", value: workflow.status },
        { label: "Step", value: workflow.current_step },
        { label: "Revision", value: workflow.revision },
        {
          label: "Updated",
          value: new Date(workflow.updated_at_ms).toLocaleString(),
        },
      ]}
    />
  );
}
