import type { EventSummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";
export function EventInspector({ event }: { event: EventSummary }) {
  return (
    <InspectorFields
      fields={[
        { label: "Status", value: event.status },
        {
          label: "Type",
          value: [event.event_type, event.category, event.tier].filter(Boolean),
        },
        { label: "System", value: event.system },
        { label: "Location", value: event.location },
        { label: "Description", value: event.description },
      ]}
    />
  );
}
